// scheduled_inputs.js

document.addEventListener('DOMContentLoaded', function() {
    // Retrieve CSRF token from meta tag (if CSRF protection is implemented)
    const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute('content');
    if (csrfToken) {
        // Set Axios to include the CSRF token in headers
        axios.defaults.headers.common['X-CSRFToken'] = csrfToken;
    }

    loadScheduledInputs();

    // Event delegation for dynamically created elements
    document.addEventListener('click', function(event) {
        const target = event.target.closest('.load-btn, .edit-btn, .delete-btn, .toggle-disable-btn, .clone-btn');

        if (target) {
            const inputId = target.dataset.id;
            if (target.classList.contains('load-btn')) {
                runInput(inputId);
            } else if (target.classList.contains('edit-btn')) {
                const buttonRect = target.getBoundingClientRect();
                toggleDropdownMenu(inputId, buttonRect);
            } else if (target.classList.contains('delete-btn')) {
                deleteScheduledInput(inputId);
            } else if (target.classList.contains('toggle-disable-btn')) {
                toggleDisable(inputId);
            } else if (target.classList.contains('clone-btn')) {
                cloneScheduledInput(inputId);
            }
        }
    });

    // Close dropdown when clicking outside
    document.addEventListener('click', function(event) {
        if (!event.target.closest('.dropdown-menu') && !event.target.closest('.edit-btn')) {
            closeAllDropdowns();
        }
    });

    /**
     * Load scheduled inputs from the server.
     */
    function loadScheduledInputs() {
        axios.get('/get_scheduled_inputs')
            .then(response => {
                const data = response.data;
                console.log('Received data:', data); // Debugging statement
                if (data.status === 'success') {
                    const scheduledInputs = data.inputs;
                    renderScheduledInputsTable(scheduledInputs);
                } else {
                    showError('Error loading scheduled inputs.');
                }
            })
            .catch(error => {
                console.error('Error loading scheduled inputs:', error);
                showError('Error loading scheduled inputs.');
            });
    }

    /**
     * Render the scheduled inputs table.
     * @param {Array} inputs - Array of scheduled input objects.
     */
    function renderScheduledInputsTable(inputs) {
        const tableContainer = document.getElementById('scheduled-inputs-table');
        tableContainer.innerHTML = ''; // Clear previous content

        // Remove any existing dropdown menus
        document.querySelectorAll('.dropdown-menu').forEach(menu => menu.remove());

        if (inputs.length === 0) {
            tableContainer.innerHTML = '<p>No scheduled inputs available.</p>';
            return;
        }

        // Create the table
        const table = document.createElement('table');
        table.className = 'table is-striped is-hoverable is-fullwidth';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');

        const headers = [
            'Title', 'ID', 'Description', 'Cron Schedule', 'Subdirectory',
            'Created At', 'Overwrite', 'Status',
            'Edit', 'Run'
        ];

        headers.forEach(headerText => {
            const th = document.createElement('th');
            th.innerText = headerText;
            headerRow.appendChild(th);
        });

        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');

        inputs.forEach(input => {
            const row = document.createElement('tr');

            // Debugging: Log each input's disabled status
            console.log(`Input ID: ${input.id}, Disabled: ${input.disabled}`);

            // Title with link
            const titleCell = document.createElement('td');
            const titleLink = document.createElement('a');
            titleLink.href = `/edit_scheduled_input/${input.id}`;
            titleLink.textContent = input.title;
            titleCell.appendChild(titleLink);
            row.appendChild(titleCell);

            // ID
            const idCell = document.createElement('td');
            idCell.textContent = input.id;
            row.appendChild(idCell);

            // Description
            const descriptionCell = document.createElement('td');
            descriptionCell.textContent = input.description;
            row.appendChild(descriptionCell);

            // Cron Schedule
            const cronCell = document.createElement('td');
            cronCell.textContent = input.cron_schedule;
            row.appendChild(cronCell);

            // Subdirectory
            const subdirectoryCell = document.createElement('td');
            subdirectoryCell.textContent = input.subdirectory || 'None';
            row.appendChild(subdirectoryCell);

            // Created At
            const createdAtCell = document.createElement('td');
            const createdAtDate = new Date(input.created_at * 1000); // Assuming timestamp is in seconds
            createdAtCell.textContent = createdAtDate.toLocaleString();
            row.appendChild(createdAtCell);

            // Overwrite
            const overwriteCell = document.createElement('td');
            overwriteCell.textContent = input.overwrite ? 'Yes' : 'No';
            row.appendChild(overwriteCell);

            // Status
            const statusCell = document.createElement('td');
            // Enhanced logic to handle different data types
            const isDisabled = (input.disabled === 1 || input.disabled === '1' || input.disabled === true || input.disabled === 'true');
            statusCell.textContent = isDisabled ? 'disabled' : 'enabled';
            statusCell.className = isDisabled ? 'status-disabled' : 'status-enabled';
            row.appendChild(statusCell);

            // Edit Button
            const editCell = document.createElement('td');
            const editButton = document.createElement('button');
            editButton.className = 'button edit-btn';
            editButton.textContent = 'Edit';
            editButton.dataset.id = input.id;
            editCell.appendChild(editButton);
            row.appendChild(editCell);

            // Run Button
            const runCell = document.createElement('td');
            const runButton = document.createElement('button');
            runButton.className = 'button load-btn';
            runButton.textContent = 'Run';
            runButton.dataset.id = input.id;
            runCell.appendChild(runButton);
            row.appendChild(runCell);

            tbody.appendChild(row);

            // Create the dropdown menu for this input
            createDropdownMenu(input);
        });

        table.appendChild(tbody);

        // Append the table to the container
        tableContainer.appendChild(table);
    }

    /**
     * Create the dropdown menu for a scheduled input and append it to the body.
     * @param {Object} input - The scheduled input object.
     */
    function createDropdownMenu(input) {
        const menu = document.createElement('div');
        menu.className = 'dropdown-menu';
        menu.id = `dropdown-menu-${input.id}`;

        const content = document.createElement('div');
        content.className = 'dropdown-content';

        // Edit Input
        const editInput = document.createElement('a');
        editInput.href = `/edit_scheduled_input/${input.id}`;
        editInput.className = 'dropdown-item';
        editInput.textContent = 'Edit Input';
        content.appendChild(editInput);

        // Delete Action
        const deleteAction = document.createElement('a');
        deleteAction.href = '#';
        deleteAction.className = 'dropdown-item delete-btn';
        deleteAction.dataset.id = input.id;
        deleteAction.textContent = 'Delete';
        content.appendChild(deleteAction);

        // Toggle Disable/Enable Action
        const toggleDisableAction = document.createElement('a');
        toggleDisableAction.href = '#';
        toggleDisableAction.className = 'dropdown-item toggle-disable-btn';
        toggleDisableAction.dataset.id = input.id;
        toggleDisableAction.textContent = (input.disabled === 1 || input.disabled === '1' || input.disabled === true || input.disabled === 'true') ? 'Enable' : 'Disable';
        content.appendChild(toggleDisableAction);

        // Clone Action
        const cloneAction = document.createElement('a');
        cloneAction.href = '#';
        cloneAction.className = 'dropdown-item clone-btn';
        cloneAction.dataset.id = input.id;
        cloneAction.textContent = 'Clone';
        content.appendChild(cloneAction);

        menu.appendChild(content);
        document.body.appendChild(menu); // Append directly to the body
    }

    /**
     * Toggle the visibility of a dropdown menu and position it near the button.
     * @param {string} inputId - The ID of the scheduled input.
     * @param {DOMRect} buttonRect - The bounding rectangle of the clicked button.
     */
    function toggleDropdownMenu(inputId, buttonRect) {
        closeAllDropdowns();

        const menu = document.getElementById(`dropdown-menu-${inputId}`);
        if (menu) {
            // Position the menu
            menu.style.position = 'fixed';

            // Position relative to the viewport
            const menuTop = buttonRect.bottom;
            const menuLeft = buttonRect.left;

            // Adjust position if the menu goes beyond the viewport
            const menuHeight = menu.offsetHeight;
            const menuWidth = menu.offsetWidth;
            const viewportHeight = window.innerHeight;
            const viewportWidth = window.innerWidth;

            // Adjust vertical position if necessary
            if (menuTop + menuHeight > viewportHeight) {
                menu.style.top = `${buttonRect.top - menuHeight}px`; // Position above the button
            } else {
                menu.style.top = `${menuTop}px`; // Position below the button
            }

            // Adjust horizontal position if necessary
            if (menuLeft + menuWidth > viewportWidth) {
                menu.style.left = `${viewportWidth - menuWidth - 10}px`; // Align to the right edge
            } else {
                menu.style.left = `${menuLeft}px`; // Align to the left of the button
            }

            menu.classList.add('is-active');
        }
    }

    /**
     * Close all open dropdown menus.
     */
    function closeAllDropdowns() {
        const menus = document.querySelectorAll('.dropdown-menu.is-active');
        menus.forEach(menu => {
            menu.classList.remove('is-active');
        });
    }

    /**
     * Execute a scheduled input.
     * @param {string} inputId - The ID of the scheduled input.
     */
    function runInput(inputId) {
        axios.post(`/run_scheduled_input/${inputId}`, {})
            .then(response => {
                if (response.data.status === 'success') {
                    showSuccess('Input executed successfully.');
                } else {
                    showError('Error executing input.');
                }
            })
            .catch(error => {
                console.error('Error running input:', error);
                showError('Error running input.');
            });
    }

    /**
     * Delete a scheduled input after confirmation.
     * @param {string} inputId - The ID of the scheduled input.
     */
    function deleteScheduledInput(inputId) {
        if (confirm('Are you sure you want to delete this input?')) {
            axios.post(`/delete_scheduled_input/${inputId}`, {})
                .then(response => {
                    if (response.data.status === 'success') {
                        showSuccess('Scheduled input deleted successfully.');
                        loadScheduledInputs();
                    } else {
                        showError('Error deleting scheduled input.');
                    }
                })
                .catch(error => {
                    console.error('Error deleting input:', error);
                    showError('Error deleting input.');
                });
        }
    }

    /**
     * Toggle the disabled status of a scheduled input.
     * @param {string} inputId - The ID of the scheduled input.
     */
    function toggleDisable(inputId) {
        axios.post(`/toggle_disable_scheduled_input/${inputId}`, {})
            .then(response => {
                if (response.data.status === 'success') {
                    const newDisabled = response.data.new_disabled;
                    showSuccess(`Scheduled input ${newDisabled ? 'disabled' : 'enabled'} successfully.`);
                    loadScheduledInputs();
                } else {
                    showError('Error updating scheduled input.');
                }
            })
            .catch(error => {
                console.error('Error updating input:', error);
                showError('Error updating input.');
            });
    }

    /**
     * Clone a scheduled input.
     * @param {string} inputId - The ID of the scheduled input.
     */
    function cloneScheduledInput(inputId) {
        if (confirm('Are you sure you want to clone this input?')) {
            axios.post(`/clone_scheduled_input/${inputId}`, {})
                .then(response => {
                    if (response.data.status === 'success') {
                        showSuccess('Scheduled input cloned successfully.');
                        loadScheduledInputs();
                    } else {
                        showError('Error cloning scheduled input.');
                    }
                })
                .catch(error => {
                    console.error('Error cloning input:', error);
                    showError('Error cloning input.');
                });
        }
    }

    /**
     * Display a success notification using Bulma's notification component.
     * @param {string} message - The message to display.
     */
    function showSuccess(message) {
        showNotification(message, 'is-primary');
    }

    /**
     * Display an error notification using Bulma's notification component.
     * @param {string} message - The error message to display.
     */
    function showError(message) {
        showNotification(message, 'is-danger');
    }

    /**
     * Display a notification using Bulma's notification component.
     * @param {string} message - The message to display.
     * @param {string} type - Bulma notification type (e.g., 'is-primary', 'is-danger').
     */
    function showNotification(message, type) {
        const container = document.getElementById('notification-container');
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.innerHTML = `
            <button class="delete"></button>
            ${message}
        `;
        container.appendChild(notification);

        // Automatically remove the notification after 5 seconds
        setTimeout(() => {
            notification.remove();
        }, 5000);

        // Add event listener to the delete button
        notification.querySelector('.delete').addEventListener('click', () => {
            notification.remove();
        });
    }
});
