// saved_searches.js

document.addEventListener('DOMContentLoaded', function() {
    // Retrieve CSRF token from meta tag (if CSRF protection is implemented)
    const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute('content');
    if (csrfToken) {
        // Set Axios to include the CSRF token in headers
        axios.defaults.headers.common['X-CSRFToken'] = csrfToken;
    }

    loadSavedSearches();

    // Event delegation for dynamically created elements
    document.getElementById('saved-searches-table').addEventListener('click', function(event) {
        const target = event.target.closest('.load-btn, .edit-btn');

        if (target) {
            if (target.classList.contains('load-btn')) {
                const query = target.dataset.query;
                runSearch(query);
            } else if (target.classList.contains('edit-btn')) {
                const searchId = target.dataset.id;
                const buttonRect = target.getBoundingClientRect();
                toggleDropdownMenu(searchId, buttonRect);
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
     * Load saved searches from the server.
     */
    function loadSavedSearches() {
        axios.get('/get_saved_searches')
            .then(response => {
                const data = response.data;
                if (data.status === 'success') {
                    const savedSearches = data.searches;
                    renderSavedSearchesTable(savedSearches);
                } else {
                    showError('Error loading saved searches.');
                }
            })
            .catch(error => {
                console.error('Error loading saved searches:', error);
                showError('Error loading saved searches.');
            });
    }

    /**
     * Render the saved searches table.
     * @param {Array} searches - Array of saved search objects.
     */
    function renderSavedSearchesTable(searches) {
        const tableContainer = document.getElementById('saved-searches-table');
        tableContainer.innerHTML = ''; // Clear previous content

        // Remove any existing dropdown menus
        document.querySelectorAll('.dropdown-menu').forEach(menu => menu.remove());

        if (searches.length === 0) {
            tableContainer.innerHTML = '<p>No saved searches available.</p>';
            return;
        }

        // Create the table
        const table = document.createElement('table');
        table.className = 'table is-striped is-hoverable is-fullwidth';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');

        const headers = [
            'Search Title', 'Description', 'Cron Schedule', 'Trigger', 'Lookback',
            'Next Scheduled Time', 'Owner', 'Execution Count', 'Status',
            'Email Enabled', 'Edit', 'Run'
        ];

        headers.forEach(headerText => {
            const th = document.createElement('th');
            th.innerText = headerText;
            headerRow.appendChild(th);
        });

        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');

        searches.forEach(search => {
            const row = document.createElement('tr');

            // Search Title
            const titleCell = document.createElement('td');
            const titleLink = document.createElement('a');
            titleLink.href = `/saved_search/${search.id}`;
            titleLink.innerText = search.title;
            titleCell.appendChild(titleLink);
            row.appendChild(titleCell);

            // Description
            const descriptionCell = document.createElement('td');
            descriptionCell.innerText = search.description || '';
            row.appendChild(descriptionCell);

            // Cron Schedule
            const cronCell = document.createElement('td');
            cronCell.innerText = search.cron_schedule || '';
            row.appendChild(cronCell);

            // Trigger
            const triggerCell = document.createElement('td');
            triggerCell.innerText = search.trigger || '';
            row.appendChild(triggerCell);

            // Lookback
            const lookbackCell = document.createElement('td');
            lookbackCell.innerText = search.lookback || '';
            row.appendChild(lookbackCell);

            // Next Scheduled Time
            const nextTimeCell = document.createElement('td');
            if (search.next_scheduled_time) {
                const nextTime = new Date(search.next_scheduled_time);
                const options = {
                    year: 'numeric',
                    month: '2-digit',
                    day: '2-digit',
                    hour: '2-digit',
                    minute: '2-digit',
                    hour12: false
                };
                const formattedTime = nextTime.toLocaleString(undefined, options);
                nextTimeCell.innerText = formattedTime;
            } else {
                nextTimeCell.innerText = '';
            }
            row.appendChild(nextTimeCell);

            // Owner
            const ownerCell = document.createElement('td');
            ownerCell.innerText = search.owner || '';
            row.appendChild(ownerCell);

            // Execution Count
            const execCountCell = document.createElement('td');
            execCountCell.innerText = search.execution_count || 0;
            row.appendChild(execCountCell);

            // Status
            const statusCell = document.createElement('td');
            statusCell.innerText = search.disabled ? 'Disabled' : 'Active';
            row.appendChild(statusCell);

            // Email Enabled
            const emailEnabledCell = document.createElement('td');
            emailEnabledCell.innerText = search.send_email ? 'Yes' : 'No';
            row.appendChild(emailEnabledCell);

            // Edit Button
            const editCell = document.createElement('td');
            const editButton = document.createElement('button');
            editButton.className = 'button edit-btn';
            editButton.innerText = 'Edit';
            editButton.dataset.id = search.id;
            editCell.appendChild(editButton);
            row.appendChild(editCell);

            // Run Button
            const runCell = document.createElement('td');
            const runButton = document.createElement('button');
            runButton.className = 'button load-btn';
            runButton.innerText = 'Run';
            runButton.dataset.query = search.query;
            runCell.appendChild(runButton);
            row.appendChild(runCell);

            tbody.appendChild(row);

            // Create the dropdown menu for this search
            createDropdownMenu(search);
        });

        table.appendChild(tbody);

        // Append the table to the container
        tableContainer.appendChild(table);
    }

    /**
     * Create the dropdown menu for a saved search and append it to the body.
     * @param {Object} search - The saved search object.
     */
    function createDropdownMenu(search) {
        const menu = document.createElement('div');
        menu.className = 'dropdown-menu';
        menu.id = `dropdown-menu-${search.id}`;

        const content = document.createElement('div');
        content.className = 'dropdown-content';

        // Edit Search
        const editSearch = document.createElement('a');
        editSearch.href = `/saved_search/${search.id}`;
        editSearch.className = 'dropdown-item';
        editSearch.innerText = 'Edit Search';
        content.appendChild(editSearch);

        // Delete Action
        const deleteAction = document.createElement('a');
        deleteAction.href = '#';
        deleteAction.className = 'dropdown-item delete-btn';
        deleteAction.dataset.id = search.id;
        deleteAction.innerText = 'Delete';
        deleteAction.addEventListener('click', (e) => {
            e.preventDefault();
            deleteSearch(search.id);
            closeAllDropdowns();
        });
        content.appendChild(deleteAction);

        // Toggle Disable Action
        const toggleDisableAction = document.createElement('a');
        toggleDisableAction.href = '#';
        toggleDisableAction.className = 'dropdown-item toggle-disable-btn';
        toggleDisableAction.dataset.id = search.id;
        toggleDisableAction.innerText = search.disabled ? 'Enable' : 'Disable';
        toggleDisableAction.addEventListener('click', (e) => {
            e.preventDefault();
            toggleDisable(search.id);
            closeAllDropdowns();
        });
        content.appendChild(toggleDisableAction);

        // Clone Action
        const cloneAction = document.createElement('a');
        cloneAction.href = '#';
        cloneAction.className = 'dropdown-item clone-btn';
        cloneAction.dataset.id = search.id;
        cloneAction.innerText = 'Clone';
        cloneAction.addEventListener('click', (e) => {
            e.preventDefault();
            cloneSearch(search.id);
            closeAllDropdowns();
        });
        content.appendChild(cloneAction);

        menu.appendChild(content);
        document.body.appendChild(menu); // Append directly to the body
    }

    /**
     * Toggle the visibility of a dropdown menu and position it near the button.
     * @param {number|string} searchId - The ID of the saved search.
     * @param {DOMRect} buttonRect - The bounding rectangle of the clicked button.
     */
    function toggleDropdownMenu(searchId, buttonRect) {
        closeAllDropdowns();

        const menu = document.getElementById(`dropdown-menu-${searchId}`);
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
     * Run a saved search by storing the query in localStorage and redirecting.
     * @param {string} query - The search query.
     */
    function runSearch(query) {
        if (!query) {
            showError('No query available to run.');
            return;
        }
        localStorage.setItem('savedQuery', query);
        window.location.href = '/'; // Redirect to the home/search page
    }

    /**
     * Delete a saved search after confirmation.
     * @param {number|string} searchId - The ID of the saved search.
     */
    function deleteSearch(searchId) {
        // Confirmation prompt
        const confirmation = confirm('Are you sure you want to delete this saved search?');
        if (!confirmation) {
            return; // Exit if the user cancels
        }

        axios.post(`/delete_search/${searchId}`, {})
            .then(response => {
                if (response.data.status === 'success') {
                    showSuccess('Saved search deleted successfully.');
                    loadSavedSearches();
                } else {
                    showError(response.data.message || 'Error deleting saved search.');
                }
            })
            .catch(error => {
                console.error('Error deleting search:', error);
                showError('An unexpected error occurred while deleting the saved search.');
            });
    }

    /**
     * Toggle the disabled status of a saved search.
     * @param {number|string} searchId - The ID of the saved search.
     */
    function toggleDisable(searchId) {
        axios.post(`/toggle_disable_search/${searchId}`, {})
            .then(response => {
                if (response.data.status === 'success') {
                    showSuccess('Saved search status updated successfully.');
                    loadSavedSearches();
                } else {
                    showError(response.data.message || 'Error updating saved search.');
                }
            })
            .catch(error => {
                console.error('Error updating search:', error);
                showError('An unexpected error occurred while updating the saved search.');
            });
    }

    /**
     * Clone a saved search.
     * @param {number|string} searchId - The ID of the saved search.
     */
    function cloneSearch(searchId) {
        axios.post(`/clone_search/${searchId}`, {})
            .then(response => {
                if (response.data.status === 'success') {
                    showSuccess('Saved search cloned successfully.');
                    loadSavedSearches();
                } else {
                    showError(response.data.message || 'Error cloning saved search.');
                }
            })
            .catch(error => {
                console.error('Error cloning search:', error);
                showError('An unexpected error occurred while cloning the saved search.');
            });
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
