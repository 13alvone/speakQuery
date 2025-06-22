// lookups.js

document.addEventListener('DOMContentLoaded', function() {
    loadLookupFiles();

    // Get modal elements
    const modal = document.getElementById("uploadModal");
    const uploadBtn = document.getElementById("uploadBtn");
    const closeModalBtn = modal.querySelector(".modal-close");
    const fileInput = document.getElementById("fileInput");
    const fileNameDisplay = document.getElementById("fileName");
    const uploadForm = document.getElementById("uploadForm");

    // Show modal when upload button is clicked
    uploadBtn.addEventListener('click', function() {
        modal.classList.add("is-active");
    });

    // Close modal when "x" is clicked
    closeModalBtn.addEventListener('click', function() {
        modal.classList.remove("is-active");
    });

    // Close modal when clicking outside of the modal content
    modal.querySelector(".modal-background").addEventListener('click', function() {
        modal.classList.remove("is-active");
    });

    // Update file name when a file is selected
    fileInput.addEventListener('change', function() {
        if (fileInput.files.length > 0) {
            fileNameDisplay.textContent = fileInput.files[0].name;
        } else {
            fileNameDisplay.textContent = "No file chosen";
        }
    });

    // Handle file upload form submission
    uploadForm.addEventListener('submit', function(event) {
        event.preventDefault();

        const allowedExtensions = ['sqlite3', 'parquet', 'csv', 'json'];
        const file = fileInput.files[0];

        if (!file) {
            showError("Please select a file to upload.");
            return;
        }

        const fileExt = file.name.split('.').pop().toLowerCase();

        if (!allowedExtensions.includes(fileExt)) {
            showError("Invalid file type. Please upload a .sqlite3, .parquet, .csv, or .json file.");
            return;
        }

        const formData = new FormData();
        formData.append("file", file);

        axios.post('/upload_file', formData, {
            headers: {
                'Content-Type': 'multipart/form-data'
            }
        })
        .then(response => {
            if (response.data.status === 'success') {
                showNotification('File uploaded successfully.');
                modal.classList.remove("is-active");
                loadLookupFiles(); // Reload the lookup files list after upload
                fileInput.value = ''; // Reset file input
                fileNameDisplay.textContent = "No file chosen";
            } else {
                showError('File upload failed: ' + response.data.message);
            }
        })
        .catch(error => {
            console.error('[x] Error uploading file:', error);
            showError('Error uploading file.');
        });
    });

    /**
     * Show the loading overlay.
     */
    function showLoadingOverlay() {
        const overlay = document.getElementById('loading-overlay');
        if (overlay) {
            overlay.classList.remove('is-hidden');
        }
    }

    /**
     * Hide the loading overlay.
     */
    function hideLoadingOverlay() {
        const overlay = document.getElementById('loading-overlay');
        if (overlay) {
            overlay.classList.add('is-hidden');
        }
    }

    /**
     * Load lookup files from the server.
     */
    function loadLookupFiles() {
        showLoadingOverlay(); // Show the loading overlay

        axios.get('/get_lookup_files')
            .then(response => {
                const data = response.data;
                if (data.status === 'success') {
                    const lookupFiles = data.files;
                    renderLookupTable(lookupFiles);
                } else {
                    showError('Error loading lookup files.');
                }
            })
            .catch(error => {
                console.error('[x] Error loading lookup files:', error);
                showError('Error loading lookup files.');
            })
            .finally(() => {
                hideLoadingOverlay(); // Hide the loading overlay
            });
    }

    /**
     * Render the lookup files table.
     * @param {Array} files - Array of lookup file objects.
     */
    function renderLookupTable(files) {
        const tableContainer = document.getElementById('lookup-table');
        tableContainer.innerHTML = ''; // Clear previous content

        if (files.length === 0) {
            tableContainer.innerHTML = '<p>No lookup files found.</p>';
            return;
        }

        // Create a wrapper div with Bulma's 'table-container' class
        const tableWrapper = document.createElement('div');
        tableWrapper.className = 'table-container';

        const table = document.createElement('table');
        table.className = 'table is-striped is-hoverable is-fullwidth';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');

        const headers = [
            'Filename', 'Filepath', 'Datetime Created', 'Last Updated',
            'Filesize', 'File Permissions', 'Row Count', 'Actions'
        ];

        headers.forEach(headerText => {
            const th = document.createElement('th');
            th.innerText = headerText;
            headerRow.appendChild(th);
        });

        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');

        files.forEach(file => {
            const row = document.createElement('tr');

            // Filename
            const filenameCell = document.createElement('td');
            filenameCell.textContent = file.filename;
            row.appendChild(filenameCell);

            // Filepath
            const filepathCell = document.createElement('td');
            filepathCell.textContent = file.filepath;
            row.appendChild(filepathCell);

            // Datetime Created
            const createdAtCell = document.createElement('td');
            createdAtCell.textContent = file.created_at;
            row.appendChild(createdAtCell);

            // Last Updated
            const updatedAtCell = document.createElement('td');
            updatedAtCell.textContent = file.updated_at;
            row.appendChild(updatedAtCell);

            // Filesize
            const filesizeCell = document.createElement('td');
            filesizeCell.textContent = file.filesize;
            row.appendChild(filesizeCell);

            // File Permissions
            const permissionsCell = document.createElement('td');
            permissionsCell.textContent = file.permissions;
            row.appendChild(permissionsCell);

            // Row Count
            const rowCountCell = document.createElement('td');
            rowCountCell.textContent = file.row_count;
            row.appendChild(rowCountCell);

            // Actions
            const actionsCell = document.createElement('td');

            // View Link
            const viewLink = document.createElement('a');
            viewLink.href = `/view_lookup?file=${encodeURIComponent(file.filepath)}`;
            viewLink.target = "_blank";
            viewLink.textContent = 'View';
            actionsCell.appendChild(viewLink);

            // Separator
            actionsCell.innerHTML += ' | ';

            // Delete Link
            const deleteLink = document.createElement('a');
            deleteLink.href = '#';
            deleteLink.textContent = 'Delete';
            deleteLink.className = 'delete-btn';
            deleteLink.dataset.filepath = file.filepath;
            actionsCell.appendChild(deleteLink);

            // Separator
            actionsCell.innerHTML += ' | ';

            // Clone Link
            const cloneLink = document.createElement('a');
            cloneLink.href = '#';
            cloneLink.textContent = 'Clone';
            cloneLink.className = 'clone-btn';
            cloneLink.dataset.filepath = file.filepath;
            actionsCell.appendChild(cloneLink);

            row.appendChild(actionsCell);

            tbody.appendChild(row);
        });

        table.appendChild(tbody);

        // Append the table to the wrapper
        tableWrapper.appendChild(table);

        // Append the wrapper to the table container
        tableContainer.appendChild(tableWrapper);
    }

    // Event delegation for delete and clone actions
    document.getElementById('lookup-table').addEventListener('click', function(event) {
        const target = event.target;

        if (target.classList.contains('delete-btn')) {
            event.preventDefault();
            const filepath = target.dataset.filepath;
            confirmDelete(filepath);
        } else if (target.classList.contains('clone-btn')) {
            event.preventDefault();
            const filepath = target.dataset.filepath;
            cloneFile(filepath);
        }
    });

    /**
     * Confirm and delete a lookup file.
     * @param {string} filepath - The filepath of the file to delete.
     */
    function confirmDelete(filepath) {
        const confirmed = confirm("Are you sure you want to delete this file?");
        if (confirmed) {
            axios.post('/delete_lookup_file', { filepath: filepath })
                .then(response => {
                    if (response.data.status === 'success') {
                        showNotification('File deleted successfully.');
                        loadLookupFiles(); // Reload the list after deletion
                    } else {
                        showError('Error deleting file: ' + response.data.message);
                    }
                })
                .catch(error => {
                    console.error('[x] Error deleting file:', error);
                    showError('Error deleting file.');
                });
        }
    }

    /**
     * Clone a lookup file.
     * @param {string} filepath - The filepath of the file to clone.
     */
    function cloneFile(filepath) {
        const newName = prompt("Enter a new name for the clone (leave blank to use default):");
        if (newName === null) return; // User cancelled the prompt

        const filename = newName.trim() !== '' ? newName.trim() : `${filepath.split('/').pop()}_copy`;

        axios.post('/clone_lookup_file', { filepath: filepath, new_name: filename })
            .then(response => {
                if (response.data.status === 'success') {
                    showNotification('File cloned successfully.');
                    loadLookupFiles(); // Reload the list after cloning
                } else {
                    showError('Error cloning file: ' + response.data.message);
                }
            })
            .catch(error => {
                console.error('[x] Error cloning file:', error);
                showError('Error cloning file.');
            });
    }

    /**
     * Display a notification using Bulma's notification component.
     * @param {string} message - The message to display.
     */
    // Notifications are provided by common.js
});
