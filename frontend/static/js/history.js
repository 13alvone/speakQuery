// history.js

document.addEventListener('DOMContentLoaded', function() {
    loadJobFiles();

    // Event delegation for dynamically created elements
    document.getElementById('loadjob-table').addEventListener('click', function(event) {
        const target = event.target;

        if (target.matches('.load-btn')) {
            const filename = target.dataset.filename;
            loadJob(filename);
        }
    });

    /**
     * Load job files from the server.
     */
    function loadJobFiles() {
        axios.get('/get_loadjob_files')
            .then(response => {
                const data = response.data;
                if (data.status === 'success') {
                    const loadjobFiles = data.files;
                    renderLoadJobTable(loadjobFiles);
                } else {
                    showError('Error loading load job files.');
                }
            })
            .catch(error => {
                console.error('[x] Error loading load job files:', error);
                showError('Error loading load job files.');
            });
    }

    /**
     * Render the load job files table.
     * @param {Array} files - Array of load job file objects.
     */
    function renderLoadJobTable(files) {
        const tableContainer = document.getElementById('loadjob-table');
        tableContainer.innerHTML = ''; // Clear previous content

        if (files.length === 0) {
            tableContainer.innerHTML = '<p>No load job files found.</p>';
            return;
        }

        const table = document.createElement('table');
        table.className = 'table is-striped is-hoverable is-fullwidth';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');

        const headers = [
            'Filename', 'Datetime Created', 'Last Updated', 'Filesize', 'Actions'
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
            filesizeCell.textContent = formatFileSize(file.filesize);
            row.appendChild(filesizeCell);

            // Actions
            const actionsCell = document.createElement('td');
            const loadButton = document.createElement('button');
            loadButton.className = 'button load-btn is-link';
            loadButton.textContent = 'Load';
            loadButton.dataset.filename = file.filename;
            actionsCell.appendChild(loadButton);
            row.appendChild(actionsCell);

            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        tableContainer.appendChild(table);
    }

    /**
     * Format file size from bytes to a more readable format.
     * @param {number} bytes - File size in bytes.
     * @returns {string} - Formatted file size.
     */
    function formatFileSize(bytes) {
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes === 0) return '0 Bytes';
        const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)), 10);
        return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
    }

    /**
     * Load a job by fetching the original query and saving the combined command to localStorage.
     * @param {string} filename - The filename of the load job.
     */
    function loadJob(filename) {
        // Show loading notification
        showNotification('Fetching original query...', 'is-info');

        // Fetch the original query from the backend
        axios.get(`/get_query_for_loadjob/${encodeURIComponent(filename)}`)
            .then(response => {
                if (response.data.query) {
                    const originalQuery = response.data.query;

                    // Comment out each line of the original query
                    const commentedQuery = originalQuery
                        .split('\n')
                        .map(line => `# ${line}`)
                        .join('\n');

                    // Combine the load job command with the commented original query
                    const loadJobCommand = `| loadjob '${filename}'\n${commentedQuery}`;

                    // Save to localStorage
                    localStorage.setItem('savedQuery', loadJobCommand);

                    // Update notification
                    showNotification(`Load job command saved:<br><code>${loadJobCommand}</code><br>Redirecting to the Search page...`, 'is-success');

                    // Redirect after a short delay to allow the user to see the notification
                    setTimeout(() => {
                        window.location.href = '/';  // Redirect to the Search (index.html) page
                    }, 3000);
                } else {
                    showError('Original query not found for this load job.');
                }
            })
            .catch(error => {
                console.error('[x] Error fetching original query:', error);
                showError('Error fetching original query.');
            });
    }

    /**
     * Display a notification using Bulma's notification component.
     * @param {string} message - The message to display.
     * @param {string} type - The Bulma notification type (e.g., 'is-primary', 'is-danger').
     */
    // Notifications are provided by common.js
});
