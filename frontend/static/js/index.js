// index.js

document.addEventListener('DOMContentLoaded', function() {
    let currentPage = 1;
    let totalRows = 0;
    let rowsPerPage = 100;
    let queryResults = [];
    let columnNames = [];
    let requestId = null;

    // Load the directory tree structure on page load
    loadDirectoryTree();

    const savedQuery = localStorage.getItem('savedQuery');
    if (savedQuery) {
        document.getElementById('query').value = savedQuery;
        autoExpandQueryBox();
    }

    // Handle the "Run Query" button click
    document.getElementById('run-query-btn').addEventListener('click', function(e) {
        e.preventDefault(); // Prevent default action to avoid page reload
        const query = document.getElementById('query').value;
        axios.post('/run_query', { query: query })
            .then(response => {
                if (response.data.status === 'success') {
                    queryResults = response.data.results;
                    columnNames = response.data.column_names;
                    totalRows = queryResults.length;

                    // Retrieve and store request_id
                    requestId = response.data.request_id;
                    if (requestId) {
                        localStorage.setItem('requestId', requestId);
                    } else {
                        console.error("[x] No request_id found in the response");
                    }

                    document.getElementById('row-count').innerText = `[i] Total Results: ${totalRows}`;
                    document.getElementById('directory-tree').style.display = 'none'; // Hide directory tree
                    renderResults(currentPage);
                    updatePaginationInfo();
                    document.getElementById('pagination-top').style.display = 'flex';
                    document.getElementById('pagination-bottom').style.display = 'flex';

                    // Reinsert the original query into the query box and store it locally
                    const queryBox = document.getElementById('query');
                    if (queryBox) {
                        queryBox.value = query;
                        localStorage.setItem('savedQuery', query);
                        autoExpandQueryBox();
                    }
                } else {
                    console.error('[x] Query error:', response.data.message);
                    showError('Error: ' + response.data.message);
                }
            })
            .catch(error => {
                console.error('[x] Error running query:', error);
                if (error.response && error.response.data && error.response.data.message) {
                    showError('Error: ' + error.response.data.message);
                } else {
                    showError('Error: An unknown error occurred.');
                }
            });
    });

    // Handle the "Save Search" button click
    document.getElementById('save-search-btn').addEventListener('click', function(e) {
        e.preventDefault(); // Prevent default action
        const requestId = localStorage.getItem('requestId');
        const savedQuery = document.getElementById('query').value;

        if (!requestId) {
            showError('No request ID found. Please run a query first.');
            return;
        }

        // Populate the hidden form fields
        document.getElementById('form-request-id').value = requestId;
        document.getElementById('form-saved-query').value = savedQuery;

        // Submit the form
        document.getElementById('save-search-form').submit();
    });

    // Pagination Buttons Event Listeners
    document.getElementById('prev-page-btn-top').addEventListener('click', function() {
        if (currentPage > 1) {
            currentPage--;
            renderResults(currentPage);
            updatePaginationInfo();
        }
    });

    document.getElementById('next-page-btn-top').addEventListener('click', function() {
        if (currentPage * rowsPerPage < totalRows) {
            currentPage++;
            renderResults(currentPage);
            updatePaginationInfo();
        }
    });

    document.getElementById('prev-page-btn-bottom').addEventListener('click', function() {
        if (currentPage > 1) {
            currentPage--;
            renderResults(currentPage);
            updatePaginationInfo();
        }
    });

    document.getElementById('next-page-btn-bottom').addEventListener('click', function() {
        if (currentPage * rowsPerPage < totalRows) {
            currentPage++;
            renderResults(currentPage);
            updatePaginationInfo();
        }
    });

    // Save as CSV and JSON Buttons Event Listeners
    document.getElementById('save-csv-btn').addEventListener('click', function() {
        saveResults('csv');
    });

    document.getElementById('save-json-btn').addEventListener('click', function() {
        saveResults('json');
    });

    /**
     * Save query results as CSV or JSON.
     * @param {string} format - The format to save ('csv' or 'json').
     */
    function saveResults(format) {
        const requestId = localStorage.getItem('requestId');
        if (!requestId) {
            console.error('[x] No request ID found. Please run a query first.');
            showError('No request ID found. Please run a query first.');
            return;
        }

        const userChoice = confirm("Do you want to save the entire dataset?");
        const saveType = userChoice ? 'all' : 'current_page';

        const start = (currentPage - 1) * rowsPerPage;
        const end = Math.min(currentPage * rowsPerPage, totalRows);

        axios.post('/save_results', {
            request_id: requestId,
            save_type: saveType,
            format: format,
            start: start,
            end: end
        })
        .then(response => {
            if (response.data.status === 'success') {
                const link = document.createElement('a');
                link.href = response.data.file_url;
                link.download = response.data.file_name;
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            } else {
                console.error('[x] Save error:', response.data.message);
                showError(`Save error: ${response.data.message}`);
            }
        })
        .catch(error => {
            console.error('[x] Error saving results:', error);
            showError('Error saving results.');
        });
    }

    /**
     * Render the query results table for the given page.
     * @param {number} page - The current page number.
     */
    function renderResults(page) {
        try {
            const start = (page - 1) * rowsPerPage;
            const end = Math.min(start + rowsPerPage, totalRows);
            const rows = queryResults.slice(start, end);

            const resultsContainer = document.getElementById('results');
            resultsContainer.innerHTML = '';

            const table = document.createElement('table');
            table.className = 'table is-striped is-hoverable';

            const thead = document.createElement('thead');
            const headerRow = document.createElement('tr');
            columnNames.forEach(col => {
                const th = document.createElement('th');
                th.textContent = col;
                headerRow.appendChild(th);
            });
            thead.appendChild(headerRow);
            table.appendChild(thead);

            const tbody = document.createElement('tbody');
            rows.forEach(row => {
                const tr = document.createElement('tr');
                columnNames.forEach(col => {
                    const td = document.createElement('td');
                    const value = row[col] !== undefined && row[col] !== null ? row[col] : 'N/A';
                    td.textContent = value;
                    tr.appendChild(td);
                });
                tbody.appendChild(tr);
            });
            table.appendChild(tbody);

            resultsContainer.appendChild(table);
        } catch (error) {
            console.error('[x] Error rendering results:', error);
            showError('Error rendering results.');
        }
    }

    /**
     * Update the pagination information display.
     */
    function updatePaginationInfo() {
        const startEntry = (currentPage - 1) * rowsPerPage + 1;
        const endEntry = Math.min(currentPage * rowsPerPage, totalRows);
        const paginationInfo = `Page: ${currentPage} (Entries: ${startEntry}-${endEntry})`;
        document.getElementById('row-count').innerText = `${paginationInfo}\nTotal Results: ${totalRows}`;
    }

    /**
     * Automatically expand the query textarea based on its content.
     */
    function autoExpandQueryBox() {
        const queryBox = document.getElementById('query');
        queryBox.style.height = 'auto';  // Reset the height
        queryBox.style.height = (queryBox.scrollHeight) + 'px';  // Set new height based on content
    }

    // Attach input event listener for auto-expanding the query box
    document.getElementById('query').addEventListener('input', autoExpandQueryBox);

    /**
     * Load the directory tree structure from the server.
     */
    function loadDirectoryTree() {
        axios.get('/get_directory_tree')
            .then(response => {
                const data = response.data;
                if (data.status === 'success') {
                    const treeData = data.tree;
                    const treeHtml = buildTreeHtml(treeData);
                    document.getElementById('directory-tree').innerHTML = treeHtml;
                    attachFileClickEvents();
                } else {
                    showError('Error loading directory structure.');
                }
            })
            .catch(error => {
                console.error('[x] Error loading directory tree:', error);
                showError('Error loading directory structure.');
            });
    }

    /**
     * Build the HTML for the directory tree.
     * @param {Object} tree - The directory tree data.
     * @returns {string} - The HTML string for the directory tree.
     */
    function buildTreeHtml(node) {
        let html = '<ul>';

        if (node.files) {
            node.files.forEach(f => {
                html += `<li><a href="#" class="file-link" data-file="${f.path}">${f.name}</a></li>`;
            });
        }

        if (node.dirs) {
            for (const [dirName, child] of Object.entries(node.dirs)) {
                html += `<li><details><summary>${dirName}</summary>`;
                html += buildTreeHtml(child);
                html += '</details></li>';
            }
        }

        html += '</ul>';
        return html;
    }

    /**
     * Attach click event listeners to file links in the directory tree.
     */
    function attachFileClickEvents() {
        const fileLinks = document.querySelectorAll('.file-link');
        fileLinks.forEach(link => {
            link.addEventListener('click', function(e) {
                e.preventDefault();
                const filePath = this.getAttribute('data-file');
                const query = `index="${filePath}"\n`;
                document.getElementById('query').value = query;
                autoExpandQueryBox();
            });
        });
    }

    // Toggle tree visibility
    document.getElementById('tree-toggle').addEventListener('click', function() {
        const tree = document.getElementById('directory-tree');
        if (tree.style.display === 'none' || tree.style.display === '') {
            tree.style.display = 'block';
            this.textContent = 'Hide Files';
        } else {
            tree.style.display = 'none';
            this.textContent = 'Show Files';
        }
    });

    // Set the syntax example
    document.getElementById('syntax-example').textContent = `
index="output_parquets/*"
(level="ERROR" OR level="CRITICAL") AND x>4
earliest="2024-01-04" latest="2024-01-05"
| eval test="test"
| eval t2="header_3"
| search header_1>= 400 AND header_2>= 500 AND (header_3 == "e" OR t2 == "that")
    `;

    /**
     * Display a notification using Bulma's notification component.
     * @param {string} message - The message to display.
     * @param {string} type - The type of notification ('is-primary', 'is-danger', etc.).
     */
    // Notifications are provided by common.js
});
