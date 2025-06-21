// schedule_input.js

document.addEventListener("DOMContentLoaded", function () {
    const form = document.getElementById('scheduled-input-form');
    const testScriptBtn = document.getElementById('test-script-btn');
    const populateDataBtn = document.getElementById('populate-data-btn');
    const codeTextarea = document.getElementById('code');
    const dfSummaryContainer = document.getElementById('df-summary-container');
    const apiUrlInput = document.getElementById('api_url');
    const jsonDisplayContainer = document.getElementById('json-display-container');
    const notificationContainer = document.getElementById('notification-container');

    // Attach the validation function to form submit
    form.addEventListener('submit', function (event) {
        event.preventDefault(); // Prevent the default form submission

        // Validate the form
        if (!validateForm()) {
            return;
        }

        const formData = new FormData(form);

        // Convert FormData to JSON, handling the 'disabled' checkbox
        const jsonData = {};
        formData.forEach((value, key) => {
            if (key === 'disabled') {
                jsonData[key] = true; // Checkbox is checked
            } else {
                jsonData[key] = value;
            }
        });

        // If 'disabled' checkbox is not checked, set it to false
        if (!formData.has('disabled')) {
            jsonData['disabled'] = false;
        }

        // Submit the form using Axios
        axios.post('/commit_scheduled_input', jsonData)
            .then(response => {
                const result = response.data;
                if (response.status === 200 && result.status === 'success') {
                    // Redirect to the scheduled_inputs.html page after a successful save
                    window.location.href = '/scheduled_inputs.html';
                } else {
                    // If there is an error, display the error message
                    showError(result.message || 'An error occurred while saving the scheduled input.');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showError('An unexpected error occurred.');
            });
    });

    // Function to validate input fields
    function validateForm() {
        const cronSchedule = document.getElementById('cron_schedule').value;

        // Simple validation pattern for cron schedule
        const cronPattern = /^[\w\s*\/,-]+$/;

        if (!cronPattern.test(cronSchedule)) {
            showError('Invalid cron schedule format.');
            return false;
        }

        return true;
    }

    // Function to escape HTML special characters
    function escapeHtml(text) {
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;',
            '`': '&#096;',
        };
        return text.replace(/[&<>"'`]/g, function(m) { return map[m]; });
    }

    // Event listener for the Populate Sample Data button
    populateDataBtn.addEventListener('click', function () {
        const apiUrl = apiUrlInput.value.trim();

        if (!apiUrl) {
            showError('Please enter an API URL.');
            return;
        }

        if (!isValidUrl(apiUrl)) {
            showError('Please enter a valid API URL.');
            return;
        }

        // Fetch the JSON data from the API
        axios.post('/fetch_api_data', { api_url: apiUrl })
            .then(response => {
                const data = response.data;
                if (data.status === 'success') {
                    // Limit the JSON data to 2000 lines
                    let jsonString = JSON.stringify(data.api_data, null, 2);
                    let lines = jsonString.split('\n');
                    if (lines.length > 2000) {
                        lines = lines.slice(0, 2000);
                        lines.push('... (truncated)');
                        jsonString = lines.join('\n');
                    }

                    // Display the prettified JSON in the right column
                    jsonDisplayContainer.innerHTML = `
                        <label class="label">Sample API Data</label>
                        <pre class="json-display">${escapeHtml(jsonString)}</pre>
                    `;
                } else {
                    showError('Error fetching API data: ' + data.message);
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showError('An error occurred while fetching the API data.');
            });
    });

    // Function to validate the API URL
    function isValidUrl(url) {
        try {
            new URL(url);
            return true;
        } catch (_) {
            return false;
        }
    }

    // Event listener for the Test Script button
    testScriptBtn.addEventListener('click', function () {
        const userCode = codeTextarea.value.trim();

        if (!userCode) {
            showError('Please enter your Python code before testing.');
            return;
        }

        // Send the code to the server for testing
        axios.post('/test_scheduled_input', { code: userCode })
            .then(response => {
                const data = response.data;
                if (data.status === 'success') {
                    // Display the DataFrame summary
                    dfSummaryContainer.style.display = 'block';
                    dfSummaryContainer.innerHTML = `
                        <label class="label">DataFrame Summary</label>
                        <pre>${escapeHtml(data.df_summary)}</pre>
                    `;
                } else {
                    dfSummaryContainer.style.display = 'none';
                    showError('Error testing script: ' + data.message);
                }
            })
            .catch(error => {
                console.error('Error:', error);
                dfSummaryContainer.style.display = 'none';
                showError('An error occurred while testing the script.');
            });
    });

    /**
     * Display a notification using Bulma's notification component.
     * @param {string} message - The message to display.
     * @param {string} type - The type of notification ('is-primary', 'is-danger', etc.).
     */
    function showNotification(message, type = 'is-primary') {
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.innerHTML = `
            <button class="delete"></button>
            ${message}
        `;
        notificationContainer.appendChild(notification);

        // Automatically remove the notification after 5 seconds
        setTimeout(() => {
            notification.remove();
        }, 5000);

        // Add event listener to the delete button
        notification.querySelector('.delete').addEventListener('click', () => {
            notification.remove();
        });
    }

    /**
     * Display an error notification.
     * @param {string} message - The error message to display.
     */
    function showError(message) {
        showNotification(message, 'is-danger');
    }

    /**
     * Display a success notification.
     * @param {string} message - The success message to display.
     */
    function showSuccess(message) {
        showNotification(message, 'is-success');
    }
});
