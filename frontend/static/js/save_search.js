// save_search.js

(function() {
    document.addEventListener("DOMContentLoaded", function() {
        const throttleSelect = document.querySelector('select[name="throttle"]');
        const sendEmailSelect = document.querySelector('select[name="send_email"]');
        const throttleFieldsContainer = document.querySelector('.throttle-fields');
        const emailFieldsContainer = document.querySelector('.email-fields');
        const form = document.getElementById('saved-search-form');

        // Throttle input fields
        const throttleByInput = document.querySelector('input[name="throttle_by"]');
        const throttleTimePeriodInput = document.querySelector('input[name="throttle_time_period"]');

        // Email input fields
        const emailAddressInput = document.querySelector('input[name="email_address"]');
        const emailContentTextarea = document.querySelector('textarea[name="email_content"]');

        // Function to toggle related fields based on select values
        function toggleFields() {

            // Throttle Fields
            if (throttleSelect.value === 'yes') {
                throttleFieldsContainer.classList.remove('is-hidden');
                throttleByInput.disabled = false;
                throttleTimePeriodInput.disabled = false;
            } else {
                throttleFieldsContainer.classList.add('is-hidden');
                throttleByInput.disabled = true;
                throttleTimePeriodInput.disabled = true;
            }

            // Send Email Fields
            if (sendEmailSelect.value === 'yes') {
                emailFieldsContainer.classList.remove('is-hidden');
                emailAddressInput.disabled = false;
                emailContentTextarea.disabled = false;
            } else {
                emailFieldsContainer.classList.add('is-hidden');
                emailAddressInput.disabled = true;
                emailContentTextarea.disabled = true;
            }
        }

        // Initial toggle based on default select values
        toggleFields();

        // Event listeners for select changes
        throttleSelect.addEventListener('change', toggleFields);
        sendEmailSelect.addEventListener('change', toggleFields);

        // Validate form fields and submit via AJAX
        async function validateAndSubmitForm(event) {
            event.preventDefault();  // Prevent the default form submission

            // Validate unique title
            const titleInput = document.querySelector('input[name="title"]');
            const title = titleInput.value.trim();

            if (title === "") {
                displayError("Title cannot be empty.");
                return;
            }

            try {
                const titleCheckResponse = await axios.post('/check_title_unique', { title: title });
                const titleCheckResult = titleCheckResponse.data;

                if (!titleCheckResult.is_unique) {
                    displayError(`The title "${title}" already exists. Please choose a different title.`);
                    return;
                }
            } catch (error) {
                console.error('Failed to validate the title uniqueness:', error);
                displayError('Failed to validate the title uniqueness.');
                return;
            }

            // Throttle validation
            if (throttleSelect.value === 'yes') {
                if (!throttleByInput.value.trim()) {
                    displayError('Throttle By must be filled out when Throttle is set to Yes.');
                    return;
                }
                if (!throttleTimePeriodInput.value.trim()) {
                    displayError('Throttle Time Period must be filled out when Throttle is set to Yes.');
                    return;
                }
            }

            // Send Email validation
            if (sendEmailSelect.value === 'yes') {
                if (!emailAddressInput.value.trim()) {
                    displayError('Email Address must be filled out when Send Email is set to Yes.');
                    return;
                }
                if (!emailContentTextarea.value.trim()) {
                    displayError('Email Content must be filled out when Send Email is set to Yes.');
                    return;
                }
            }

            // Collect form data
            const formData = new FormData(form);
            const jsonData = {};
            formData.forEach((value, key) => {
                jsonData[key] = value;
            });

            try {
                const response = await axios.post('/commit_saved_search', jsonData);
                const result = response.data;

                if (response.status === 200 && result.status === 'success') {
                    // If the save is successful, redirect to /saved_searches.html
                    window.location.href = '/saved_searches.html';
                } else {
                    // If there is an error, display the error message
                    displayError(result.message || 'An error occurred while saving the search.');
                }
            } catch (error) {
                console.error('An unexpected error occurred while saving the search:', error);
                displayError('An unexpected error occurred while saving the search.');
            }
        }

        form.addEventListener('submit', validateAndSubmitForm);

        /**
         * Display an error notification using Bulma's notification component.
         * @param {string} message - The error message to display.
         */
        function displayError(message) {
            const container = document.getElementById('notification-container');
            const notification = document.createElement('div');
            notification.className = 'notification is-danger';
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
})();
