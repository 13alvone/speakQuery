// saved_search.js

document.addEventListener("DOMContentLoaded", function() {
    // Apply styling to readonly fields (if any)
    document.querySelectorAll('input[readonly]').forEach(function(input) {
        input.classList.add('readonly-field');
    });

    // Select relevant form elements
    const throttleSelect = document.querySelector('select[name="throttle"]');
    const sendEmailSelect = document.querySelector('select[name="send_email"]');
    const throttleByInput = document.querySelector('input[name="throttle_by"]');
    const throttleTimePeriodInput = document.querySelector('input[name="throttle_time_period"]');
    const emailAddressInput = document.querySelector('input[name="email_address"]');
    const emailContentTextarea = document.querySelector('textarea[name="email_content"]');
    const form = document.getElementById('saveSearchForm');
    const searchId = form.getAttribute('data-search-id'); // Retrieve search_id from data attribute

    // Function to toggle visibility of related fields based on select values
    function toggleFields() {
        // Throttle Fields
        if (throttleSelect.value.toLowerCase() === 'yes') {
            throttleByInput.parentElement.parentElement.style.display = 'flex'; // Adjust based on your HTML structure
            throttleTimePeriodInput.parentElement.parentElement.style.display = 'flex';
        } else {
            throttleByInput.value = '';
            throttleTimePeriodInput.value = '';
            throttleByInput.parentElement.parentElement.style.display = 'none';
            throttleTimePeriodInput.parentElement.parentElement.style.display = 'none';
        }

        // Send Email Fields
        if (sendEmailSelect.value.toLowerCase() === 'yes') {
            emailAddressInput.parentElement.parentElement.style.display = 'flex';
            emailContentTextarea.parentElement.parentElement.style.display = 'flex';
        } else {
            emailAddressInput.value = '';
            emailContentTextarea.value = '';
            emailAddressInput.parentElement.parentElement.style.display = 'none';
            emailContentTextarea.parentElement.parentElement.style.display = 'none';
        }
    }

    // Initial toggle based on current select values
    toggleFields();

    // Add event listeners to select elements to handle dynamic form changes
    throttleSelect.addEventListener('change', toggleFields);
    sendEmailSelect.addEventListener('change', toggleFields);

    // Function to display error notifications
    function showError(message) {
        // Implement your notification logic here
        // Example using a simple alert (replace with your modal logic)
        alert(`Error: ${message}`);
    }

    // Function to display success notifications
    function showNotification(message) {
        // Implement your notification logic here
        // Example using a simple alert (replace with your modal logic)
        alert(`Success: ${message}`);
    }

    // Function to handle form submission
    async function handleFormSubmission(event) {
        event.preventDefault(); // Prevent default form submission

        // Gather form data
        const formData = {
            title: document.getElementById('title').value.trim(),
            description: document.getElementById('description').value.trim(),
            query: document.getElementById('query').value.trim(),
            cron_schedule: document.getElementById('cron_schedule').value.trim(),
            trigger: document.getElementById('trigger').value,
            lookback: parseInt(document.getElementById('lookback').value, 10),
            throttle: document.getElementById('throttle').value,
            throttle_time_period: document.getElementById('throttle_time_period').value.trim(),
            throttle_by: document.getElementById('throttle_by').value.trim(),
            event_message: document.getElementById('event_message').value.trim(),
            send_email: document.getElementById('send_email').value,
            email_address: document.getElementById('email_address').value.trim(),
            email_content: document.getElementById('email_content').value.trim(),
            disabled: document.getElementById('disabled').checked
        };

        // Basic client-side validation
        if (!formData.title || !formData.description || !formData.query || !formData.cron_schedule) {
            showError("Please fill in all required fields.");
            return;
        }

        // Additional conditional validations
        if (formData.throttle.toLowerCase() === 'yes') {
            if (!formData.throttle_by || !formData.throttle_time_period) {
                showError("Throttle By and Throttle Time Period must be filled out when Throttle is set to Yes.");
                return;
            }
        }

        if (formData.send_email.toLowerCase() === 'yes') {
            if (!formData.email_address || !formData.email_content) {
                showError("Email Address and Email Content must be filled out when Send Email is set to Yes.");
                return;
            }
        }

        try {
            // Send POST request to the backend to update the saved search
            const response = await axios.post(`/update_saved_search/${searchId}`, formData);

            if (response.data.status === 'success') {
                // On success, redirect to saved_searches.html
                window.location.href = '/saved_searches.html';
            } else {
                // On failure, redirect and show error popup
                window.location.href = '/saved_searches.html';
                showError(response.data.message || 'An error occurred while updating the saved search.');
            }
        } catch (error) {
            console.error('Error updating saved search:', error);
            // Redirect and show a generic error message
            window.location.href = '/saved_searches.html';
            showError('An unexpected error occurred while updating the saved search.');
        }
    }

    // Attach the form submission handler
    form.addEventListener('submit', handleFormSubmission);
});
