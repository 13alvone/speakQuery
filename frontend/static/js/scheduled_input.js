// scheduled_input.js

document.addEventListener("DOMContentLoaded", function() {
    // Apply styling to readonly fields (if any)
    document.querySelectorAll('input[readonly]').forEach(function(input) {
        input.classList.add('readonly-field');
    });

    // Select relevant form elements
    const overwriteSelect = document.querySelector('select[name="overwrite"]');
    const subdirectoryInput = document.querySelector('input[name="subdirectory"]');
    const form = document.getElementById('scheduledInputForm');
    const inputId = form.getAttribute('data-input-id'); // Retrieve input_id from data attribute

    // Function to toggle visibility of related fields based on select values (if needed)
    function toggleFields() {
        // Example: If there are fields that depend on the overwrite selection, handle them here
        // Currently, no additional fields are dependent on 'overwrite', but this can be expanded
    }

    // Initial toggle based on current select values
    toggleFields();

    // Add event listeners to select elements to handle dynamic form changes
    overwriteSelect.addEventListener('change', toggleFields);

    // Notifications are provided by common.js

    // Function to handle form submission
    async function handleFormSubmission(event) {
        event.preventDefault(); // Prevent default form submission

        // Gather form data
        const formData = {
            title: document.getElementById('title').value.trim(),
            description: document.getElementById('description').value.trim(),
            code: document.getElementById('code').value.trim(),
            cron_schedule: document.getElementById('cron_schedule').value.trim(),
            overwrite: document.getElementById('overwrite').value,
            subdirectory: document.getElementById('subdirectory').value.trim(),
            disabled: document.getElementById('disabled').checked
        };

        // Basic client-side validation
        if (!formData.title || !formData.description || !formData.code || !formData.cron_schedule || !formData.subdirectory) {
            showError("Please fill in all required fields.");
            return;
        }

        // Additional conditional validations can be added here if necessary

        try {
            // Send POST request to the backend to update the scheduled input
            const response = await axios.post(`/update_scheduled_input/${inputId}`, formData);

            if (response.data.status === 'success') {
                // On success, redirect to scheduled_inputs.html
                window.location.href = '/scheduled_inputs.html';
            } else {
                // On failure, stay on the page and show error message
                showError(response.data.message || 'An error occurred while updating the scheduled input.');
            }
        } catch (error) {
            console.error('Error updating scheduled input:', error);
            // Show a generic error message
            showError('An unexpected error occurred while updating the scheduled input.');
        }
    }

    // Attach the form submission handler
    form.addEventListener('submit', handleFormSubmission);
});
