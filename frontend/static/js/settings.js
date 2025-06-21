// frontend/static/js/settings.js

document.addEventListener('DOMContentLoaded', function() {
    loadSettings();

    const settingsForm = document.getElementById('settings-form');
    settingsForm.addEventListener('submit', function(event) {
        event.preventDefault();
        updateSettings();
    });

    // Handle reset button to reload original settings
    settingsForm.addEventListener('reset', function(event) {
        event.preventDefault();
        loadSettings();
    });

    // Initialize auto-expanding inputs
    initializeAutoExpand();
});

/**
 * Initialize auto-expanding input fields.
 */
function initializeAutoExpand() {
    const inputs = document.querySelectorAll('.monospaced-input');

    inputs.forEach(input => {
        input.addEventListener('input', () => {
            adjustInputWidth(input);
        });

        // Initialize width based on current value
        adjustInputWidth(input);
    });
}

/**
 * Adjust the width of the input field based on its content.
 * @param {HTMLElement} input - The input element to adjust.
 */
function adjustInputWidth(input) {
    const mirror = document.getElementById('input-mirror');
    if (!mirror) return;

    // Set mirror's font properties to match the input
    const style = window.getComputedStyle(input);
    mirror.style.fontFamily = style.fontFamily;
    mirror.style.fontSize = style.fontSize;
    mirror.style.fontWeight = style.fontWeight;
    mirror.style.fontStyle = style.fontStyle;
    mirror.style.letterSpacing = style.letterSpacing;
    mirror.style.padding = style.padding;
    mirror.style.border = style.border;

    // Set the mirror's text content to the input's value
    mirror.textContent = input.value || input.placeholder;

    // Calculate the new width
    const newWidth = mirror.offsetWidth + 20; // Adding extra space for cursor

    // Set the input's width, with a maximum limit
    input.style.width = `${newWidth}px`;
}

/**
 * Fetch current settings from the server and populate the form.
 */
function loadSettings() {
    axios.get('/get_settings')
        .then(response => {
            if (response.data.status === 'success') {
                const settings = response.data.settings;
                populateForm(settings);
            } else {
                showError('Failed to load settings.');
            }
        })
        .catch(error => {
            console.error('Error fetching settings:', error);
            handleError(error, 'Error fetching settings.');
        });
}

/**
 * Populate the settings form with fetched data.
 * @param {Object} settings - Key-value pairs of settings.
 */
function populateForm(settings) {
    for (const key in settings) {
        if (settings.hasOwnProperty(key)) {
            const input = document.getElementById(key);
            if (input) {
                if (key === 'MAX_CONTENT_LENGTH') {
                    // Convert bytes to gigabytes for display
                    input.value = (parseInt(settings[key], 10) / (1024 ** 3)).toFixed(2);
                } else if (key === 'ALLOWED_EXTENSIONS') {
                    // Ensure no spaces after commas
                    input.value = settings[key].split(',').map(ext => ext.trim()).join(',');
                } else {
                    input.value = settings[key];
                }

                // Trigger input event to adjust width
                input.dispatchEvent(new Event('input'));
            }
        }
    }
}

/**
 * Collect form data, convert Max Content Length from GB to bytes, and send an update request to the server.
 */
function updateSettings() {
    const form = document.getElementById('settings-form');
    const formData = new FormData(form);
    const settings = {};

    for (const pair of formData.entries()) {
        settings[pair[0]] = pair[1];
    }

    // Convert Max Content Length from GB to bytes
    if (settings['MAX_CONTENT_LENGTH']) {
        const gbValue = parseFloat(settings['MAX_CONTENT_LENGTH']);
        if (isNaN(gbValue) || gbValue < 0) {
            showError('Max Content Length must be a positive number.');
            return;
        }
        settings['MAX_CONTENT_LENGTH'] = Math.round(gbValue * (1024 ** 3)); // Convert GB to bytes
    }

    // Validate form data
    if (!validateForm(settings)) {
        return;
    }

    axios.post('/update_settings', { settings: settings })
        .then(response => {
            if (response.data.status === 'success') {
                showNotification('Settings updated successfully.', false);
            } else {
                showNotification('Failed to update settings.', true);
            }
        })
        .catch(error => {
            console.error('Error updating settings:', error);
            handleError(error, 'Error updating settings.');
        });
}

/**
 * Validate form fields before submission.
 * @returns {boolean} - True if valid, false otherwise.
 */
function validateForm(settings) {
    // Example: Ensure directory paths are not empty and follow a specific pattern
    const directoryFields = ['UPLOAD_FOLDER', 'TEMP_DIR', 'LOOKUP_DIR', 'LOADJOB_DIR', 'INDEXES_DIR'];
    for (const field of directoryFields) {
        if (!settings[field] || !isValidPath(settings[field])) {
            showError(`Invalid path for ${field.replace('_', ' ')}.`);
            return false;
        }
    }

    // Validate ALLOWED_EXTENSIONS
    const extensions = settings['ALLOWED_EXTENSIONS'].split(',');
    if (extensions.some(ext => !/^[a-z0-9]+$/i.test(ext.trim()))) {
        showError('Allowed Extensions must be comma-separated alphanumeric values.');
        return false;
    }

    // Validate Saved Searches Database Filename
    const dbFilename = settings['SAVED_SEARCHES_DB'];
    if (!/^[a-zA-Z0-9_\-\.]+$/.test(dbFilename)) {
        showError('Saved Searches Database Filename contains invalid characters.');
        return false;
    }

    // Validate Scheduled Inputs Database Filename
    const scheduledDbFilename = settings['SCHEDULED_INPUTS_DB'];
    if (!/^[a-zA-Z0-9_\-\.]+$/.test(scheduledDbFilename)) {
        showError('Scheduled Inputs Database Filename contains invalid characters.');
        return false;
    }

    // Additional validations as needed
    return true;
}

/**
 * Simple path validation function.
 * @param {string} path - The directory path to validate.
 * @returns {boolean} - True if valid, false otherwise.
 */
function isValidPath(path) {
    // Implement your path validation logic here
    // For simplicity, we'll check that it doesn't contain illegal characters
    return /^[a-zA-Z0-9_\-\/\\]+$/.test(path);
}
