// frontend/static/js/common.js

// Common functions for notifications and dropdown handling

/**
 * Function to show notifications.
 * @param {string} message - The message to display.
 * @param {boolean} isError - Flag indicating if the message is an error.
 */
function showNotification(message, isError = false) {
    const container = document.getElementById('notification-container');

    if (!container) {
        console.error('Notification container with ID "notification-container" not found in the DOM.');
        return;
    }

    const notification = document.createElement('div');
    notification.className = `notification ${isError ? 'is-danger' : 'is-primary'}`;
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

/**
 * Function to show error notifications.
 * @param {string} message - The error message to display.
 */
function showError(message) {
    showNotification(message, true);
}

/**
 * Function to close all active notifications.
 */
function closeAllNotifications() {
    const notifications = document.querySelectorAll('#notification-container .notification');
    notifications.forEach(notification => notification.remove());
}

/**
 * Function to handle errors from Axios requests.
 * @param {object} error - The error object from Axios.
 * @param {string} defaultMessage - The default error message to display.
 */
function handleError(error, defaultMessage = 'An unexpected error occurred.') {
    if (error.response) {
        // Server responded with a status other than 2xx
        const serverMessage = error.response.data.message || error.response.statusText;
        showError(`Server Error: ${serverMessage}`);
    } else if (error.request) {
        // Request was made but no response received
        showError('Network Error: Please check your connection.');
    } else {
        // Something else happened while setting up the request
        showError(defaultMessage);
    }
}

/**
 * Function to create a dropdown menu.
 * @param {Array} options - Array of option objects with properties: text, href, className, id.
 * @returns {HTMLElement} - The dropdown menu element.
 */
function createDropdownMenu(options) {
    const dropdown = document.createElement('div');
    dropdown.className = 'dropdown is-hoverable';

    const trigger = document.createElement('div');
    trigger.className = 'dropdown-trigger';
    const button = document.createElement('button');
    button.className = 'button';
    button.setAttribute('aria-haspopup', 'true');
    button.setAttribute('aria-controls', 'dropdown-menu');
    button.innerHTML = '<span>Edit</span><span class="icon is-small"><i class="fas fa-angle-down" aria-hidden="true"></i></span>';
    trigger.appendChild(button);

    const menu = document.createElement('div');
    menu.className = 'dropdown-menu';
    menu.id = 'dropdown-menu';
    menu.setAttribute('role', 'menu');
    const content = document.createElement('div');
    content.className = 'dropdown-content';

    options.forEach(option => {
        const item = document.createElement('a');
        item.href = option.href || '#';
        item.className = `dropdown-item ${option.className || ''}`;
        if (option.id) {
            item.dataset.id = option.id;
        }
        item.textContent = option.text;
        content.appendChild(item);
    });

    menu.appendChild(content);
    dropdown.appendChild(trigger);
    dropdown.appendChild(menu);

    return dropdown;
}

// Event listeners for closing dropdowns when clicking outside
document.addEventListener('click', function(event) {
    const isDropdown = event.target.closest('.dropdown');
    if (!isDropdown) {
        const activeDropdowns = document.querySelectorAll('.dropdown.is-active');
        activeDropdowns.forEach(dropdown => dropdown.classList.remove('is-active'));
    }
});
