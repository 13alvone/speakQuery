// frontend/static/js/common.js

// Common functions for notifications and dropdown handling

// Set Axios to include CSRF token if present
const csrfMetaTag = document.querySelector('meta[name="csrf-token"]');
if (csrfMetaTag) {
    axios.defaults.headers.common['X-CSRFToken'] = csrfMetaTag.getAttribute('content');
}

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

    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'delete';
    notification.appendChild(deleteBtn);

    const messageSpan = document.createElement('span');
    messageSpan.textContent = message;
    notification.appendChild(messageSpan);

    container.appendChild(notification);

    // Automatically remove the notification after 5 seconds
    setTimeout(() => {
        notification.remove();
    }, 5000);

    // Add event listener to the delete button
    deleteBtn.addEventListener('click', () => {
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

/**
 * Create a dropdown menu appended to the body.
 * @param {string|number} id - Identifier used for the menu element.
 * @param {Array} actions - Array of action objects with properties:
 *   text, href, className, id, onClick.
 * @returns {HTMLElement} The created menu element.
 */
function createBodyDropdownMenu(id, actions) {
    const menu = document.createElement('div');
    menu.className = 'dropdown-menu';
    menu.id = `dropdown-menu-${id}`;

    const content = document.createElement('div');
    content.className = 'dropdown-content';

    actions.forEach(action => {
        const item = document.createElement('a');
        item.href = action.href || '#';
        item.className = `dropdown-item ${action.className || ''}`.trim();
        if (action.id !== undefined) {
            item.dataset.id = action.id;
        }
        item.textContent = action.text;
        if (action.onClick) {
            item.addEventListener('click', e => {
                e.preventDefault();
                action.onClick(e);
            });
        }
        content.appendChild(item);
    });

    menu.appendChild(content);
    document.body.appendChild(menu);
    return menu;
}

/**
 * Toggle the visibility of a dropdown menu and position it near the button.
 * @param {string|number} menuId - The identifier passed to createBodyDropdownMenu.
 * @param {DOMRect} buttonRect - Bounding rectangle of the clicked button.
 */
function toggleDropdownMenu(menuId, buttonRect) {
    closeAllDropdowns();

    const menu = document.getElementById(`dropdown-menu-${menuId}`);
    if (menu) {
        menu.style.position = 'fixed';

        const menuTop = buttonRect.bottom;
        const menuLeft = buttonRect.left;

        const menuHeight = menu.offsetHeight;
        const menuWidth = menu.offsetWidth;
        const viewportHeight = window.innerHeight;
        const viewportWidth = window.innerWidth;

        if (menuTop + menuHeight > viewportHeight) {
            menu.style.top = `${buttonRect.top - menuHeight}px`;
        } else {
            menu.style.top = `${menuTop}px`;
        }

        if (menuLeft + menuWidth > viewportWidth) {
            menu.style.left = `${viewportWidth - menuWidth - 10}px`;
        } else {
            menu.style.left = `${menuLeft}px`;
        }

        menu.classList.add('is-active');
    }
}

/** Close all open dropdown menus. */
function closeAllDropdowns() {
    const menus = document.querySelectorAll('.dropdown-menu.is-active');
    menus.forEach(menu => {
        menu.classList.remove('is-active');
    });
}
