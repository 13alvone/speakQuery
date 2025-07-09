/** Simple theme persistence module reused from common.js */

function initTheme() {
    let stored = localStorage.getItem('theme');
    if (!stored) {
        stored = 'light';
        localStorage.setItem('theme', 'light');
    }
    if (stored === 'light') {
        document.body.classList.add('light-theme');
    } else {
        document.body.classList.remove('light-theme');
    }
}

function attachThemeToggle() {
    const btn = document.getElementById('theme-toggle');
    if (!btn) {
        return;
    }
    btn.addEventListener('click', () => {
        const isLight = document.body.classList.toggle('light-theme');
        localStorage.setItem('theme', isLight ? 'light' : 'dark');
    });
}

function initializeThemeToggle() {
    initTheme();
    attachThemeToggle();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeThemeToggle);
} else {
    initializeThemeToggle();
}
