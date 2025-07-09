// frontend/static/js/users.js

document.addEventListener('DOMContentLoaded', function() {
    loadUsers();
    const form = document.getElementById('create-user-form');
    form.addEventListener('submit', function(e) {
        e.preventDefault();
        const username = form.username.value;
        const password = form.password.value;
        const role = form.role.value;
        axios.post('/users', {username, password, role})
            .then(resp => {
                if (resp.data.status === 'success') {
                    showNotification('User created', false);
                    form.reset();
                    loadUsers();
                } else {
                    showNotification(resp.data.message || 'Error creating user', true);
                }
            })
            .catch(() => {
                showNotification('Error creating user', true);
            });
    });
});

function loadUsers() {
    axios.get('/users')
        .then(resp => {
            if (resp.data.status === 'success') {
                const tbody = document.querySelector('#users-table tbody');
                tbody.innerHTML = '';
                resp.data.users.forEach(u => {
                    const tr = document.createElement('tr');
                    const last = u.last_login ? new Date(u.last_login * 1000).toLocaleString() : '';
                    tr.innerHTML = `<td>${escapeHtml(u.username)}</td><td>${escapeHtml(u.role)}</td><td>${escapeHtml(last)}</td>`;
                    tbody.appendChild(tr);
                });
            }
        })
        .catch(() => {
            showNotification('Failed to load users', true);
        });
}
