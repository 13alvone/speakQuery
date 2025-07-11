// schedule_input.js - repo management interface

document.addEventListener('DOMContentLoaded', function () {
    loadRepos();

    const cloneForm = document.getElementById('clone-repo-form');
    cloneForm.addEventListener('submit', function (e) {
        e.preventDefault();
        const name = document.getElementById('clone-name').value.trim();
        const url = document.getElementById('clone-url').value.trim();
        if (!name || !url) {
            showError('Name and URL required');
            return;
        }
        axios.post('/clone_repo', { name: name, git_url: url })
            .then(() => {
                showSuccess('Repository cloned');
                cloneForm.reset();
                loadRepos();
            })
            .catch(err => handleError(err, 'Failed to clone repository'));
    });

    function loadRepos() {
        axios.get('/list_repos')
            .then(resp => {
                if (resp.data.status === 'success') {
                    renderRepos(resp.data.repos);
                } else {
                    showError('Failed to load repositories');
                }
            })
            .catch(err => handleError(err, 'Failed to load repositories'));
    }

    function renderRepos(repos) {
        const container = document.getElementById('repos-container');
        container.innerHTML = '';
        repos.forEach(repo => {
            const box = document.createElement('div');
            box.className = 'box';
            const title = document.createElement('h2');
            title.className = 'subtitle';
            title.textContent = repo.name;
            box.appendChild(title);

            const pullBtn = document.createElement('button');
            pullBtn.className = 'button is-info mr-2';
            pullBtn.textContent = 'Pull';
            pullBtn.addEventListener('click', () => {
                axios.post(`/pull_repo/${repo.id}`, {})
                    .then(() => showSuccess('Repository updated'))
                    .catch(err => handleError(err, 'Failed to pull repository'));
            });
            box.appendChild(pullBtn);

            const delBtn = document.createElement('button');
            delBtn.className = 'button is-danger mr-2';
            delBtn.textContent = 'Delete Repo';
            delBtn.addEventListener('click', () => {
                if (confirm('Delete this repository?')) {
                    axios.post(`/delete_repo/${repo.id}`, {})
                        .then(() => {
                            showSuccess('Repository deleted');
                            loadRepos();
                        })
                        .catch(err => handleError(err, 'Failed to delete repo'));
                }
            });
            box.appendChild(delBtn);

            const scriptsDiv = document.createElement('div');
            scriptsDiv.id = `repo-${repo.id}-scripts`;
            scriptsDiv.className = 'mt-3';
            box.appendChild(scriptsDiv);

            container.appendChild(box);
            loadScripts(repo.id);
        });
    }

    function loadScripts(repoId) {
        axios.get(`/list_repo_scripts/${repoId}`)
            .then(resp => {
                if (resp.data.status === 'success') {
                    renderScripts(repoId, resp.data.scripts);
                } else {
                    showError('Failed to list scripts');
                }
            })
            .catch(err => handleError(err, 'Failed to list scripts'));
    }

    function renderScripts(repoId, scripts) {
        const container = document.getElementById(`repo-${repoId}-scripts`);
        container.innerHTML = '';
        if (scripts.length === 0) {
            container.textContent = 'No scripts found.';
            return;
        }
        scripts.forEach(name => {
            const row = document.createElement('div');
            row.className = 'field is-grouped mb-1';

            const nameEl = document.createElement('p');
            nameEl.className = 'control';
            nameEl.textContent = name;
            row.appendChild(nameEl);

            const inputEl = document.createElement('input');
            inputEl.className = 'input control';
            inputEl.type = 'text';
            inputEl.placeholder = 'Cron schedule';
            const inputWrap = document.createElement('p');
            inputWrap.className = 'control';
            inputWrap.appendChild(inputEl);
            row.appendChild(inputWrap);

            const setBtn = document.createElement('button');
            setBtn.className = 'button is-primary control';
            setBtn.textContent = 'Set';
            setBtn.addEventListener('click', () => {
                const cron = inputEl.value.trim();
                if (!cron) { showError('Cron schedule required'); return; }
                axios.post('/set_script_schedule', { repo_id: repoId, script_name: name, cron_schedule: cron })
                    .then(() => showSuccess('Schedule saved'))
                    .catch(err => handleError(err, 'Failed to set schedule'));
            });
            row.appendChild(setBtn);

            const delSchedBtn = document.createElement('button');
            delSchedBtn.className = 'button is-warning control';
            delSchedBtn.textContent = 'Delete Schedule';
            delSchedBtn.addEventListener('click', () => {
                if (confirm('Delete this schedule?')) {
                    axios.post('/delete_script_schedule', { repo_id: repoId, script_name: name })
                        .then(() => showSuccess('Schedule removed'))
                        .catch(err => handleError(err, 'Failed to delete schedule'));
                }
            });
            row.appendChild(delSchedBtn);

            const editBtn = document.createElement('button');
            editBtn.className = 'button is-link control';
            editBtn.textContent = 'Edit';
            editBtn.addEventListener('click', () => {
                const content = prompt('New file contents?');
                if (content !== null) {
                    axios.post('/edit_repo_file', { repo_id: repoId, file_path: name, content: content })
                        .then(() => showSuccess('File updated'))
                        .catch(err => handleError(err, 'Failed to edit file'));
                }
            });
            row.appendChild(editBtn);

            const delBtn = document.createElement('button');
            delBtn.className = 'button is-danger control';
            delBtn.textContent = 'Delete';
            delBtn.addEventListener('click', () => {
                if (confirm('Delete this file?')) {
                    axios.post('/delete_repo_file', { repo_id: repoId, file_path: name })
                        .then(() => { showSuccess('File deleted'); loadScripts(repoId); })
                        .catch(err => handleError(err, 'Failed to delete file'));
                }
            });
            row.appendChild(delBtn);

            container.appendChild(row);
        });
    }
});
