const loginSection = document.getElementById('login-section');
const dashboardSection = document.getElementById('dashboard-section');
const loginForm = document.getElementById('login-form');
const loginError = document.getElementById('login-error');
const logoutBtn = document.getElementById('logout-btn');
const usersTableBody = document.getElementById('users-table-body');
const statActive = document.getElementById('stat-active');
const statCapacity = document.getElementById('stat-capacity');
const actionError = document.getElementById('action-error');

let authToken = localStorage.getItem('kafka_admin_token') || null;

// Initial check
if (authToken) {
    showDashboard();
}

loginForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const user = document.getElementById('admin-user').value;
    const pass = document.getElementById('admin-pass').value;
    const token = btoa(`${user}:${pass}`);

    try {
        const res = await fetch('/api/admin/login', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (res.ok) {
            authToken = token;
            localStorage.setItem('kafka_admin_token', token);
            loginError.classList.remove('show');
            showDashboard();
        } else {
            loginError.innerText = 'Invalid credentials';
            loginError.classList.add('show');
        }
    } catch (err) {
        loginError.innerText = 'Failed to connect';
        loginError.classList.add('show');
    }
});

logoutBtn.addEventListener('click', () => {
    authToken = null;
    localStorage.removeItem('kafka_admin_token');
    dashboardSection.classList.add('hidden');
    loginSection.classList.remove('hidden');
});

async function showDashboard() {
    loginSection.classList.add('hidden');
    dashboardSection.classList.remove('hidden');
    await loadUsers();
}

async function loadUsers() {
    try {
        const res = await fetch('/api/admin/users', {
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        if (res.status === 401) {
            logoutBtn.click();
            return;
        }

        const data = await res.json();
        
        statActive.innerText = data.total_users;
        statCapacity.innerText = data.max_users - data.total_users;

        usersTableBody.innerHTML = '';
        
        if (Object.keys(data.users).length === 0) {
            usersTableBody.innerHTML = '<tr><td colspan="4" style="text-align:center;">No users registered yet</td></tr>';
            return;
        }

        for (const [username, info] of Object.entries(data.users)) {
            const tr = document.createElement('tr');
            
            const dateStr = info.created_at ? new Date(info.created_at).toLocaleDateString() : 'Unknown';
            const handleStr = info.githubHandle ? `@${info.githubHandle}` : info.githubId;
            
            tr.innerHTML = `
                <td><strong>${username}</strong></td>
                <td><span class="badge">${handleStr}</span></td>
                <td>${dateStr}</td>
                <td><button class="delete-btn" onclick="deleteUser('${username}')">Delete User</button></td>
            `;
            usersTableBody.appendChild(tr);
        }

    } catch (err) {
        actionError.innerText = 'Failed to load users';
        actionError.classList.add('show');
    }
}

async function deleteUser(username) {
    if (!confirm(`Are you absolutely sure you want to delete ${username}? This removes their Kafka topic and ACLs permanently.`)) {
        return;
    }

    try {
        actionError.classList.remove('show');
        const res = await fetch(`/api/admin/users/${username}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${authToken}` }
        });

        const data = await res.json();
        if (res.ok) {
            alert(`User ${username} successfully deleted.`);
            loadUsers();
        } else {
            actionError.innerText = data.error || 'Failed to delete user';
            actionError.classList.add('show');
        }
    } catch (err) {
        actionError.innerText = 'Network error while deleting';
        actionError.classList.add('show');
    }
}
