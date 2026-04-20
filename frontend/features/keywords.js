import { api } from '../services/api.js';

export async function renderKeywords(container, toast) {
  container.innerHTML = '<div class="sch-app-state">Loading keywords…</div>';
  try {
    const data = await api.getKeywords('per_page=100');
    if (!data.items.length) {
      container.innerHTML = '<div class="sch-app-state">No keywords found.</div>';
      return;
    }

    container.innerHTML = `
      <div class="sch-app-card">
        <h3>Keyword / content optimization workspace</h3>
        <div class="sch-app-table-wrap">
          <table class="sch-app-table">
            <thead><tr><th>Keyword</th><th>Client</th><th>Type</th><th>Status</th><th>Priority</th><th>Actions</th></tr></thead>
            <tbody>
              ${data.items
                .map(
                  (row) => `<tr>
                    <td>${row.main_keyword}</td>
                    <td>${row.client_name || '-'}</td>
                    <td>${row.content_type}</td>
                    <td>${row.status}</td>
                    <td>${row.priority}</td>
                    <td>
                      <button data-kw-trash="${row.id}">Trash</button>
                      <button data-kw-queue="${row.id}">Queue</button>
                    </td>
                  </tr>`
                )
                .join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;

    container.querySelectorAll('[data-kw-trash]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        await api.updateKeyword(btn.dataset.kwTrash, { lifecycle_status: 'trashed' });
        toast('Keyword moved to trash');
        renderKeywords(container, toast);
      });
    });

    container.querySelectorAll('[data-kw-queue]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        await api.updateKeyword(btn.dataset.kwQueue, { status: 'queued', lifecycle_status: 'active' });
        toast('Keyword queued', 'success');
        renderKeywords(container, toast);
      });
    });
  } catch (error) {
    container.innerHTML = `<div class="sch-app-state sch-app-state--error">${error.message}</div>`;
  }
}
