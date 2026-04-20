import { api } from '../services/api.js';

export async function renderIssues(container, toast) {
  container.innerHTML = '<div class="sch-app-state">Loading technical issues…</div>';
  try {
    const data = await api.getIssues('status=open');
    container.innerHTML = `
      <div class="sch-app-card">
        <h3>Technical issues panel</h3>
        ${
          data.items.length
            ? `<ul class="sch-app-list">
                ${data.items
                  .map(
                    (item) => `<li>
                      <strong>[${item.type}] ${item.title || item.signal_type}</strong>
                      <div>${item.recommended_action || 'No recommendation'}</div>
                      <button data-issue-resolve="${item.type}:${item.id}">Resolve</button>
                      <button data-issue-ignore="${item.type}:${item.id}">Ignore</button>
                    </li>`
                  )
                  .join('')}
              </ul>`
            : '<div class="sch-app-state">No open issues 🎉</div>'
        }
      </div>`;

    container.querySelectorAll('[data-issue-resolve], [data-issue-ignore]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const [type, id] = (btn.dataset.issueResolve || btn.dataset.issueIgnore).split(':');
        const status = btn.dataset.issueResolve ? 'resolved' : 'ignored';
        await api.updateIssueStatus(type, id, status);
        toast(`Issue ${status}`);
        renderIssues(container, toast);
      });
    });
  } catch (error) {
    container.innerHTML = `<div class="sch-app-state sch-app-state--error">${error.message}</div>`;
  }
}
