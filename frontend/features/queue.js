import { api } from '../services/api.js';

export async function renderQueue(container, toast) {
  container.innerHTML = '<div class="sch-app-state">Loading automation queue…</div>';
  try {
    const data = await api.getQueue();
    container.innerHTML = `
      <div class="sch-app-card">
        <h3>Automation / orchestration queue</h3>
        <p><button id="sch-run-worker">Run worker now</button></p>
        <h4>Jobs</h4>
        <div class="sch-app-table-wrap">
          <table class="sch-app-table">
            <thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Attempts</th><th>Updated</th></tr></thead>
            <tbody>${data.jobs.map((j) => `<tr><td>${j.id}</td><td>${j.job_type}</td><td>${j.status}</td><td>${j.attempts}</td><td>${j.updated_at}</td></tr>`).join('')}</tbody>
          </table>
        </div>
      </div>`;

    container.querySelector('#sch-run-worker')?.addEventListener('click', async () => {
      await api.runWorker();
      toast('Worker executed');
      renderQueue(container, toast);
    });
  } catch (error) {
    container.innerHTML = `<div class="sch-app-state sch-app-state--error">${error.message}</div>`;
  }
}
