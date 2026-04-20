import { api } from '../services/api.js';

export async function renderOverview(container) {
  container.innerHTML = '<div class="sch-app-state">Loading dashboard…</div>';
  try {
    const data = await api.getBootstrap();
    const cards = [
      ['Queued jobs', data.metrics.jobs.queued || 0],
      ['Running jobs', data.metrics.jobs.running || 0],
      ['Awaiting approval', data.metrics.jobs.awaiting_approval || 0],
      ['Published jobs', data.metrics.jobs.published || 0],
      ['Open issues', data.metrics.open_issues || 0],
      ['Active clients', data.metrics.clients_active || 0],
    ];

    container.innerHTML = `
      <div class="sch-app-grid">${cards
        .map(([label, value]) => `<article class="sch-app-card"><h3>${label}</h3><p>${value}</p></article>`)
        .join('')}</div>
      <section class="sch-app-card">
        <h3>Integrations</h3>
        <ul>
          <li>GSC: ${data.integrations.gsc_enabled ? 'Enabled' : 'Disabled'}</li>
          <li>GA4: ${data.integrations.ga_enabled ? 'Enabled' : 'Disabled'}</li>
          <li>SERP Provider: ${data.integrations.serp_provider}</li>
        </ul>
      </section>
    `;
  } catch (error) {
    container.innerHTML = `<div class="sch-app-state sch-app-state--error">${error.message}</div>`;
  }
}
