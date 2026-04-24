import { api } from '../services/api.js';

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function normalizeLifecycle(value) {
  return value === 'trashed' ? 'trash' : value;
}

function statusBadgeClass(status) {
  if (status === 'queued' || status === 'processing') return 'sch-app-chip sch-app-chip--info';
  if (status === 'done') return 'sch-app-chip sch-app-chip--success';
  if (status === 'failed') return 'sch-app-chip sch-app-chip--danger';
  return 'sch-app-chip';
}

export async function renderKeywords(container, toast) {
  let selectedClientId = '';
  let lifecycle = 'active';
  let search = '';
  let isBusy = false;

  async function loadAndRender() {
    if (isBusy) {
      return;
    }
    isBusy = true;
    container.innerHTML = '<div class="sch-app-state">Loading keywords…</div>';

    try {
      const [bootstrap, data] = await Promise.all([
        api.getBootstrap(),
        api.getKeywords(
          new URLSearchParams({
            per_page: '200',
            ...(lifecycle !== 'all' ? { lifecycle_status: lifecycle } : {}),
            ...(search ? { search } : {}),
          }).toString()
        ),
      ]);

      const clients = (bootstrap.clients || []).slice().sort((a, b) => (a.name || '').localeCompare(b.name || ''));
      const allRows = (data.items || []).filter((row) => normalizeLifecycle(row.lifecycle_status) !== 'trashed');
      const filteredRows = selectedClientId
        ? allRows.filter((row) => String(row.client_id) === String(selectedClientId))
        : allRows;

      const byClient = new Map();
      filteredRows.forEach((row) => {
        const key = row.client_id || 0;
        if (!byClient.has(key)) {
          byClient.set(key, {
            id: row.client_id || 0,
            name: row.client_name || 'Onbekende klant',
            rows: [],
          });
        }
        byClient.get(key).rows.push(row);
      });

      const grouped = Array.from(byClient.values()).sort((a, b) => a.name.localeCompare(b.name));
      const activeCount = allRows.filter((row) => normalizeLifecycle(row.lifecycle_status) === 'active').length;
      const trashCount = allRows.filter((row) => normalizeLifecycle(row.lifecycle_status) === 'trash').length;

      container.innerHTML = `
        <div class="sch-app-card">
          <h3>Keyword workspace per klant</h3>
          <div class="sch-keyword-toolbar">
            <label>
              <span>Klant</span>
              <select data-filter-client>
                <option value="">Alle klanten</option>
                ${clients
                  .map((client) => `<option value="${client.id}" ${String(client.id) === String(selectedClientId) ? 'selected' : ''}>${escapeHtml(client.name)}</option>`)
                  .join('')}
              </select>
            </label>
            <label>
              <span>Zoeken</span>
              <input type="search" value="${escapeHtml(search)}" placeholder="Keyword of klantnaam" data-filter-search>
            </label>
            <div class="sch-keyword-tabs">
              <button type="button" class="${lifecycle === 'active' ? 'is-active' : ''}" data-filter-lifecycle="active">Actief (${activeCount})</button>
              <button type="button" class="${lifecycle === 'trash' ? 'is-active' : ''}" data-filter-lifecycle="trash">Prullenbak (${trashCount})</button>
              <button type="button" class="${lifecycle === 'all' ? 'is-active' : ''}" data-filter-lifecycle="all">Alles (${allRows.length})</button>
            </div>
          </div>

          <div class="sch-keyword-bulk">
            <button type="button" data-bulk-action="queue">Bulk: queue</button>
            <button type="button" data-bulk-action="trash">Bulk: naar prullenbak</button>
            <button type="button" data-bulk-action="restore">Bulk: herstellen</button>
          </div>

          ${grouped.length
            ? grouped
                .map(
                  (group) => `
                <section class="sch-keyword-client-group">
                  <h4>${escapeHtml(group.name)} <span class="sch-app-chip">${group.rows.length} keywords</span></h4>
                  <div class="sch-app-table-wrap">
                    <table class="sch-app-table">
                      <thead>
                        <tr>
                          <th><input type="checkbox" data-check-group="${group.id}"></th>
                          <th>Keyword</th>
                          <th>Type</th>
                          <th>Status</th>
                          <th>Lifecycle</th>
                          <th>Priority</th>
                          <th>Acties</th>
                        </tr>
                      </thead>
                      <tbody>
                        ${group.rows
                          .map(
                            (row) => `
                            <tr>
                              <td><input type="checkbox" data-check-keyword value="${row.id}" data-group-id="${group.id}"></td>
                              <td>${escapeHtml(row.main_keyword)}</td>
                              <td>${escapeHtml(row.content_type || '-')}</td>
                              <td><span class="${statusBadgeClass(row.status)}">${escapeHtml(row.status || '-')}</span></td>
                              <td><span class="sch-app-chip">${escapeHtml(normalizeLifecycle(row.lifecycle_status || '-'))}</span></td>
                              <td>${Number.isFinite(Number(row.priority)) ? Number(row.priority) : '-'}</td>
                              <td class="sch-actions-inline">
                                <button type="button" data-kw-queue="${row.id}">Queue</button>
                                <button type="button" data-kw-trash="${row.id}">Trash</button>
                                <button type="button" data-kw-restore="${row.id}">Herstel</button>
                              </td>
                            </tr>
                          `
                          )
                          .join('')}
                      </tbody>
                    </table>
                  </div>
                </section>
              `
                )
                .join('')
            : '<div class="sch-app-state">Geen keywords gevonden voor deze filters.</div>'}
        </div>
      `;

      container.querySelector('[data-filter-client]')?.addEventListener('change', (event) => {
        selectedClientId = event.target.value;
        loadAndRender();
      });
      container.querySelector('[data-filter-search]')?.addEventListener('change', (event) => {
        search = event.target.value.trim();
        loadAndRender();
      });
      container.querySelectorAll('[data-filter-lifecycle]').forEach((btn) => {
        btn.addEventListener('click', () => {
          lifecycle = btn.dataset.filterLifecycle || 'active';
          loadAndRender();
        });
      });

      container.querySelectorAll('[data-check-group]').forEach((checkAll) => {
        checkAll.addEventListener('change', () => {
          const groupId = checkAll.dataset.checkGroup;
          container.querySelectorAll(`[data-check-keyword][data-group-id="${groupId}"]`).forEach((item) => {
            item.checked = checkAll.checked;
          });
        });
      });

      async function updateKeyword(id, payload, successMessage) {
        await api.updateKeyword(id, payload);
        toast(successMessage, 'success');
        loadAndRender();
      }

      container.querySelectorAll('[data-kw-queue]').forEach((btn) => {
        btn.addEventListener('click', () => updateKeyword(btn.dataset.kwQueue, { status: 'queued', lifecycle_status: 'active' }, 'Keyword queued'));
      });
      container.querySelectorAll('[data-kw-trash]').forEach((btn) => {
        btn.addEventListener('click', () => updateKeyword(btn.dataset.kwTrash, { lifecycle_status: 'trash' }, 'Keyword naar prullenbak verplaatst'));
      });
      container.querySelectorAll('[data-kw-restore]').forEach((btn) => {
        btn.addEventListener('click', () => updateKeyword(btn.dataset.kwRestore, { lifecycle_status: 'active' }, 'Keyword hersteld'));
      });

      container.querySelectorAll('[data-bulk-action]').forEach((btn) => {
        btn.addEventListener('click', async () => {
          const selectedIds = Array.from(container.querySelectorAll('[data-check-keyword]:checked')).map((el) => el.value);
          if (!selectedIds.length) {
            toast('Selecteer eerst minimaal één keyword', 'error');
            return;
          }

          let payload = { lifecycle_status: 'active' };
          let message = 'Keywords bijgewerkt';
          if (btn.dataset.bulkAction === 'queue') {
            payload = { status: 'queued', lifecycle_status: 'active' };
            message = 'Keywords queued';
          } else if (btn.dataset.bulkAction === 'trash') {
            payload = { lifecycle_status: 'trash' };
            message = 'Keywords naar prullenbak verplaatst';
          }

          await Promise.all(selectedIds.map((id) => api.updateKeyword(id, payload)));
          toast(message, 'success');
          loadAndRender();
        });
      });
    } catch (error) {
      container.innerHTML = `<div class="sch-app-state sch-app-state--error">${escapeHtml(error.message)}</div>`;
    } finally {
      isBusy = false;
    }
  }

  loadAndRender();
}
