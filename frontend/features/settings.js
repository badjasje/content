import { api } from '../services/api.js';

const SETTINGS_TABS = [
  { id: 'ai', label: 'AI & content' },
  { id: 'integrations', label: 'Integraties' },
  { id: 'random-machine', label: 'Random machine' },
];

export async function renderSettings(container, toast) {
  container.innerHTML = '<div class="sch-app-state">Loading settings…</div>';
  try {
    const data = await api.getSettings();
    container.innerHTML = `
      <form class="sch-app-card sch-settings" id="sch-settings-form">
        <div class="sch-settings__header">
          <h3>Instellingen</h3>
          <p>Gebruik tabs om instellingen per onderwerp te beheren.</p>
        </div>

        <div class="sch-settings-tabs" role="tablist" aria-label="Instellingen categorieën">
          ${SETTINGS_TABS.map((tab, index) => `
            <button
              type="button"
              class="sch-settings-tabs__trigger ${index === 0 ? 'is-active' : ''}"
              role="tab"
              id="sch-settings-tab-${tab.id}"
              aria-controls="sch-settings-panel-${tab.id}"
              aria-selected="${index === 0 ? 'true' : 'false'}"
              data-tab-target="${tab.id}"
            >
              ${tab.label}
            </button>
          `).join('')}
        </div>

        <section
          class="sch-settings-panel is-active"
          role="tabpanel"
          id="sch-settings-panel-ai"
          aria-labelledby="sch-settings-tab-ai"
          data-tab-panel="ai"
        >
          <label>OpenAI model <input name="openai_model" value="${data.openai_model || ''}"></label>
          <label>Temperature <input type="number" step="0.1" min="0" max="2" name="openai_temperature" value="${data.openai_temperature || '0.6'}"></label>
          <label><input type="checkbox" name="enable_featured_images" ${data.enable_featured_images ? 'checked' : ''}> Featured images</label>
          <label><input type="checkbox" name="enable_supporting" ${data.enable_supporting ? 'checked' : ''}> Supporting content</label>
          <label><input type="checkbox" name="enable_auto_discovery" ${data.enable_auto_discovery ? 'checked' : ''}> Auto discovery</label>
        </section>

        <section
          class="sch-settings-panel"
          role="tabpanel"
          id="sch-settings-panel-integrations"
          aria-labelledby="sch-settings-tab-integrations"
          data-tab-panel="integrations"
          hidden
        >
          <label><input type="checkbox" name="gsc_enabled" ${data.gsc_enabled ? 'checked' : ''}> GSC enabled</label>
          <label><input type="checkbox" name="ga_enabled" ${data.ga_enabled ? 'checked' : ''}> GA enabled</label>
        </section>

        <section
          class="sch-settings-panel"
          role="tabpanel"
          id="sch-settings-panel-random-machine"
          aria-labelledby="sch-settings-tab-random-machine"
          data-tab-panel="random-machine"
          hidden
        >
          <label><input type="checkbox" name="random_machine_enabled" ${data.random_machine_enabled ? 'checked' : ''}> Random machine</label>
          <label>Random daily max <input type="number" min="1" max="100" name="random_daily_max" value="${data.random_daily_max || 10}"></label>
          <label><input type="checkbox" name="random_trends_enabled" ${data.random_trends_enabled ? 'checked' : ''}> Random machine + Google Trends</label>
          <label>Trends geo <input name="random_trends_geo" maxlength="5" value="${data.random_trends_geo || 'NL'}"></label>
          <label>Max trends topics <input type="number" min="1" max="20" name="random_trends_max_topics" value="${data.random_trends_max_topics || 8}"></label>
        </section>

        <div class="sch-settings__actions">
          <button type="submit">Save</button>
        </div>
      </form>
    `;

    const form = container.querySelector('#sch-settings-form');
    const tabButtons = [...container.querySelectorAll('[data-tab-target]')];
    const tabPanels = [...container.querySelectorAll('[data-tab-panel]')];

    const setActiveTab = (tabId) => {
      tabButtons.forEach((button) => {
        const isActive = button.dataset.tabTarget === tabId;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-selected', isActive ? 'true' : 'false');
      });

      tabPanels.forEach((panel) => {
        const isActive = panel.dataset.tabPanel === tabId;
        panel.classList.toggle('is-active', isActive);
        panel.hidden = !isActive;
      });
    };

    tabButtons.forEach((button) => {
      button.addEventListener('click', () => setActiveTab(button.dataset.tabTarget));
    });

    form?.addEventListener('submit', async (event) => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      const payload = Object.fromEntries(formData.entries());
      ['enable_featured_images', 'enable_supporting', 'enable_auto_discovery', 'gsc_enabled', 'ga_enabled', 'random_machine_enabled', 'random_trends_enabled'].forEach((k) => {
        payload[k] = event.currentTarget.querySelector(`[name="${k}"]`).checked;
      });
      await api.saveSettings(payload);
      toast('Settings saved');
    });
  } catch (error) {
    container.innerHTML = `<div class="sch-app-state sch-app-state--error">${error.message}</div>`;
  }
}
