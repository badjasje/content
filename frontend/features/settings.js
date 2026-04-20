import { api } from '../services/api.js';

export async function renderSettings(container, toast) {
  container.innerHTML = '<div class="sch-app-state">Loading settings…</div>';
  try {
    const data = await api.getSettings();
    container.innerHTML = `
      <form class="sch-app-card" id="sch-settings-form">
        <h3>Settings / integrations</h3>
        <label>OpenAI model <input name="openai_model" value="${data.openai_model || ''}"></label>
        <label>Temperature <input type="number" step="0.1" min="0" max="2" name="openai_temperature" value="${data.openai_temperature || '0.6'}"></label>
        <label><input type="checkbox" name="enable_featured_images" ${data.enable_featured_images ? 'checked' : ''}> Featured images</label>
        <label><input type="checkbox" name="enable_supporting" ${data.enable_supporting ? 'checked' : ''}> Supporting content</label>
        <label><input type="checkbox" name="enable_auto_discovery" ${data.enable_auto_discovery ? 'checked' : ''}> Auto discovery</label>
        <label><input type="checkbox" name="gsc_enabled" ${data.gsc_enabled ? 'checked' : ''}> GSC enabled</label>
        <label><input type="checkbox" name="ga_enabled" ${data.ga_enabled ? 'checked' : ''}> GA enabled</label>
        <label><input type="checkbox" name="random_machine_enabled" ${data.random_machine_enabled ? 'checked' : ''}> Random machine</label>
        <label>Random daily max <input type="number" min="1" max="100" name="random_daily_max" value="${data.random_daily_max || 10}"></label>
        <button type="submit">Save</button>
      </form>
    `;

    container.querySelector('#sch-settings-form')?.addEventListener('submit', async (event) => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      const payload = Object.fromEntries(formData.entries());
      ['enable_featured_images', 'enable_supporting', 'enable_auto_discovery', 'gsc_enabled', 'ga_enabled', 'random_machine_enabled'].forEach((k) => {
        payload[k] = event.currentTarget.querySelector(`[name="${k}"]`).checked;
      });
      await api.saveSettings(payload);
      toast('Settings saved');
    });
  } catch (error) {
    container.innerHTML = `<div class="sch-app-state sch-app-state--error">${error.message}</div>`;
  }
}
