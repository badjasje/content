const config = window.SCH_APP_CONFIG || {};

async function request(path, options = {}) {
  const response = await fetch(`${config.restBase}${path}`, {
    credentials: 'same-origin',
    headers: {
      'Content-Type': 'application/json',
      'X-WP-Nonce': config.restNonce || '',
      ...(options.headers || {}),
    },
    ...options,
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.message || data.code || 'API request failed');
  }
  return data;
}

export const api = {
  getBootstrap: () => request('/bootstrap'),
  getKeywords: (query = '') => request(`/keywords${query ? `?${query}` : ''}`),
  updateKeyword: (id, payload) => request(`/keywords/${id}`, { method: 'POST', body: JSON.stringify(payload) }),
  getIssues: (query = '') => request(`/issues${query ? `?${query}` : ''}`),
  updateIssueStatus: (type, id, status) => request(`/issues/${type}/${id}`, { method: 'POST', body: JSON.stringify({ status }) }),
  getQueue: () => request('/queue'),
  runWorker: () => request('/queue/run-worker', { method: 'POST', body: '{}' }),
  getSettings: () => request('/settings'),
  saveSettings: (payload) => request('/settings', { method: 'POST', body: JSON.stringify(payload) }),
};
