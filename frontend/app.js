import { renderAppShell } from './components/appShell.js';
import { createToastContainer, showToast } from './components/toast.js';
import { renderOverview } from './features/overview.js';
import { renderKeywords } from './features/keywords.js';
import { renderIssues } from './features/issues.js';
import { renderQueue } from './features/queue.js';
import { renderSettings } from './features/settings.js';

const root = document.getElementById('sch-frontend-app-root');

if (root) {
  const sections = [
    { id: 'overview', label: 'Overview' },
    { id: 'keywords', label: 'Keywords' },
    { id: 'issues', label: 'Technical Issues' },
    { id: 'queue', label: 'Automation Queue' },
    { id: 'settings', label: 'Settings' },
  ];

  const shell = renderAppShell(root, sections);
  const toastContainer = createToastContainer();
  root.appendChild(toastContainer);
  const toast = (message, tone) => showToast(toastContainer, message, tone);

  const renderers = {
    overview: () => renderOverview(shell.view),
    keywords: () => renderKeywords(shell.view, toast),
    issues: () => renderIssues(shell.view, toast),
    queue: () => renderQueue(shell.view, toast),
    settings: () => renderSettings(shell.view, toast),
  };

  function navigate(id) {
    shell.activate(id);
    shell.view.innerHTML = '<div class="sch-app-state">Loading…</div>';
    const fn = renderers[id] || renderers.overview;
    fn();
  }

  shell.onNavigate(navigate);
  navigate('overview');
}
