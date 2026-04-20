export function renderAppShell(root, sections) {
  root.innerHTML = `
    <div class="sch-app-shell">
      <aside class="sch-app-sidebar">
        <h2>Content Hub</h2>
        <nav>${sections
          .map((s) => `<button data-nav="${s.id}" class="sch-app-nav-btn">${s.label}</button>`)
          .join('')}</nav>
      </aside>
      <section class="sch-app-main">
        <header class="sch-app-topbar">
          <strong>Orchestrator App Layer</strong>
          <span class="sch-app-topbar-meta">Existing plugin data + actions</span>
        </header>
        <div class="sch-app-view" id="sch-app-view"></div>
      </section>
    </div>
  `;

  const navButtons = Array.from(root.querySelectorAll('[data-nav]'));
  function activate(id) {
    navButtons.forEach((btn) => btn.classList.toggle('is-active', btn.dataset.nav === id));
  }

  return {
    view: root.querySelector('#sch-app-view'),
    activate,
    onNavigate: (cb) => navButtons.forEach((btn) => btn.addEventListener('click', () => cb(btn.dataset.nav))),
  };
}
