export function createToastContainer() {
  const el = document.createElement('div');
  el.className = 'sch-app-toast-wrap';
  return el;
}

export function showToast(container, message, tone = 'success') {
  const item = document.createElement('div');
  item.className = `sch-app-toast sch-app-toast--${tone}`;
  item.textContent = message;
  container.appendChild(item);
  setTimeout(() => item.remove(), 3200);
}
