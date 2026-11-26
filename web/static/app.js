// Minimal inline SVG chart for dashboard 24h messages. Vanilla JS; data-only.
(() => {
  const el = document.querySelector('.chart[data-chart]');
  if (!el) return;
  let data;
  try { data = JSON.parse(el.dataset.chart); } catch (e) { return; }
  if (!Array.isArray(data) || data.length === 0) {
    el.innerHTML = '<div class="muted">Немає даних за 24 години.</div>';
    return;
  }
  const w = Math.max(320, el.clientWidth - 24);
  const h = 180;
  const pad = {l: 40, r: 10, t: 10, b: 28};
  const innerW = w - pad.l - pad.r;
  const innerH = h - pad.t - pad.b;
  const values = data.map(d => Number(d.n || 0));
  const maxV = Math.max(1, ...values);
  const barW = innerW / data.length - 2;

  const parts = [];
  parts.push(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}" style="width:100%;height:auto">`);
  // y axis
  for (let i = 0; i <= 4; i++) {
    const y = pad.t + innerH * (i / 4);
    const v = Math.round(maxV * (1 - i / 4));
    parts.push(`<line x1="${pad.l}" x2="${w - pad.r}" y1="${y}" y2="${y}" stroke="#223047" stroke-width="1"/>`);
    parts.push(`<text x="4" y="${y + 4}" fill="#8f9eb4" font-size="11">${v}</text>`);
  }
  // bars
  data.forEach((d, i) => {
    const v = Number(d.n || 0);
    const hh = (v / maxV) * innerH;
    const x = pad.l + i * (innerW / data.length) + 1;
    const y = pad.t + innerH - hh;
    parts.push(`<rect x="${x}" y="${y}" width="${Math.max(2, barW)}" height="${hh}" fill="#5ea8ff"><title>${d.bucket}: ${v}</title></rect>`);
  });
  // x labels (first, middle, last)
  const picks = [0, Math.floor(data.length / 2), data.length - 1];
  picks.forEach(i => {
    if (!data[i]) return;
    const x = pad.l + i * (innerW / data.length) + 1;
    const label = String(data[i].bucket).slice(5, 16);  // MM-DD HH
    parts.push(`<text x="${x}" y="${h - 8}" fill="#8f9eb4" font-size="11">${label}</text>`);
  });
  parts.push(`</svg>`);
  el.innerHTML = parts.join('');
})();

// Local collector controls. The token is rendered only on the localhost page.
(() => {
  const panel = document.querySelector('.collector-control');
  if (!panel) return;

  const token = panel.dataset.token;
  const serviceRunning = panel.dataset.serviceRunning === 'true';
  const result        = document.querySelector('#collector-result');
  const sourcesResult = document.querySelector('#sources-result');
  const picker        = document.querySelector('#chat-picker');
  const radios        = [...document.querySelectorAll('[name="collector-mode"]')];
  const searchInput   = document.querySelector('#chat-search');
  const filterButtons = [...document.querySelectorAll('.picker-filters button')];
  const selectVisible = document.querySelector('#picker-select-visible');
  const clearSelection = document.querySelector('#picker-clear');
  const counter       = document.querySelector('#picker-count');
  const syncHint      = document.querySelector('#sync-chats-hint');

  let currentFilter = 'all';

  if (syncHint) {
    syncHint.textContent = serviceRunning
      ? 'Парсер запущений — виконається миттєво у головному процесі'
      : 'Парсер зупинений — одноразова синхронізація запуститься зараз';
  }

  // Update visible options according to search + filter and update the counter.
  function refreshPicker() {
    if (!picker) return;
    const q = (searchInput?.value || '').trim().toLowerCase();
    let visible = 0, checked = 0;
    picker.querySelectorAll('.chat-option').forEach(option => {
      const title    = (option.dataset.title    || '').toLowerCase();
      const username = (option.dataset.username || '').toLowerCase();
      const id       = (option.dataset.id       || '');
      const type     = (option.dataset.type     || '').toLowerCase();
      const isSelected = option.querySelector('input').checked;
      const matchesSearch = !q || title.includes(q) || username.includes(q)
                                || id.includes(q);
      let matchesFilter;
      if (currentFilter === 'channel') matchesFilter = (type === 'channel');
      else if (currentFilter === 'group') matchesFilter = (type === 'supergroup'
          || type === 'megagroup' || type === 'group' || type === 'chat');
      else if (currentFilter === 'selected') matchesFilter = isSelected;
      else matchesFilter = true;
      const show = matchesSearch && matchesFilter;
      option.classList.toggle('is-hidden', !show);
      if (show) visible++;
      if (isSelected) checked++;
    });
    if (counter) counter.textContent = `${checked} обрано (${visible} видимих)`;
  }

  // Confirm before unchecking an already-active (enabled) target.
  picker?.querySelectorAll('.chat-option input').forEach(input => {
    input.addEventListener('change', (evt) => {
      const option = evt.target.closest('.chat-option');
      const enabled = option?.dataset.enabled === 'true';
      if (enabled && !input.checked) {
        const title = option.dataset.title || option.dataset.username
                    || option.dataset.id;
        const ok = window.confirm(
          `Чат "${title}" зараз активний (збір триває). ` +
          `Зняти позначку? Після натискання «Запустити збір» ціль буде зупинено.`);
        if (!ok) {
          input.checked = true;
        }
      }
      refreshPicker();
    });
  });

  searchInput?.addEventListener('input', refreshPicker);
  filterButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      filterButtons.forEach(b => {
        b.classList.toggle('is-active', b === btn);
        b.setAttribute('aria-selected', b === btn ? 'true' : 'false');
      });
      currentFilter = btn.dataset.filter;
      refreshPicker();
    });
  });
  selectVisible?.addEventListener('click', () => {
    picker.querySelectorAll('.chat-option:not(.is-hidden) input').forEach(input => {
      if (!input.checked) input.checked = true;
    });
    refreshPicker();
  });
  clearSelection?.addEventListener('click', () => {
    const activeStays = [];
    picker.querySelectorAll('.chat-option input:checked').forEach(input => {
      const option = input.closest('.chat-option');
      if (option?.dataset.enabled === 'true') {
        activeStays.push(option.dataset.title || option.dataset.id);
      } else {
        input.checked = false;
      }
    });
    if (activeStays.length) {
      setResult('is-ok',
        `Знято невибрані; активні цілі залишено (${activeStays.length}).`);
    }
    refreshPicker();
  });

  const setMode = () => {
    const all = document.querySelector('[name="collector-mode"]:checked')?.value === 'all';
    picker?.classList.toggle('is-disabled', all);
    picker?.querySelectorAll('input').forEach(input => input.disabled = all);
  };
  radios.forEach(radio => radio.addEventListener('change', setMode));
  setMode();
  refreshPicker();

  function setResult(cls, text, target) {
    const node = target || result;
    if (!node) return;
    node.className = cls;
    node.textContent = text;
  }

  async function run(path, body = {}, target) {
    const buttons = panel.querySelectorAll('button');
    buttons.forEach(button => button.disabled = true);
    setResult('is-working', 'Виконується…', target);
    try {
      const response = await fetch(path, {
        method: 'POST',
        headers: {'content-type': 'application/json', 'x-tgparser-control': token},
        body: JSON.stringify(body),
      });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.message || data.detail || 'Операцію не виконано');
      }
      setResult('is-ok', data.pid ? `${data.message}. PID ${data.pid}` : data.message, target);
      return data;
    } catch (error) {
      setResult('is-error', error.message, target);
      return null;
    } finally {
      buttons.forEach(button => button.disabled = false);
      setMode();
    }
  }

  document.querySelector('#collector-start')?.addEventListener('click', () => {
    const mode = document.querySelector('[name="collector-mode"]:checked').value;
    const chats = [...picker.querySelectorAll('input:checked')].map(input => input.value);
    if (mode === 'selected' && !chats.length) {
      setResult('is-error', 'Оберіть щонайменше один чат.');
      return;
    }
    run('/control/start', {mode, chats}).then(data => {
      if (data) window.setTimeout(() => window.location.reload(), 1200);
    });
  });
  document.querySelector('#collector-stop')?.addEventListener('click', () => {
    run('/control/stop').then(data => {
      if (data) window.setTimeout(() => window.location.reload(), 1200);
    });
  });

  const addForm  = document.querySelector('#add-chat-form');
  const addInput = document.querySelector('#add-chat-input');
  addForm?.addEventListener('submit', (evt) => {
    evt.preventDefault();
    const raw = (addInput?.value || '').trim();
    if (!raw) { setResult('is-error', 'Введіть @username, посилання або chat_id.', sourcesResult); return; }
    run('/control/add_chat', {raw_target: raw}, sourcesResult).then(data => {
      if (!data) return;
      addInput.value = '';
      if (data.status === 'selected') return;   // already on the platform
      syncNow();                                 // resolve it right away
    });
  });

  // Delete a chat from the platform. The button lives inside the <label>,
  // so stop the click from toggling the checkbox.
  picker?.querySelectorAll('.chat-delete').forEach(btn => {
    btn.addEventListener('click', (evt) => {
      evt.preventDefault();
      evt.stopPropagation();
      const option = btn.closest('.chat-option');
      const title = option?.dataset.title || option?.dataset.username || btn.dataset.chatId;
      if (option?.dataset.enabled === 'true') {
        setResult('is-error',
          `«${title}» — активна ціль. Спершу зніміть її з вибору та перезапустіть збір.`,
          sourcesResult);
        return;
      }
      if (!window.confirm(`Видалити «${title}» з платформи? ` +
          'Чат зникне зі списку і не повернеться після синхронізації.')) return;
      run('/control/delete_chat', {chat_id: btn.dataset.chatId}, sourcesResult).then(data => {
        if (data) { option.remove(); refreshPicker(); }
      });
    });
  });

  const sleep = ms => new Promise(r => window.setTimeout(r, ms));

  // Wait for the running service's discovery loop to consume the request.
  async function waitQueuedSync() {
    for (let i = 0; i < 60; i++) {
      await sleep(1500);
      try {
        const r = await fetch('/control/sync_status', {
          method: 'POST',
          headers: {'content-type': 'application/json', 'x-tgparser-control': token},
          body: '{}',
        });
        const s = await r.json();
        if (r.ok && s.done) return s.last_status !== 'failed';
      } catch (e) { /* retry */ }
    }
    return false;
  }

  let syncing = false;
  const syncLock = on => {
    syncing = on;
    ['#sync-chats-btn', '#add-chat-btn', '#collector-start', '#collector-stop']
      .forEach(sel => { const b = document.querySelector(sel); if (b) b.disabled = on; });
  };

  async function syncNow() {
    if (syncing) return;
    syncLock(true);
    try { await doSync(); } finally { syncLock(false); }
  }

  async function doSync() {
    setResult('is-working', 'Синхронізація з Telegram…', sourcesResult);
    const data = await run('/control/sync_dialogs', {}, sourcesResult);
    syncLock(true);   // run() re-enables buttons; keep them locked
    if (!data) return;
    if (data.mode === 'queued') {
      setResult('is-working', 'Парсер оновлює список діалогів…', sourcesResult);
      const ok = await waitQueuedSync();
      if (!ok) {
        setResult('is-error', 'Синхронізація не завершилась вчасно; спробуйте ще раз.', sourcesResult);
        return;
      }
      setResult('is-ok', 'Синхронізацію виконано. Оновлюємо список…', sourcesResult);
    }
    window.setTimeout(() => window.location.reload(), 1200);
  }

  document.querySelector('#sync-chats-btn')?.addEventListener('click', () => { syncNow(); });
})();
