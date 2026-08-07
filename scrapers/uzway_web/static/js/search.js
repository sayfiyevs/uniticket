(function () {
  const input = document.getElementById('question');
  const btn = document.getElementById('search-btn');
  const legsSel = document.getElementById('max_legs');
  const results = document.getElementById('results');
  const consolePanel = document.getElementById('console-panel');
  const consoleOut = document.getElementById('console-out');

  document.querySelectorAll('.example-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      input.value = chip.dataset.q;
      runSearch();
    });
  });

  btn.addEventListener('click', runSearch);
  input.addEventListener('keydown', e => { if (e.key === 'Enter') runSearch(); });

  function esc(s) {
    const d = document.createElement('div');
    d.textContent = s == null ? '' : String(s);
    return d.innerHTML;
  }

  async function runSearch() {
    const question = input.value.trim();
    if (!question) { input.focus(); return; }

    btn.disabled = true;
    btn.textContent = 'Searching…';
    results.innerHTML = '<div class="empty-state">Loading schedules and searching itineraries…</div>';
    consolePanel.style.display = 'none';

    try {
      const resp = await fetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, max_legs: parseInt(legsSel.value, 10) })
      });
      const data = await resp.json();

      if (!resp.ok) {
        results.innerHTML = renderError(data.error || 'Search failed.', data.detail);
        return;
      }
      if (data.error) {
        results.innerHTML = `<div class="empty-state">${esc(data.error)}</div>`;
        return;
      }
      render(data);
    } catch (e) {
      results.innerHTML = renderError('Request failed.', String(e));
    } finally {
      btn.disabled = false;
      btn.textContent = 'Search';
    }
  }

  function renderError(msg, detail) {
    return `<div class="banner-error"><strong>${esc(msg)}</strong>` +
           (detail ? `<div style="margin-top:6px;"><code>${esc(detail)}</code></div>` : '') +
           `</div>`;
  }

  function render(data) {
    let html = '';

    if (data.notes && data.notes.length) {
      html += '<div class="notes-box">' + data.notes.map(n => `<div>! ${esc(n)}</div>`).join('') + '</div>';
    }

    if (!data.itineraries.length) {
      html += '<div class="empty-state">No itineraries found for that search.</div>';
    } else {
      data.itineraries.forEach(it => { html += renderItinerary(it); });
    }

    html += `<p class="help-text">${data.legs_loaded} usable legs loaded` +
            (data.legs_dropped ? ` (${data.legs_dropped} skipped: missing price or time)` : '') +
            `</p>`;

    results.innerHTML = html;

    consoleOut.textContent = data.console_text || '';
    consolePanel.style.display = 'block';
  }

  function renderItinerary(it) {
    let chain = '';
    it.legs.forEach((leg, i) => {
      if (i === 0) {
        chain += stopHtml(leg.origin_city, leg.departure_time);
      }
      chain += legHtml(leg);
      chain += stopHtml(leg.destination_city, leg.arrival_time);
    });

    const estBadge = it.has_estimated ? '<span class="itin-est-badge">ESTIMATED</span>' : '';
    const changeLabel = it.changes === 0 ? 'Direct' : `${it.changes} change${it.changes > 1 ? 's' : ''}`;

    return `
      <div class="itin-card">
        <div class="itin-head">
          <span class="itin-rank">#${it.rank}</span>
          <span class="itin-price">${esc(it.price_total_label)}</span>
          <span class="itin-price-sub">${esc(it.price_per_person_label)}/person</span>
          <span class="itin-duration">${esc(it.duration_label)} total</span>
          <span class="itin-changes">${changeLabel}</span>
          ${estBadge}
        </div>
        <div class="chain">${chain}</div>
      </div>`;
  }

  function stopHtml(city, time) {
    return `<div class="chain-stop"><div class="city">${esc(city)}</div><div class="time">${esc(time || '?')}</div></div>`;
  }

  function legHtml(leg) {
    const waitHtml = leg.wait_after_label
      ? `<div class="chain-wait">wait ${esc(leg.wait_after_label)}</div>` : '';
    return `
      <div class="chain-leg">
        <div class="mode-pill ${leg.mode}">${esc(leg.mode_label)}</div>
        <div class="chain-leg-line ${leg.mode}"></div>
        <div class="chain-leg-meta">${esc(leg.transport_name || '')} · ${esc(leg.duration_label)}</div>
        <div class="chain-leg-price">${esc(leg.price_label)}${leg.estimated ? ' (est.)' : ''}</div>
        ${waitHtml}
      </div>`;
  }
})();
