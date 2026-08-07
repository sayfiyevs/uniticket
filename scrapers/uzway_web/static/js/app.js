(function () {
  function setChip(el, status) {
    el.className = 'chip ' + status;
    el.innerHTML = '<span class="dot"></span>' + status;
  }

  function initRunner(box) {
    const kind = box.dataset.kind;
    const runBtn = box.querySelector('.run-btn');
    const stopBtn = box.querySelector('.stop-btn');
    const chip = box.querySelector('.job-chip');
    const out = box.querySelector('.job-console');
    let jobId = null;
    let seen = 0;
    let poller = null;

    function appendLines(lines) {
      const atBottom = out.scrollTop + out.clientHeight >= out.scrollHeight - 8;
      lines.forEach(line => {
        const div = document.createElement('div');
        div.className = 'line';
        div.textContent = line;
        out.appendChild(div);
      });
      if (atBottom) out.scrollTop = out.scrollHeight;
    }

    function buildPayload() {
      const payload = { kind };
      if (kind === 'network') {
        const days = box.querySelector('.field-days');
        const cities = box.querySelector('.field-cities');
        const hubs = box.querySelector('.field-hubs');
        if (days && days.value) payload.days = days.value;
        if (cities && cities.value) payload.cities = cities.value;
        if (hubs && hubs.value) payload.hubs = hubs.value;
      }
      if (kind === 'pipeline') {
        payload.pipeline = box.dataset.pipeline;
      }
      return payload;
    }

    async function start() {
      out.innerHTML = '';
      seen = 0;
      runBtn.disabled = true;
      setChip(chip, 'running');

      const resp = await fetch('/api/jobs/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildPayload())
      });
      const data = await resp.json();
      if (!resp.ok || data.error) {
        appendLines(['[web] ' + (data.error || 'Failed to start job.')]);
        setChip(chip, 'failed');
        runBtn.disabled = false;
        return;
      }
      jobId = data.job_id;
      stopBtn.style.display = 'inline-flex';
      poller = setInterval(poll, 900);
      poll();
    }

    async function poll() {
      if (!jobId) return;
      const resp = await fetch(`/api/jobs/${jobId}?since=${seen}`);
      if (!resp.ok) return;
      const data = await resp.json();
      if (data.lines && data.lines.length) {
        appendLines(data.lines);
        seen = data.total_lines;
      }
      if (data.status !== 'running') {
        clearInterval(poller);
        setChip(chip, data.status);
        runBtn.disabled = false;
        stopBtn.style.display = 'none';
      }
    }

    async function stop() {
      if (!jobId) return;
      await fetch(`/api/jobs/${jobId}/stop`, { method: 'POST' });
    }

    runBtn.addEventListener('click', start);
    if (stopBtn) stopBtn.addEventListener('click', stop);
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.job-runner').forEach(initRunner);
  });
})();
