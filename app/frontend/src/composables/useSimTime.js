import { ref, computed, watch, onBeforeUnmount } from 'vue';

export function useSimTime(persistKey = 'merchantSimState') {
  const simEnabled = ref(false);
  const simRunning = ref(false);
  const accel = ref(1);
  const simStartInput = ref('');
  const simBaseIso = ref('');
  const jumpValue = ref(0);
  const liveSimUpdate = ref(true);
  const liveSimIntervalSec = ref(5);

  let wallStartMs = Date.now();
  const simClock = ref(Date.now());
  let timer = null;

  const normalizeStart = (raw) => {
    if (!raw) return Date.now();
    const needsZ = /\dT\d{2}:\d{2}:\d{2}(\.\d+)?$/.test(raw) && !(/[zZ]|[+\-]\d{2}:?\d{2}$/.test(raw));
    const s = needsZ ? raw + 'Z' : raw;
    const ts = Date.parse(s);
    return isFinite(ts) ? ts : Date.now();
  };

  function initBase() {
    const base = normalizeStart(simStartInput.value.trim());
    simBaseIso.value = new Date(base).toISOString();
    wallStartMs = Date.now();
    simClock.value = Date.now();
  }

  const simNow = computed(() => {
    if (!simEnabled.value) return new Date().toISOString();
    if (!simBaseIso.value) return new Date().toISOString();
    const base = Date.parse(simBaseIso.value);
    if (!isFinite(base)) return new Date().toISOString();
    const elapsedReal = simClock.value - wallStartMs;
    const adv = simRunning.value ? elapsedReal * Math.max(0, accel.value) : 0;
    return new Date(base + adv).toISOString();
  });

  let lastFetchMs = 0;
  let inFlight = false;
  function tick(onFetch) {
    if (!simEnabled.value) return;
    simClock.value = Date.now();
    if (simRunning.value && liveSimUpdate.value && typeof onFetch === 'function') {
      const now = Date.now();
      if (!inFlight && (now - lastFetchMs) >= Math.max(1000, liveSimIntervalSec.value * 1000)) {
        lastFetchMs = now;
        inFlight = true;
        Promise.resolve(onFetch()).finally(() => { inFlight = false; });
      }
    }
  }

  function startLoop(onFetch) {
    stopLoop();
    timer = setInterval(() => tick(onFetch), 1000);
  }
  function stopLoop() { if (timer) { clearInterval(timer); timer = null; } }

  function toggleRun(onFetch) {
    if (!simEnabled.value) return;
    if (!simRunning.value) {
      if (!simBaseIso.value) initBase();
      simRunning.value = true;
      if (liveSimUpdate.value && typeof onFetch === 'function') onFetch();
    } else {
      simBaseIso.value = simNow.value;
      wallStartMs = Date.now();
      simRunning.value = false;
    }
  }
  function reset() { if (simEnabled.value) { simRunning.value = false; initBase(); } }
  function applyJump() {
    if (!simEnabled.value || !jumpValue.value) return;
    const current = Date.parse(simNow.value);
    const delta = jumpValue.value * 1000;
    if (simRunning.value) {
      const elapsed = Date.now() - wallStartMs;
      const newBase = current + delta - elapsed * Math.max(0, accel.value);
      simBaseIso.value = new Date(newBase).toISOString();
    } else {
      simBaseIso.value = new Date(current + delta).toISOString();
    }
    jumpValue.value = 0;
  }
  function syncRealNow() {
    if (!simEnabled.value) return;
    simBaseIso.value = new Date().toISOString();
    wallStartMs = Date.now();
  }

  const persist = () => {
    try {
      localStorage.setItem(persistKey, JSON.stringify({
        enabled: simEnabled.value,
        running: simRunning.value,
        accel: accel.value,
        simBaseIso: simBaseIso.value,
        simStartInput: simStartInput.value,
        live: liveSimUpdate.value,
        liveInterval: liveSimIntervalSec.value,
        saved: Date.now(),
      }));
    } catch {}
  };
  const load = () => {
    try {
      const raw = localStorage.getItem(persistKey);
      if (!raw) return;
      const st = JSON.parse(raw);
      simEnabled.value = !!st.enabled;
      simRunning.value = !!st.running;
      accel.value = Number(st.accel) || 1;
      simBaseIso.value = st.simBaseIso || '';
      simStartInput.value = st.simStartInput || '';
      liveSimUpdate.value = st.live !== false;
      if (typeof st.liveInterval === 'number') liveSimIntervalSec.value = st.liveInterval;
      if (simEnabled.value) startLoop();
    } catch {}
  };

  watch([simEnabled, simRunning, accel, simBaseIso, simStartInput, liveSimUpdate, liveSimIntervalSec], persist);
  watch(simStartInput, v => { if (simEnabled.value && !simRunning.value && v.trim()) initBase(); });

  onBeforeUnmount(stopLoop);

  return {
    // state
    simEnabled, simRunning, accel, simStartInput, simBaseIso, jumpValue,
    liveSimUpdate, liveSimIntervalSec, simNow,
    // control
    toggleRun, reset, applyJump, syncRealNow, initBase, startLoop, stopLoop, load,
  };
}
