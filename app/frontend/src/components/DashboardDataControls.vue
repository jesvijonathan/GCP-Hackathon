<template>
  <div class="dash-data-controls" :class="{ open, intro:introVisible }" ref="rootEl">
    <div class="ddc-header" @click="toggle">
      <strong style="flex:1;">Dashboard Data / Time</strong>
      <button class="toggle-btn" type="button">{{ open ? '×' : '⋯' }}</button>
    </div>
    <div v-if="open" class="ddc-body">
      <!-- Mode Selection (reuse merchant style) -->
      <div class="section row gap6">
        <button type="button" class="btn mode" :class="{active: windowMode}" @click="enableWindowMode">Window</button>
        <button type="button" class="btn mode" :class="{active: !windowMode}" @click="enableRangeMode">Since/Until</button>
      </div>
      <!-- Window Mode -->
      <div v-if="windowMode" class="section grid2 align-top">
        <div class="field-group">
          <label class="lbl">Window</label>
          <input v-model.trim="windowVal" class="inp" placeholder="6h" @keydown.enter.prevent="enterReload" />
        </div>
        <div class="field-group">
          <label class="lbl">Unit</label>
          <select v-model="unit" class="inp" @keydown.enter.prevent="enterReload">
            <option value="hour">Hour</option>
            <option value="day">Day</option>
            <option value="week">Week</option>
          </select>
        </div>
      </div>
      <!-- Range Mode -->
      <div v-else class="section">
        <div class="grid2">
          <div>
            <label class="lbl">Since (ISO/epoch)</label>
            <input v-model.trim="sinceStr" class="inp" placeholder="2025-01-01T00:00:00Z" @keydown.enter.prevent="enterReload" />
          </div>
          <div>
            <label class="lbl">Until (ISO/epoch)</label>
            <input v-model.trim="untilStr" class="inp" placeholder="Optional" @keydown.enter.prevent="enterReload" />
          </div>
        </div>
        <small class="help">Leave Until blank for open-ended; clear both to re-enable window mode.</small>
      </div>
      <!-- Dashboard-specific interval/lookback mapping -->
      <div class="section grid2">
        <div>
          <label class="lbl">Interval</label>
          <select v-model="interval" class="inp" @keydown.enter.prevent="enterReload">
            <option value="30m">30m</option>
            <option value="1h">1h</option>
            <option value="1d">1d</option>
          </select>
        </div>
        <div>
          <label class="lbl">Lookback Windows</label>
          <input type="number" min="6" max="500" v-model.number="lookback" class="inp" />
        </div>
      </div>
      <!-- Allow Future -->
      <div class="section row aic gap6 no-wrap">
        <label class="lbl-small toggle-item"><input type="checkbox" v-model="allowFuture" /> <span>Allow Future</span></label>
      </div>
      <!-- Simulation Block -->
      <div class="section sim-block">
        <div class="row aic jcsb">
          <label class="lbl" style="margin:0;">Simulated Time</label>
          <label class="lbl-small row aic" style="gap:4px;color:#0f766e;">
            <input type="checkbox" v-model="simEnabled" /> Enable
          </label>
        </div>
        <div class="now-line"><span class="now-val">{{ simNow }}</span><span v-if="simEnabled" class="accel">x{{ accel }}</span></div>
        <div v-if="simEnabled" class="sim-config">
          <div class="grid2">
            <div>
              <label class="lbl-small">Start ISO</label>
              <input v-model.trim="simStartInput" class="inp" placeholder="YYYY-MM-DDTHH:MM:SSZ" />
            </div>
            <div>
              <label class="lbl-small">Accel</label>
              <input type="number" step="0.1" min="0" v-model.number="accel" class="inp" />
            </div>
          </div>
          <div class="grid2">
            <div>
              <label class="lbl-small">Jump (+/-s)</label>
              <input type="number" v-model.number="jumpValue" class="inp" />
            </div>
            <div class="row gap6 aife">
              <button type="button" class="btn sm" @click="applyJump" :disabled="!jumpValue">Jump</button>
              <button type="button" class="btn sm alt" @click="resetSim">Reset</button>
            </div>
          </div>
          <div class="grid2">
            <button type="button" class="btn" :class="simRunning ? 'run':'pause'" @click="toggleSimRun" :disabled="!simEnabled">{{ simRunning ? 'Pause':'Start' }}</button>
            <button type="button" class="btn alt" @click="syncRealNow" :disabled="!simEnabled">Sync Now</button>
          </div>
        </div>
        <div v-else class="section" style="margin-top:4px;">
          <label class="lbl-small">Override Now (ISO)</label>
          <input v-model.trim="nowIso" class="inp" placeholder="Leave blank for real now" @keydown.enter.prevent="enterReload" />
        </div>
      </div>
      <!-- Auto Refresh -->
      <div class="section auto-refresh-wrap">
        <div class="row aic gap6 no-wrap auto-refresh-row">
          <label class="lbl-small toggle-item"><input type="checkbox" v-model="autoRefresh" /> <span>Auto Refresh</span></label>
          <div v-if="autoRefresh" class="refresh-inline">
            <input type="number" min="2" v-model.number="refreshSec" class="inp inp-sm refresh-input" @keydown.enter.prevent="enterReload" placeholder="10" />
            <span class="ri-s">s</span>
          </div>
        </div>
        <transition name="fade">
          <div v-if="autoRefresh" class="refresh-meta">
            <div class="refresh-countdown" :title="'Next refresh in ' + nextRefreshIn + 's'">Next: <strong>{{ nextRefreshIn }}</strong>s</div>
            <div class="refresh-counter" :title="'Auto refresh count'">Count: <strong>{{ refreshCount }}</strong></div>
          </div>
        </transition>
      </div>
      <!-- Buttons -->
      <div class="section row gap6">
        <button type="button" class="btn primary" :disabled="loading" @click="emitReload">{{ loading? 'Loading...' : 'Fetch' }}</button>
        <button type="button" class="btn alt" @click="emitEnsure">Ensure</button>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue';

// Reuse simulation storage key to keep time in sync with merchant panel.
const LS_SIM_KEY = 'merchantSimState';
// Use same base key suffix pattern but distinct to avoid overwriting merchant state unintentionally.
// We will still listen for merchant key to synchronize simulation/time controls.
const STATE_KEY = 'dashboardDataCtrlState_v2';
const MERCHANT_STATE_KEY = 'merchantDataCtrlState_v1';
let lastAppliedTs = 0;
let simTimer = null;
function useSim(){
  const simEnabled = ref(false); const simRunning = ref(false); const accel = ref(1.0);
  const simStartInput = ref(''); const simBaseIso = ref(''); const nowIso = ref(''); const jumpValue = ref(0);
  let wallStartMs = Date.now(); const simClock = ref(Date.now());
  let lastSimDispatchMs = 0; // throttle event dispatch to consumers
  const simNow = computed(()=>{
    if(!simEnabled.value) return nowIso.value || new Date().toISOString();
    if(!simBaseIso.value) return new Date().toISOString();
    const base = Date.parse(simBaseIso.value); if(!isFinite(base)) return new Date().toISOString();
    const elapsed = simClock.value - wallStartMs; const adv = simRunning.value ? elapsed * Math.max(0,accel.value) : 0; return new Date(base + adv).toISOString();
  });
  function initBase(){ const raw=(simStartInput.value||'').trim(); const needsZ=/\dT\d{2}:\d{2}:\d{2}(\.\d+)?$/.test(raw)&&!/[zZ]|[\+\-]\d{2}:?\d{2}$/.test(raw); const s=raw?(needsZ?raw+'Z':raw):new Date().toISOString(); simBaseIso.value=new Date(Date.parse(s)).toISOString(); wallStartMs=Date.now(); nowIso.value=simBaseIso.value; }
  function tick(){ if(!simEnabled.value) return; simClock.value=Date.now(); nowIso.value=simNow.value; }
  function loop(){ if(simTimer) clearInterval(simTimer); simTimer=setInterval(tick,1000); }
  function toggleSimRun(){ if(!simEnabled.value) return; if(!simRunning.value){ if(!simBaseIso.value) initBase(); simRunning.value=true; } else { simBaseIso.value=simNow.value; wallStartMs=Date.now(); simRunning.value=false; } }
  function resetSim(){ if(!simEnabled.value) return; simRunning.value=false; initBase(); }
  function applyJump(){ if(!simEnabled.value||!jumpValue.value) return; const cur=Date.parse(simNow.value); const delta=jumpValue.value*1000; if(simRunning.value){ const elapsed=Date.now()-wallStartMs; const newBase=cur+delta - elapsed*Math.max(0,accel.value); simBaseIso.value=new Date(newBase).toISOString(); } else { simBaseIso.value=new Date(cur+delta).toISOString(); } jumpValue.value=0; nowIso.value=simNow.value; }
  function syncRealNow(){ if(!simEnabled.value) return; simBaseIso.value=new Date().toISOString(); wallStartMs=Date.now(); nowIso.value=simBaseIso.value; }
  function persist(){ try{ localStorage.setItem(LS_SIM_KEY, JSON.stringify({ enabled:simEnabled.value, running:simRunning.value, accel:accel.value, simBaseIso:simBaseIso.value, simStartInput:simStartInput.value, lastSimNow:simNow.value })); }catch{} }
  function load(){ try{ const raw=localStorage.getItem(LS_SIM_KEY); if(!raw) return; const st=JSON.parse(raw); simEnabled.value=!!st.enabled; simRunning.value=!!st.running; accel.value=Number(st.accel)||1; simStartInput.value=st.simStartInput||''; simBaseIso.value=st.simBaseIso||st.lastSimNow||''; if(simEnabled.value) loop(); }catch{} }
  watch([simEnabled, simRunning, accel, simBaseIso, simStartInput], persist);
  watch(simEnabled, en=>{ if(en){ if(!simBaseIso.value) initBase(); loop(); } else { if(simTimer) clearInterval(simTimer); }});
  // --- New: persist plain simulated now (or manual override) for other components (MerchantRisk, DashboardOverview) ---
  // These components currently read localStorage key 'simNow' to anchor time range; previously we never set it so they fell back to real time.
  function writeGlobalSimNow(v){
    try {
      if(v) localStorage.setItem('simNow', v); else localStorage.removeItem('simNow');
      // Throttled custom event so dashboard/merchant charts can react
      const now = Date.now();
      const FORCE_INTERVAL_MS = 5000; // min spacing
      if(now - lastSimDispatchMs > FORCE_INTERVAL_MS){
        lastSimDispatchMs = now;
        try { window.dispatchEvent(new CustomEvent('sim-now-updated', { detail:{ simNow: v, running: simRunning.value, accel: accel.value } })); } catch {}
      }
    } catch {}
  }
  // Persist every change (tick) while enabled; when disabled use manual override nowIso if present.
  watch(simNow, v=>{ if(simEnabled.value) writeGlobalSimNow(v); });
  watch([simEnabled, nowIso], ()=>{
    if(simEnabled.value){ writeGlobalSimNow(simNow.value); }
    else { writeGlobalSimNow(nowIso.value || ''); }
  });
  // On load, if simulation was previously enabled ensure global key is restored so views load correctly first fetch.
  onMounted(()=>{ if(simEnabled.value){ writeGlobalSimNow(simNow.value); } else if(nowIso.value){ writeGlobalSimNow(nowIso.value); } });
  load();
  return { simEnabled, simRunning, accel, simStartInput, nowIso, simNow, jumpValue, toggleSimRun, resetSim, applyJump, syncRealNow };
}

export default {
  name:'DashboardDataControls',
  props:{ loading:{ type:Boolean, default:false } },
  emits:['reload','ensure'],
  setup(props,{emit}){
  // Start collapsed by default; show intro full opacity 2s
  const open = ref(false);
  const introVisible = ref(true);
  const rootEl = ref(null);
    // Shared with merchant style
    const windowMode = ref(true);
    const windowVal = ref('6h');
    const sinceStr = ref(''); const untilStr = ref('');
    const unit = ref('hour');
    // Dashboard specific
  const interval = ref('30m');
  const lookback = ref(48);
  // Added for parity with merchant controls
  const limit = ref(5000);
  const order = ref('desc');
    const allowFuture = ref(false);
    const autoRefresh = ref(false); const refreshSec = ref(10); let refreshTimer=null; let countdownTimer=null;
    const nextRefreshIn = ref(0); const refreshCount = ref(0);
    const { simEnabled, simRunning, accel, simStartInput, nowIso, simNow, jumpValue, toggleSimRun, resetSim, applyJump, syncRealNow } = useSim();

    function toggle(){ open.value=!open.value; }
    function enableWindowMode(){ windowMode.value=true; if(!windowVal.value) windowVal.value='6h'; }
    function enableRangeMode(){ windowMode.value=false; if(!sinceStr.value) sinceStr.value=new Date(Date.now()-6*3600*1000).toISOString(); }
    function toggleMode(){ windowMode.value = !windowMode.value; if(windowMode.value){ enableWindowMode(); } else { enableRangeMode(); } }
  function payload(){ return { interval: interval.value, lookback: lookback.value, window: windowMode.value? windowVal.value:'', since: windowMode.value? '': sinceStr.value, until: windowMode.value? '': untilStr.value, unit: unit.value, limit: limit.value, order: order.value, allowFuture: allowFuture.value, now: simEnabled.value? simNow.value : nowIso.value }; }

    function emitReload(){ const pl=payload(); try{ console.log('[DashDC] reload', pl);}catch{} emit('reload', pl); if(autoRefresh.value){ nextRefreshIn.value = refreshSec.value; } }
    function emitEnsure(){ const pl=payload(); try{ console.log('[DashDC] ensure', pl);}catch{} emit('ensure', pl); if(autoRefresh.value){ nextRefreshIn.value = refreshSec.value; } }
    function enterReload(){ emitReload(); }
    function scheduleAuto(){
      if(refreshTimer){ clearInterval(refreshTimer); refreshTimer=null; }
      if(countdownTimer){ clearInterval(countdownTimer); countdownTimer=null; }
      if(autoRefresh.value){
        const ms = Math.max(2, refreshSec.value)*1000;
        nextRefreshIn.value = Math.max(2, refreshSec.value);
        refreshTimer = setInterval(()=>{ emitReload(); refreshCount.value++; nextRefreshIn.value = Math.max(2, refreshSec.value); }, ms);
        countdownTimer = setInterval(()=>{ if(nextRefreshIn.value>0) nextRefreshIn.value--; }, 1000);
      } else { nextRefreshIn.value=0; }
    }
    function persist(){
      try{
        const ts = Date.now();
        const st = { wm:windowMode.value,w:windowVal.value,s:sinceStr.value,u:untilStr.value,unit:unit.value,int:interval.value,lb:lookback.value,lim:limit.value,ord:order.value,fut:allowFuture.value,ar:autoRefresh.value,rs:refreshSec.value,lastUpdated:ts };
        lastAppliedTs = ts;
        localStorage.setItem(STATE_KEY, JSON.stringify(st));
      }catch{}
    }
    function applyState(st){
      if(st.lastUpdated && st.lastUpdated <= lastAppliedTs) return;
      if(typeof st.wm==='boolean') windowMode.value=st.wm;
      if(st.w) windowVal.value=st.w;
      if(st.s!==undefined) sinceStr.value=st.s;
      if(st.u!==undefined) untilStr.value=st.u;
      if(st.unit) unit.value=st.unit;
      if(st.int) interval.value=st.int;
      if(st.lb) lookback.value=Number(st.lb)||lookback.value;
      if(st.lim) limit.value=Number(st.lim)||limit.value;
      if(st.ord) order.value=st.ord;
      if(typeof st.fut==='boolean') allowFuture.value=st.fut;
      if(typeof st.ar==='boolean') autoRefresh.value=st.ar;
      if(st.rs) refreshSec.value=Number(st.rs)||refreshSec.value;
      if(st.lastUpdated) lastAppliedTs = st.lastUpdated;
    }
    function loadPersisted(){
      try{ const raw=localStorage.getItem(STATE_KEY); if(raw){ applyState(JSON.parse(raw)); } }catch{}
      // Also pull merchant state once at mount for cross-page sync of time/window baseline
      try{ const mraw=localStorage.getItem(MERCHANT_STATE_KEY); if(mraw){ const mst=JSON.parse(mraw); applyState(mst); } }catch{}
    }
  watch([windowMode, windowVal, sinceStr, untilStr, unit, interval, lookback, limit, order, allowFuture], persist);
    watch(autoRefresh, v=>{ scheduleAuto(); persist(); if(v){ emitReload(); refreshCount.value++; } });
    watch(refreshSec, ()=>{ if(autoRefresh.value){ scheduleAuto(); persist(); }});
    function updateBottomReserve(){
      try{
        if(!rootEl.value || !open.value){
          document.documentElement.style.setProperty('--panel-bottom-pad','0px');
          return;
        }
        const h = rootEl.value.getBoundingClientRect().height;
        document.documentElement.style.setProperty('--panel-bottom-pad', (h+24)+'px');
      }catch{}
    }
    function storageListener(e){
      if(e.key===STATE_KEY || e.key===MERCHANT_STATE_KEY){
        if(e.newValue){ try { const st=JSON.parse(e.newValue); applyState(st); } catch {} }
      }
    }
  onMounted(()=>{ loadPersisted(); if(autoRefresh.value){ scheduleAuto(); emitReload(); refreshCount.value++; } else { emitReload(); } nextTick(updateBottomReserve); window.addEventListener('resize', updateBottomReserve); window.addEventListener('storage', storageListener); setTimeout(()=>{ introVisible.value=false; },2000); });
    // Listen for external overview or merchant-driven interval/lookback updates
    function extListener(e){
      try {
        if(!e || !e.detail) return;
        const { interval:extInt, lookback:extLb } = e.detail;
        let changed = false;
        if(extInt && extInt !== interval.value){ interval.value = extInt; changed = true; }
        if(extLb && Number(extLb) !== lookback.value){ lookback.value = Number(extLb); changed = true; }
        if(changed){ persist(); }
      } catch {}
    }
    window.addEventListener('dash-interval-updated', extListener);
    watch(open, ()=> nextTick(updateBottomReserve));
    watch([autoRefresh, refreshSec], ()=> nextTick(updateBottomReserve));
  onBeforeUnmount(()=>{ if(refreshTimer) clearInterval(refreshTimer); if(countdownTimer) clearInterval(countdownTimer); document.documentElement.style.removeProperty('--panel-bottom-pad'); window.removeEventListener('resize', updateBottomReserve); window.removeEventListener('storage', storageListener); });
    onBeforeUnmount(()=>{ window.removeEventListener('dash-interval-updated', extListener); });

    return { open, introVisible, toggle, rootEl, windowMode, windowVal, enableWindowMode, enableRangeMode, toggleMode, sinceStr, untilStr, unit, interval, lookback, limit, order, allowFuture, autoRefresh, refreshSec, nextRefreshIn, refreshCount, simEnabled, simRunning, accel, simStartInput, nowIso, simNow, jumpValue, toggleSimRun, resetSim, applyJump, syncRealNow, emitReload, emitEnsure, enterReload };
  }
};
</script>

<style scoped>
.dash-data-controls{position:fixed;right:14px;bottom:14px;width:340px;background:#ffffff;border:1px solid #14b8a6;border-radius:14px;box-shadow:0 4px 10px rgba(0,0,0,0.08),0 8px 24px rgba(0,128,128,0.18);font-size:12px;z-index:10;backdrop-filter:blur(2px);opacity:1;transition:opacity .25s ease, box-shadow .25s ease;} 
.dash-data-controls.intro{opacity:1 !important;}
.dash-data-controls:not(.open){opacity:.1;}
.dash-data-controls:not(.open):hover,.dash-data-controls:not(.open):focus-within{opacity:1;}
.ddc-header{display:flex;align-items:center;padding:8px 10px;background:#f0fdfa;border-bottom:1px solid #14b8a6;cursor:pointer;font-size:12px;color:#065f5b;}
.toggle-btn{background:transparent;border:none;font-size:16px;cursor:pointer;color:#065f5b;font-weight:700;}
.ddc-body{display:flex;flex-direction:column;gap:10px;padding:10px 12px;max-height:460px;overflow:auto;}
.section{display:flex;flex-direction:column;gap:6px;}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px;}
.align-top{align-items:start;}
.field-group{display:flex;flex-direction:column;gap:4px;}
.lbl{font-size:11px;font-weight:600;color:#036d69;}
.lbl-small{font-size:10px;font-weight:600;color:#6b7280;}
.help{font-size:10px;color:#6b7280;margin-top:4px;display:block;}
.inp{width:100%;padding:6px 8px;border:1px solid #14b8a6;border-radius:6px;font-size:11px;}
.inp-sm{width:60px;padding:4px 6px;font-size:10px;}
.btn{border:1px solid #14b8a6;background:#f0fdfa;color:#065f5b;padding:8px 10px;border-radius:6px;font-size:11px;font-weight:600;cursor:pointer;transition:.15s;}
.btn:hover{background:#14b8a6;color:#fff;}
.btn.alt{background:#ffffff;}
.btn.primary{flex:1;background:#14b8a6;color:#fff;}
.btn.primary[disabled]{background:#94a3b8;border-color:#94a3b8;color:#fff;cursor:default;opacity:.8;}
.btn.sm{padding:6px 8px;font-size:10px;}
.btn.run{background:#dcfce7;color:#065f46;}
.btn.pause{background:#fee2e2;color:#991b1b;}
.mode.active{background:#14b8a6;color:#fff;}
.section.row .btn.mode{flex:1;display:flex;align-items:center;justify-content:center;min-width:0;}
.section.row .btn.mode:not(.active){background:#ffffff;}
.section.row.gap6{gap:8px;}
.now-line{display:flex;align-items:center;gap:6px;font-weight:600;color:#0f766e;font-size:11px;}
.now-val{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.accel{font-size:10px;color:#6b7280;font-weight:600;}
.sim-config{display:flex;flex-direction:column;gap:8px;margin-top:4px;}
.row{display:flex;flex-direction:row;}
.aic{align-items:center;}
.aife{align-items:flex-end;}
.jcsb{justify-content:space-between;}
.gap6{gap:6px;}
.no-wrap{flex-wrap:nowrap;}
.toggle-item{display:inline-flex;align-items:center;gap:4px;white-space:nowrap;}
.refresh-inline{display:flex;align-items:center;gap:4px;}
.ri-s{font-size:10px;color:#6b7280;}
.sim-block{border:1px solid #e2e8f0;padding:8px 8px 10px;border-radius:8px;background:#f8fafc;}
.auto-refresh-row{justify-content:center;}
.auto-refresh-wrap{display:flex;flex-direction:column;gap:4px;}
.refresh-meta{display:flex;justify-content:space-between;align-items:center;font-size:10px;color:#036d69;padding:2px 4px 0;}
.refresh-meta strong{font-size:11px;color:#065f5b;}
.fade-enter-active,.fade-leave-active{transition:opacity .25s ease;}
.fade-enter-from,.fade-leave-to{opacity:0;}
</style>
