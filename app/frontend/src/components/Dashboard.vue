<template>
  <div class="merchant-dashboard teal-theme">
    <DashboardSideBar
      :merchants="merchants"
      :selectedMerchant="selectedMerchant"
      :searchQuery="searchQuery"
      :filters="filters"
      @select-merchant="selectMerchant"
      @route-to-merchant="routeToMerchantPage"
      @delete-merchant="deleteMerchantFromDashboard"
      @update:searchQuery="searchQuery = $event"
      @update:filters="filters = $event"
    />

    <section class="details-panel" v-if="selectedMerchant">
      <div class="focus-toolbar">
        <div class="focus-left">
          <h3 class="focus-title">Merchant: {{ selectedMerchant?.name || selectedMerchant?.id }}</h3>
        </div>
        <div class="focus-actions">
          <button class="focus-exit-btn" @click="exitFocus" title="Return to dashboard overview">← Back to Overview</button>
        </div>
      </div>
      <div class="merchant-layout">
        <div class="left-col">
          <DashboardMerchantInfo :merchant="selectedMerchant" />
          <DashboardActionCenter :merchant="selectedMerchant" @action="handleMerchantAction" @manage-restrictions="openRestrictModal" />
        </div>
        <div class="right-col">
          <MerchantRisk :merchant="selectedMerchant.id" />
        </div>
      </div>
      <div v-if="showRestrictModal" class="modal-overlay">
        <div class="modal-content large">
          <div class="modal-header">
            <h3>Manage Restrictions – {{ selectedMerchant?.name }}</h3>
            <button class="close-btn" @click="closeRestrictModal">×</button>
          </div>
          <div class="modal-body">
            <form @submit.prevent="applyRestrictions" class="restriction-form">
              <div class="form-grid">
                <div class="form-section">
                  <h4>Core Limits</h4>
                  <label class="form-field"><span>Daily Transaction Limit</span><input v-model.number="restrictionForm.dailyTransactionLimit" type="number" min="0" class="form-input" /></label>
                  <label class="form-field"><span>Monthly Transaction Limit</span><input v-model.number="restrictionForm.monthlyTransactionLimit" type="number" min="0" class="form-input" /></label>
                  <label class="form-field"><span>Max Transaction Amount</span><input v-model.number="restrictionForm.maxTransactionAmount" type="number" min="0" class="form-input" /></label>
                </div>
                <div class="form-section">
                  <h4>Additional</h4>
                  <label class="form-field"><span>Allowed Countries</span><input v-model="restrictionForm.allowedCountries" type="text" class="form-input" placeholder="e.g., US, CA" /></label>
                  <label class="form-field check-field"><input v-model="restrictionForm.requireAdditionalVerification" type="checkbox" /> <span>Require Additional Verification</span></label>
                  <label class="form-field check-field"><input v-model="restrictionForm.blockInternationalTransactions" type="checkbox" /> <span>Block International Transactions</span></label>
                </div>
                <div class="form-section">
                  <h4>Category Codes</h4>
                  <div class="flex-row"><input v-model="newCategoryCode" type="text" class="form-input" placeholder="Add code" @keyup.enter.prevent="addCategoryCode" /><button type="button" class="mini-btn" @click="addCategoryCode">Add</button></div>
                  <div class="code-tags" v-if="restrictionForm.restrictedCategoryCodes.length">
                    <span class="code-tag" v-for="code in restrictionForm.restrictedCategoryCodes" :key="code">{{ code }} <button type="button" @click="removeCategoryCode(code)">×</button></span>
                  </div>
                </div>
                <div class="form-section">
                  <h4>Timing</h4>
                  <label class="form-field"><span>Start Date</span><input v-model="restrictionForm.startDate" type="date" class="form-input" /></label>
                  <label class="form-field"><span>End Date</span><input v-model="restrictionForm.endDate" type="date" class="form-input" /></label>
                  <label class="form-field"><span>Reason *</span><textarea v-model="restrictionForm.reason" rows="4" class="form-textarea" placeholder="Explain why..." required></textarea></label>
                </div>
              </div>
            </form>
          </div>
          <div class="modal-footer">
            <button class="btn-cancel" type="button" @click="closeRestrictModal">Cancel</button>
            <button class="btn-secondary" type="button" @click="clearRestrictions" :disabled="savingRestriction">Clear</button>
            <button class="btn-primary" type="button" @click="applyRestrictions" :disabled="savingRestriction || !restrictionForm.reason.trim()">{{ savingRestriction ? 'Saving...' : 'Apply Restrictions' }}</button>
          </div>
        </div>
      </div>
    </section>

    <section class="details-panel" v-else>
      <DashboardOverview
        :api-base="apiBase"
        @select-merchant-by-name="onSelectMerchantByName"
        @deselect-merchant="selectedMerchant = null"
        ref="overviewRef"
      />
    </section>
    <DashboardDataControls @reload="applyControls" @ensure="applyControlsEnsure" />
  </div>
</template>
<script>
import { toast } from "vue3-toastify";
import "vue3-toastify/dist/index.css";
import DashboardSideBar from './DashboardSideBar.vue';
import DashboardMerchantInfo from './DashboardMerchantInfo.vue';
import DashboardActionCenter from './DashboardActionCenter.vue';
import MerchantRisk from './MerchantRisk.vue';
import DashboardOverview from './DashboardOverview.vue';
import DashboardDataControls from './DashboardDataControls.vue';

export default {
  name: "MerchantDashboard",
  components: { DashboardSideBar, DashboardMerchantInfo, DashboardActionCenter, MerchantRisk, DashboardOverview, DashboardDataControls },
  data(){
    return {
      merchants: [],
      selectedMerchant: null,
      searchQuery: '',
      filters: { riskHigh:false, statusActive:false, dateRangeThisMonth:false },
      apiBase: 'http://localhost:8000',
      loading:false,
      error:null,
      showRestrictModal:false,
      savingRestriction:false,
      restrictionForm: {
        dailyTransactionLimit: null,
        monthlyTransactionLimit: null,
        maxTransactionAmount: null,
        restrictedCategoryCodes: [],
        allowedCountries: '',
        requireAdditionalVerification: false,
        blockInternationalTransactions: false,
        reason: '',
        startDate: new Date().toISOString().split('T')[0],
        endDate: ''
      },
      newCategoryCode: ''
    };
  },
  methods: {
    selectMerchant(m){ this.selectedMerchant = m; this.refreshSelectedDetails(); },
    exitFocus(){ this.selectedMerchant = null; },
    onSelectMerchantByName(name){ if(this.selectedMerchant && this.selectedMerchant.id===name){ this.selectedMerchant=null; return;} const m=this.merchants.find(mm=>mm.id===name); if(m) this.selectMerchant(m); },
    // Defensive: if an external attempt tries to select a merchant not loaded, ignore gracefully
    ensureValidSelection(){
      if(this.selectedMerchant && !this.merchants.some(m=>m.id===this.selectedMerchant.id)){
        this.selectedMerchant = null; // drop invalid selection
      }
    },
    routeToMerchantPage(id){ try { this.$router.push(`/merchant/${id}`);} catch(e){ toast.error('Navigation failed'); } },
    handleMerchantAction(actionType){ if(!this.selectedMerchant){ toast.error('No merchant selected'); return; } if(actionType==='permanent-ban'){ toast.error(`${this.selectedMerchant.name} permanently banned!`);} else if(actionType==='shadow-ban'){ toast.warning(`${this.selectedMerchant.name} shadow banned!`);} else if(actionType==='continue'){ toast.success(`${this.selectedMerchant.name} continues operating.`);} },
    async fetchOverviewAndEnrich(){
      this.loading=true; this.error=null;
      try {
        // Load persisted dashboard interval if available (aligns with overview & merchant risk persistence)
        let interval = '30m';
        try { const raw = localStorage.getItem('dashboardDataCtrlState_v2'); if(raw){ const st = JSON.parse(raw); if(st && st.int) interval = st.int; } } catch {}
        const [listResp, overviewResp] = await Promise.all([
          fetch(`${this.apiBase}/v1/merchants`),
          fetch(`${this.apiBase}/v1/dashboard/overview?interval=${encodeURIComponent(interval)}&top=30&include_all=true`)
        ]);
        if(!listResp.ok) throw new Error('merchant list '+listResp.status);
        if(!overviewResp.ok) throw new Error('overview '+overviewResp.status);
        const listJson=await listResp.json();
        const overview=await overviewResp.json();
  let names = listJson.merchants || [];
        const evalMap={}; (overview.all_merchants||[]).forEach(r=>{ evalMap[r.merchant]=r; });
        const coerceBool = (v, d=true)=>{
          if(v===true) return true; if(v===false) return false;
          if(v==null) return d;
          if(typeof v === 'number') return v !== 0;
          const s = String(v).trim().toLowerCase();
          if(['false','0','no','off','inactive','disabled'].includes(s)) return false;
          if(['true','1','yes','on','active','enabled'].includes(s)) return true;
          return d; };
        // Build merchant objects. Previously we excluded 'MER001' explicitly; removing that so it appears.
        // If you need to exclude demo merchants, move that logic to a config flag instead of hardcoding.
        if((!names || !names.length) && (overview.all_merchants && overview.all_merchants.length)){
          // Fallback: derive names from overview payload if list endpoint returned empty.
            console.warn('[Dashboard] /v1/merchants returned empty; deriving from overview.all_merchants');
            names = overview.all_merchants.map(r=>r.merchant).filter(Boolean);
        }
        console.debug('[Dashboard] merchant names resolved:', names);
        this.merchants = names.map(n=>{
          const e=evalMap[n]||{};
          let activationFlag = coerceBool(e.activation_flag, true);
          // Fallback: if details.status explicitly inactive, force inactive
          const statusRaw = (e.details?.status || '').toString().toLowerCase();
          if(statusRaw==='inactive') activationFlag = false;
          const autoAction = coerceBool(e.auto_action, false);
          const restrictions = e.restrictions||null;
          const hasRestrictions = !!(e.has_restrictions || (restrictions && Object.keys(restrictions).length));
          return {
            id:n,
            name:n,
            status: activationFlag ? (statusRaw || 'active') : 'inactive',
            lastActivity:e.window_end,
            riskMetrics:{ riskScore: e.risk_score },
            evaluation:e,
            activation_flag: activationFlag,
            auto_action: autoAction,
            restrictions,
            has_restrictions: hasRestrictions,
            alerts:[],
            transactions:{ monthly:{ volume:[] } }
          };
        });
        if(this.selectedMerchant){
          const upd=this.merchants.find(m=>m.id===this.selectedMerchant.id);
          if(upd) this.selectedMerchant=upd;
        }
      } catch(err){ this.error=err.message; toast.error('Failed loading merchants: '+err.message);} finally { this.loading=false; }
      // After refresh, ensure any pre-selected merchant still exists
      this.ensureValidSelection();
    },
    async refreshSelectedDetails(){
      if(!this.selectedMerchant) return;
      try {
        // Merchant doc refresh
        try {
          const md = await fetch(`${this.apiBase}/v1/merchants/${encodeURIComponent(this.selectedMerchant.id)}`);
          if(md.ok){
            const mj = await md.json();
            const doc = mj.merchant||{};
            this.selectedMerchant.activation_flag = doc.activation_flag !== false;
            this.selectedMerchant.auto_action = !!doc.auto_action;
            this.selectedMerchant.status = (doc.activation_flag===false?'inactive':(doc.status||'active')).toLowerCase();
            this.selectedMerchant.updated_at = doc.updated_at;
            this.selectedMerchant.created_at = doc.created_at;
            this.selectedMerchant.restrictions = doc.restrictions || this.selectedMerchant.restrictions;
            this.selectedMerchant.has_restrictions = !!(doc.restrictions && Object.keys(doc.restrictions).length);
          }
        } catch {}
        // Risk summary refresh (ensures panel reflects current merchant selection)
  // Use persisted dashboard interval & derive window from merchant risk state if available
  let interval = '30m';
  try { const raw = localStorage.getItem('dashboardDataCtrlState_v2'); if(raw){ const st=JSON.parse(raw); if(st && st.int) interval = st.int; } } catch {}
  let windowParam = '6h';
  try { const mrRaw = localStorage.getItem('merchantRiskState_v1'); if(mrRaw){ const ms = JSON.parse(mrRaw); if(ms && ms.lookbackHours){ windowParam = ms.lookbackHours + 'h'; } } } catch {}
  const r = await fetch(`${this.apiBase}/v1/${this.selectedMerchant.id}/risk-eval/summary?interval=${encodeURIComponent(interval)}&window=${encodeURIComponent(windowParam)}`);
        if(r.ok){
          const js = await r.json();
            this.selectedMerchant.evaluationSummary = js;
            // Sync quick riskMetrics for other child components (e.g., DashboardMerchantInfo / RiskMetrics)
            const latestScore = js?.latest?.score ?? js?.latest?.risk_score ?? (js?.latest?.components?.total);
            if(latestScore != null){
              if(!this.selectedMerchant.riskMetrics) this.selectedMerchant.riskMetrics = {};
              this.selectedMerchant.riskMetrics.riskScore = latestScore;
            }
        }
      } catch(e){ /* silent */ }
    },
    applyControls(p){ const ov=this.$refs.overviewRef; if(!ov) return; if(p.interval && ov.interval!==p.interval) ov.interval=p.interval; if(p.lookback && ov.lookback!==p.lookback) ov.lookback=p.lookback; ov.fetchData(); },
    applyControlsEnsure(p){ this.applyControls(p); const ov=this.$refs.overviewRef; if(ov) ov.fetchSeries(); },
    openRestrictModal(){ if(!this.selectedMerchant) return; const r=this.selectedMerchant.restrictions || this.selectedMerchant.evaluation?.restrictions || {}; this.restrictionForm = { dailyTransactionLimit: r.dailyTransactionLimit ?? null, monthlyTransactionLimit: r.monthlyTransactionLimit ?? null, maxTransactionAmount: r.maxTransactionAmount ?? null, restrictedCategoryCodes: Array.isArray(r.restrictedCategoryCodes)?[...r.restrictedCategoryCodes]:[], allowedCountries: r.allowedCountries||'', requireAdditionalVerification: !!r.requireAdditionalVerification, blockInternationalTransactions: !!r.blockInternationalTransactions, reason: r.reason||'', startDate: r.startDate || new Date().toISOString().split('T')[0], endDate: r.endDate||'' }; this.newCategoryCode=''; this.showRestrictModal=true; },
    closeRestrictModal(){ this.showRestrictModal=false; },
    addCategoryCode(){ const code=(this.newCategoryCode||'').trim(); if(!code) return; if(!this.restrictionForm.restrictedCategoryCodes.includes(code)) this.restrictionForm.restrictedCategoryCodes.push(code); this.newCategoryCode=''; },
    removeCategoryCode(code){ const ix=this.restrictionForm.restrictedCategoryCodes.indexOf(code); if(ix>=0) this.restrictionForm.restrictedCategoryCodes.splice(ix,1); },
    async applyRestrictions(){ if(!this.selectedMerchant) return; if(!this.restrictionForm.reason.trim()) return; this.savingRestriction=true; try { const body={ restrictions:{ ...this.restrictionForm } }; const resp=await fetch(`${this.apiBase}/v1/merchants/${encodeURIComponent(this.selectedMerchant.id)}`, { method:'PATCH', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(body) }); if(!resp.ok) throw new Error('Restriction update failed '+resp.status); const js=await resp.json(); const doc=js.merchant||{}; this.selectedMerchant.restrictions=doc.restrictions||body.restrictions; this.selectedMerchant.has_restrictions=!!(this.selectedMerchant.restrictions && Object.keys(this.selectedMerchant.restrictions).length); this.showRestrictModal=false; toast.success('Restrictions applied'); this.fetchOverviewAndEnrich(); } catch(e){ toast.error(String(e.message||e)); } finally { this.savingRestriction=false; } },
    async clearRestrictions(){ if(!this.selectedMerchant) return; if(!confirm('Clear all restrictions for this merchant?')) return; this.savingRestriction=true; try { const body={ restrictions:{} }; const resp=await fetch(`${this.apiBase}/v1/merchants/${encodeURIComponent(this.selectedMerchant.id)}`, { method:'PATCH', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(body) }); if(!resp.ok) throw new Error('Failed to clear'); this.selectedMerchant.restrictions={}; this.selectedMerchant.has_restrictions=false; toast.success('Restrictions cleared'); this.fetchOverviewAndEnrich(); this.showRestrictModal=false; } catch(e){ toast.error(String(e.message||e)); } finally { this.savingRestriction=false; } }
    ,async deleteMerchantFromDashboard(id){
      if(!id) return; if(!confirm(`Delete merchant '${id}' from dashboard? This removes risk & evaluation data.`)) return;
      try {
        const resp = await fetch(`${this.apiBase}/v1/merchants/${encodeURIComponent(id)}`, { method:'DELETE' });
        if(!resp.ok){ if(resp.status===404){ toast.warning('Merchant already removed'); } else { throw new Error('Delete failed '+resp.status); } }
        else { toast.success(`Merchant '${id}' removed`); }
      } catch(e){ toast.error(String(e.message||e)); }
      finally { if(this.selectedMerchant && this.selectedMerchant.id===id) this.selectedMerchant=null; this.fetchOverviewAndEnrich(); }
    }
  },
  mounted(){ this.fetchOverviewAndEnrich(); this._escHandler=(e)=>{ if(e.key==='Escape' && this.selectedMerchant) this.exitFocus(); }; window.addEventListener('keydown', this._escHandler); },
  beforeUnmount(){ if(this._escHandler) window.removeEventListener('keydown', this._escHandler); },
  watch:{ selectedMerchant(n,o){ if(n && (!o || n.id!==o.id)) this.refreshSelectedDetails(); } }
};
</script>

<style>
/* Teal, white and grey theme */
:root {
  --teal-50: #e6fbf8;
  --teal-100: #bfeeea;
  --teal-200: #88e2d9;
  --teal-300: #5fd8c8;
  --teal-500: #14b8a6;
  --teal-600: #0f9a92;
  --teal-700: #0b7f7b;
  --grey-50: #fafafa;
  --grey-100: #f4f5f7;
  --grey-200: #e5e7eb;
  --grey-500: #6b7280;
  --grey-600: #9ca3af;
  --grey-700: #374151;
  --bg: #ffffff;
}

.merchant-dashboard.teal-theme {
  background: #f5f5f5;
  color: #374151;
  min-height: 100vh;
}

/* Layout */
.merchant-dashboard {
  display: grid;
  grid-template-columns: 350px 1fr;
  gap: 20px;
  padding: 20px;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  min-height: 100vh;
  box-sizing: border-box;
}

/* Details Panel */
.details-panel {
  display: block;
  padding: 0;
}

.merchant-layout { display: grid; grid-template-columns: 320px 1fr; gap: 20px; align-items: start; }
.merchant-layout .left-col { display: flex; flex-direction: column; gap: 20px; }
.merchant-layout .right-col { min-width: 0; }

/* Focus toolbar */
.focus-toolbar { display:flex; justify-content:space-between; align-items:center; padding:10px 14px 12px; background:#ffffff; border:1px solid #e5e7eb; border-radius:10px; margin-bottom:18px; box-shadow:0 2px 6px rgba(0,0,0,0.06); gap:12px; flex-wrap:wrap; }
.focus-title { margin:0; font-size:16px; font-weight:600; color:#0f766e; }
.focus-exit-btn { background:#f0fdfa; color:#065f5b; border:1px solid #14b8a6; padding:8px 14px; border-radius:8px; font-weight:600; font-size:13px; cursor:pointer; display:inline-flex; align-items:center; gap:6px; transition:all .15s; }
.focus-exit-btn:hover { background:#14b8a6; color:#ffffff; }
.focus-exit-btn:active { transform:translateY(1px); }

/* Restriction Modal Styles */
.modal-overlay { position:fixed; inset:0; background:rgba(0,0,0,0.55); display:flex; justify-content:center; align-items:center; z-index:2000; }
.modal-content { background:#ffffff; border-radius:14px; box-shadow:0 20px 60px rgba(0,0,0,0.35); width:92%; max-width:880px; max-height:90vh; display:flex; flex-direction:column; overflow:hidden; }
.modal-header { display:flex; align-items:center; justify-content:space-between; padding:18px 22px; background:linear-gradient(90deg,#f0fdfa,#ecfeff); border-bottom:1px solid #d1d5db; }
.modal-header h3 { margin:0; font-size:18px; font-weight:600; color:#065f5b; }
.close-btn { background:none; border:none; font-size:24px; line-height:1; cursor:pointer; color:#6b7280; border-radius:50%; width:34px; height:34px; display:flex; align-items:center; justify-content:center; }
.close-btn:hover { background:#f3f4f6; color:#374151; }
.modal-body { padding:22px 24px; overflow-y:auto; }
.form-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:20px; }
.form-section h4 { margin:0 0 10px; font-size:14px; font-weight:700; letter-spacing:.5px; color:#0f766e; text-transform:uppercase; }
.form-field { display:grid; gap:4px; font-size:13px; margin-bottom:10px; }
.form-field span { font-weight:600; color:#374151; font-size:12px; letter-spacing:.5px; text-transform:uppercase; }
.form-input, .form-textarea { width:100%; padding:8px 10px; border:1px solid #d1d5db; border-radius:6px; font-size:13px; box-sizing:border-box; }
.form-input:focus, .form-textarea:focus { outline:2px solid #0d9488; outline-offset:1px; }
.form-textarea { resize:vertical; }
.check-field { grid-auto-flow:column; grid-template-columns:auto 1fr; align-items:center; gap:8px; }
.flex-row { display:flex; gap:8px; }
.mini-btn { background:#0d9488; color:#fff; border:none; padding:6px 12px; border-radius:6px; font-size:12px; cursor:pointer; font-weight:600; }
.mini-btn:hover { background:#0b7f7b; }
.code-tags { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
.code-tag { background:#f0fdfa; border:1px solid #14b8a6; padding:4px 8px; border-radius:16px; font-size:12px; display:inline-flex; align-items:center; gap:4px; }
.code-tag button { background:none; border:none; cursor:pointer; font-size:14px; line-height:1; color:#0f766e; }
.code-tag button:hover { color:#ef4444; }
.modal-footer { padding:14px 20px; border-top:1px solid #e5e7eb; background:#fafafa; display:flex; justify-content:flex-end; gap:10px; }
.btn-cancel { background:#ffffff; border:1px solid #d1d5db; padding:10px 18px; border-radius:8px; cursor:pointer; font-weight:600; font-size:13px; }
.btn-cancel:hover { background:#f3f4f6; }
.btn-secondary { background:#f59e0b; color:#ffffff; border:none; padding:10px 18px; border-radius:8px; cursor:pointer; font-weight:600; font-size:13px; }
.btn-secondary:hover { background:#d97706; }
.btn-primary { background:#008080; color:#ffffff; border:none; padding:10px 18px; border-radius:8px; cursor:pointer; font-weight:600; font-size:13px; }
.btn-primary:hover { background:#006666; }
.btn-primary:disabled { opacity:.55; cursor:not-allowed; }

.empty-state {
  grid-template-columns: 1fr;
  place-items: center;
}

.empty-message {
  text-align: center;
  color: #6b7280;
  padding: 60px 40px;
  background: white;
  border-radius: 12px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
}

.empty-message h3 {
  color: #008080;
  margin-bottom: 10px;
  font-size: 18px;
}

/* Responsive Design */
@media (max-width: 1024px) {
  .merchant-dashboard {
    grid-template-columns: 1fr;
    gap: 15px;
  }

  .merchant-layout { grid-template-columns: 1fr; }
}

/* Remove duplicate styles if they are already in component styles */
/* If styles are truly global for the dashboard, they should remain here */
/* e.g. the main grid layout, the theme variables */
/* The component-specific styles have been moved to their respective files */
</style>