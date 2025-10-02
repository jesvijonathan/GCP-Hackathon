<template>
  <div class="card action-center-card" v-if="merchant">
    <h1 class="action-card-heading">
      Action Center for {{ merchant.name }}
    </h1>
    <div v-if="restrictionActive" class="restriction-summary">
      <div class="rs-head">Restriction Active</div>
      <div class="rs-grid">
        <div class="rs-row" v-if="restrictionDisplay.reason">
          <div class="rs-label">Reason</div>
          <div class="rs-val">{{ restrictionDisplay.reason }}</div>
        </div>
        <div class="rs-row" v-if="restrictionDisplay.startDate">
          <div class="rs-label">Start</div>
          <div class="rs-val">{{ restrictionDisplay.startDate }}</div>
        </div>
        <div class="rs-row" v-if="restrictionDisplay.endDate">
          <div class="rs-label">End</div>
          <div class="rs-val">{{ restrictionDisplay.endDate }}</div>
        </div>
        <div class="rs-row" v-if="restrictionDisplay.dailyTransactionLimit">
          <div class="rs-label">Daily Limit</div>
          <div class="rs-val">{{ restrictionDisplay.dailyTransactionLimit }}</div>
        </div>
        <div class="rs-row" v-if="restrictionDisplay.monthlyTransactionLimit">
          <div class="rs-label">Monthly Limit</div>
          <div class="rs-val">{{ restrictionDisplay.monthlyTransactionLimit }}</div>
        </div>
        <div class="rs-row" v-if="restrictionDisplay.maxTransactionAmount">
          <div class="rs-label">Max Tx</div>
          <div class="rs-val">{{ restrictionDisplay.maxTransactionAmount }}</div>
        </div>
      </div>
    </div>
    <button @click="$emit('action', 'permanent-ban')" class="action-card-button">
      Permanent Ban
    </button>
    <button @click="$emit('action', 'shadow-ban')" class="action-card-button">
      Shadow Ban
    </button>
    <button @click="$emit('action', 'continue')" class="action-card-button">
      Continue Merchant
    </button>
    <button @click="emitManageRestrictions" class="action-card-button secondary" style="background:#f59e0b;">Manage Restrictions</button>
  </div>
</template>

<script>
export default {
  name: "DashboardActionCenter",
  props: {
    merchant: {
      type: Object,
      required: true,
    },
  },
  emits: ['action','manage-restrictions'],
  computed: {
    restrictionObj(){
      return this.merchant?.evaluation?.restrictions || this.merchant?.restrictions || null;
    },
    restrictionActive(){ return !!(this.merchant?.evaluation?.has_restrictions || (this.restrictionObj && Object.keys(this.restrictionObj).length)); },
    restrictionDisplay(){
      const r = this.restrictionObj || {};
      const keys = ['reason','startDate','endDate','dailyTransactionLimit','monthlyTransactionLimit','maxTransactionAmount'];
      const out = {};
      keys.forEach(k=>{ if(r[k] != null && r[k] !== '') out[k]=r[k]; });
      if(!Object.keys(out).length && this.restrictionActive) out['status'] = 'Enabled';
      return out;
    }
  },
  methods:{
    emitManageRestrictions(){ this.$emit('manage-restrictions'); }
  }
};
</script>

<style scoped>
/* Styles copied from original Dashboard.vue */
.card {
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  background: #fff;
  padding: 20px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
  transition: all 0.3s ease;
  height: fit-content;
}

.card:hover {
  transform: translateY(-2px);
  box-shadow: 0 8px 25px rgba(0, 0, 0, 0.12);
}

.action-center-card {
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
  padding: 25px;
}

.action-card-button {
  background: #008080;
  color: white;
  border: none;
  border-radius: 8px;
  padding: 12px 20px;
  margin: 6px;
  width: 200px;
  cursor: pointer;
  font-weight: 500;
  font-size: 14px;
  transition: all 0.2s ease;
}

.action-card-button:hover {
  background: #006666;
  transform: translateY(-1px);
  box-shadow: 0 4px 8px rgba(0, 128, 128, 0.3);
}
.action-card-button.secondary:hover { background:#d97706; }

.restriction-summary { width:100%; background:#fff7ed; border:1px solid #fed7aa; padding:12px 14px; border-radius:8px; margin-bottom:10px; }
.rs-head { font-size:12px; font-weight:700; color:#b45309; margin-bottom:6px; letter-spacing:.5px; text-transform:uppercase; }
.rs-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:6px 14px; }
.rs-row { display:flex; flex-direction:column; background:#fffbeb; border:1px solid #fde68a; padding:6px 8px; border-radius:6px; }
.rs-label { font-size:10px; font-weight:600; letter-spacing:.5px; text-transform:uppercase; color:#92400e; }
.rs-val { font-size:12px; font-weight:600; color:#7c2d12; word-break:break-word; }

.action-card-heading {
  color: #008080;
  margin-bottom: 20px;
  font-size: 16px;
  text-align: center;
  font-weight: 600;
}

/* Responsive styles */
@media (max-width: 480px) {
  .action-card-button {
    width: 160px;
  }
}
</style>