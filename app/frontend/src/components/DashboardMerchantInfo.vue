<template>
  <div class="card merchant-info-card" v-if="merchant">
    <div class="card-header header-flex">
      <h3 style="display:flex;align-items:center;gap:10px;">
        Merchant Overview
        <span v-if="status" :class="['inline-status', 'is-'+statusClass]" :title="'Status: '+statusLabel">{{ statusLabel }}</span>
      </h3>
      <div class="badges">
        <span v-if="country" class="badge neutral" :title="'Country'">{{ country }}</span>
        <span v-if="category" class="badge neutral" :title="'Category'">{{ category }}</span>
      </div>
    </div>
    <div class="card-content grid-info">
      <div class="info-block wide">
        <div class="label">Merchant</div>
        <div class="value mono">{{ merchant.name || merchant.id }}</div>
      </div>
      <div class="info-block">
        <div class="label">ID</div>
        <div class="value mono">{{ merchant.id }}</div>
      </div>
      <div class="info-block" v-if="activationFlag !== null">
        <div class="label">Activation</div>
        <div class="value" :style="activationFlag? 'color:#047857;font-weight:600;' : 'color:#b91c1c;font-weight:600;'">{{ activationFlag ? 'ENABLED':'DISABLED' }}</div>
      </div>
      <div class="info-block" v-if="autoAction !== null">
        <div class="label">Auto Action</div>
        <div class="value" :style="autoAction? 'color:#0369a1;font-weight:600;' : 'color:#6b7280;'">{{ autoAction ? 'ON':'OFF' }}</div>
      </div>
      <div class="info-block" v-if="hasRestrictions">
        <div class="label">Restrictions</div>
        <div class="value" style="color:#b45309;font-weight:600;">ACTIVE</div>
      </div>
      <div class="info-block" v-if="country">
        <div class="label">Country</div>
        <div class="value">{{ country }}</div>
      </div>
      <div class="info-block" v-if="category">
        <div class="label">Category</div>
        <div class="value">{{ category }}</div>
      </div>
      <div class="info-block" v-if="riskScore !== null">
        <div class="label">Risk Score</div>
        <div class="value risk" :class="riskClass(riskScore)">{{ fmt(riskScore) }}</div>
      </div>
      <div class="info-block" v-if="confidence !== null">
        <div class="label">Confidence</div>
        <div class="value">{{ (confidence*100).toFixed(0) }}%</div>
      </div>
      <div class="info-block" v-if="lastEvaluated">
        <div class="label">Last Eval</div>
        <div class="value" :title="lastEvaluated">{{ timeAgo(lastEvaluated) }}</div>
      </div>
      <div class="info-block" v-if="createdAt">
        <div class="label">Created</div>
        <div class="value" :title="createdAt">{{ shortDate(createdAt) }}</div>
      </div>
      <div class="info-block" v-if="updatedAt">
        <div class="label">Updated</div>
        <div class="value" :title="updatedAt">{{ timeAgo(updatedAt) }}</div>
      </div>
      <div class="info-block wide" v-if="drivers && drivers.length">
        <div class="label">Top Drivers</div>
        <div class="drivers">
          <span v-for="d in drivers" :key="d" class="driver-pill">{{ driverLabel(d) }}</span>
        </div>
      </div>
    </div>
    <div v-if="countsAvailable" class="counts-row">
      <div class="count-box" v-for="c in countList" :key="c.key">
        <div class="cb-label">{{ c.label }}</div>
        <div class="cb-val">{{ c.val }}</div>
      </div>
    </div>
    <div v-if="!evaluation && !riskScore" class="empty-hint">No evaluation data yet – trigger a risk evaluation to populate metrics.</div>
  </div>
</template>

<script>
export default {
  name: "DashboardMerchantInfo",
  props: {
    merchant: {
      type: Object,
      required: true,
    },
  },
  computed: {
    evaluation() {
      return this.merchant?.evaluation || null;
    },
    riskScore() {
      return this.merchant?.riskMetrics?.riskScore ?? (this.evaluation?.risk_score ?? null);
    },
    confidence() {
      return this.evaluation?.confidence ?? null;
    },
    status() {
      // If activation_flag explicitly false, force Inactive label regardless of legacy status field
      if (this.merchant && this.merchant.activation_flag === false) return 'Inactive';
      if (this.evaluation && this.evaluation.activation_flag === false) return 'Inactive';
      return this.merchant?.status || this.evaluation?.details?.status || null;
    },
    statusLabel() {
      if(!this.status) return 'UNKNOWN';
      // Normalize capitalization (Active -> ACTIVE, Inactive -> INACTIVE)
      return this.status.toString().toUpperCase();
    },
    statusClass() {
      const s = (this.status||'').toLowerCase();
      if(['active','ok','live'].includes(s)) return 'active';
      if(['inactive'].includes(s)) return 'inactive';
      if(['suspended','warn','warning'].includes(s)) return 'warn';
      if(['banned','blocked','disabled'].includes(s)) return 'blocked';
      return 'neutral';
    },
    country() { return this.evaluation?.details?.country || null; },
    category() { return this.evaluation?.details?.category || null; },
  activationFlag(){ return this.merchant?.activation_flag ?? null; },
  autoAction(){ return this.merchant?.auto_action ?? null; },
  hasRestrictions(){ return !!(this.merchant?.evaluation?.has_restrictions || this.merchant?.evaluation?.restrictions || this.merchant?.has_restrictions || this.merchant?.restrictions); },
    createdAt() { return this.evaluation?.details?.created_at || null; },
    updatedAt() { return this.evaluation?.details?.updated_at || null; },
    lastEvaluated() { return this.evaluation?.window_end || null; },
    drivers() { return this.evaluation?.drivers || []; },
    counts() { return this.evaluation?.counts || {}; },
    countsAvailable() { return Object.keys(this.counts||{}).length > 0; },
    countList() {
      const order = [
        ['tweets','Twt'],['reddit','Red'],['news','News'],['reviews','Rev'],['wl','WL Tx'],['stock_prices','Px']
      ];
      return order.map(([k,label])=> ({ key:k, label, val: this.counts[k] ?? 0 }));
    }
  },
  methods: {
    fmt(v){ return v==null? '—' : Number(v).toFixed(0); },
    riskClass(v){
      if(v==null) return 'rk-neutral';
      if(v>=80) return 'rk-high';
      if(v>=50) return 'rk-med';
      return 'rk-low';
    },
    driverLabel(key){
      const map = {
        tweet_sentiment:'Tweet Sentiment',
        reddit_sentiment:'Reddit Sentiment',
        news_sentiment:'News Sentiment',
        reviews_rating:'Reviews',
        wl_flag_ratio:'WL Flags',
        stock_volatility:'Volatility'
      };
      return map[key] || key;
    },
    timeAgo(iso){
      if(!iso) return '—';
      try {
        const d = new Date(iso); const diff = (Date.now()-d.getTime())/1000;
        if(diff<60) return Math.floor(diff)+'s ago';
        if(diff<3600) return Math.floor(diff/60)+'m ago';
        if(diff<86400) return Math.floor(diff/3600)+'h ago';
        return Math.floor(diff/86400)+'d ago';
      } catch { return iso; }
    },
    shortDate(iso){
      try { const d=new Date(iso); return d.toLocaleDateString(); } catch { return iso; }
    }
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

.card-header h3 {
  margin: 0 0 15px 0;
  font-size: 16px;
  color: #008080;
  font-weight: 600;
  border-bottom: 2px solid #e6fbf8;
  padding-bottom: 8px;
}


.card-content { font-size: 13px; color:#374151; display:flex; flex-wrap:wrap; gap:14px; }
.header-flex { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; flex-wrap:wrap; }
.badges { display:flex; gap:6px; flex-wrap:wrap; }
.badge { padding:4px 8px; font-size:11px; font-weight:600; border-radius:14px; background:#f1f5f9; color:#036d69; letter-spacing:.3px; text-transform:uppercase; }
.badge.st-active { background:#def7ec; color:#047857; }
.badge.st-warn { background:#fef3c7; color:#b45309; }
.badge.st-blocked { background:#fee2e2; color:#b91c1c; }
.badge.st-inactive { background:#e2e8f0; color:#475569; }
.badge.neutral { background:#e0f2f1; color:#0f766e; }

/* Inline status near title */
.inline-status { font-size:11px; font-weight:600; padding:4px 8px; border-radius:12px; background:#e0f2f1; color:#036d69; text-transform:uppercase; letter-spacing:.5px; }
.inline-status.is-active { background:#def7ec; color:#047857; }
.inline-status.is-inactive { background:#fee2e2; color:#b91c1c; }
.inline-status.is-warn { background:#fef3c7; color:#b45309; }
.inline-status.is-blocked { background:#fee2e2; color:#991b1b; }

.grid-info { display:grid; grid-template-columns: repeat(auto-fill, minmax(130px,1fr)); width:100%; }
.info-block { display:flex; flex-direction:column; gap:3px; min-width:110px; }
.info-block.wide { grid-column: span 2; }
.label { font-size:10px; text-transform:uppercase; letter-spacing:.5px; font-weight:600; color:#6b7280; }
.value { font-size:14px; font-weight:500; word-break:break-word; }
.value.mono { font-family: "SFMono-Regular", ui-monospace, Menlo, monospace; font-size:13px; }
.value.risk { font-weight:600; }
.rk-high { color:#dc2626; }
.rk-med { color:#d97706; }
.rk-low { color:#059669; }
.rk-neutral { color:#6b7280; }

.drivers { display:flex; flex-wrap:wrap; gap:6px; }
.driver-pill { background:#f0fdfa; border:1px solid #14b8a6; color:#065f5b; padding:4px 8px; font-size:11px; border-radius:12px; font-weight:500; }

.counts-row { display:flex; flex-wrap:wrap; gap:10px; margin-top:14px; }
.count-box { background:#f8fafc; border:1px solid #e5e7eb; border-radius:8px; padding:8px 10px; min-width:70px; display:flex; flex-direction:column; gap:2px; }
.cb-label { font-size:10px; letter-spacing:.5px; text-transform:uppercase; color:#6b7280; font-weight:600; }
.cb-val { font-size:16px; font-weight:600; color:#008080; }

.empty-hint { margin-top:12px; font-size:12px; color:#6b7280; font-style:italic; }

/* Specific styles for merchant-info-card if any, otherwise inherited */
/* .merchant-info-card specific overrides can be added here if needed */
</style>