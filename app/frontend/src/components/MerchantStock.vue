<template>
  <section class="st-container">
    <header class="st-header">
      <div class="st-title">
        <h3>Stock Analytics</h3>
        <span class="st-sub" v-if="dateRange">{{ dateRange }}</span>
      </div>
      <div class="st-kpis">
        <div class="kpi">
          <div class="kpi-label">Latest Close</div>
          <div class="kpi-value">{{ latestClose != null ? formatNumber(latestClose) : '-' }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Change</div>
          <div class="kpi-value" :class="pctChangeClass">
            <template v-if="pctChange!=null">{{ pctChange>0? '+' : ''}}{{ pctChange.toFixed(2) }}%</template>
            <template v-else>-</template>
          </div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Days</div>
          <div class="kpi-value">{{ prices.length }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Volatility (σ)</div>
          <div class="kpi-value">{{ volatility != null ? volatility.toFixed(2) : '-' }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Earnings Events</div>
          <div class="kpi-value">{{ earnings.length }}</div>
        </div>
      </div>
    </header>

    <div v-if="loading" class="st-refresh">Refreshing… (previous data shown)</div>
    <div v-else-if="!prices.length" class="st-empty">No stock price data in range.</div>

    <section class="st-charts">
      <div class="chart-card">
        <div class="chart-title">Price (Close)</div>
        <div v-if="chartJsLoaded" class="chart-wrap" style="height:260px;">
          <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
          <canvas ref="priceCanvas" height="140"></canvas>
        </div>
        <div v-else class="chart-fallback">Chart.js unavailable. Latest close: {{ latestClose }}</div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Returns Distribution (Daily %)</div>
        <div v-if="returns.length" class="dist-wrap">
          <div class="dist-bar" v-for="b in returnBuckets" :key="b.label">
            <div class="bar" :style="{height: b.pct+'%', background: bucketColor(b)}" :title="b.label+' ('+b.count+')'"></div>
            <div class="bar-label">{{ b.short }}</div>
          </div>
        </div>
        <div v-else class="chart-fallback">Need at least 2 sequential price points to compute daily returns (received {{ prices.length }}).</div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Meta</div>
        <div class="meta-grid">
          <div v-for="(v,k) in trimmedMeta" :key="k" class="meta-row">
            <span class="meta-key">{{ k }}</span>
            <span class="meta-val">{{ formatMeta(v) }}</span>
          </div>
          <div v-if="!Object.keys(trimmedMeta).length" style="font-size:12px;color:#6b7280;">
            No meta returned. (If backend supports meta, ensure include_stock_meta=true and merchant has stock meta.)
          </div>
        </div>
      </div>
    </section>

  </section>
</template>

<script>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue';
export default {
  name: 'MerchantStock',
  props: {
    prices: { type: Array, default: () => [] }, // [{date, close, ...}]
    earnings: { type: Array, default: () => [] },
    actions: { type: Array, default: () => [] },
    meta: { type: Object, default: () => ({}) },
    loading: { type: Boolean, default: false },
  },
  setup(props){
    let Chart = null;
    const chartJsLoaded = ref(false);
    const chartsBuilding = ref(true);
    const priceCanvas = ref(null);
    let priceChart = null;
    let resizeObs = null;

    const sortedPrices = computed(()=> {
      const arr = (props.prices||[]) .filter(p=>p && p.date && (p.close!=null))
        .map(p=>({ ...p, _dt: new Date(p.date) }));
      return arr.sort((a,b)=> a._dt - b._dt);
    });
    const prices = computed(()=> sortedPrices.value.map(p=>p.close));
    const labels = computed(()=> sortedPrices.value.map(p=> new Date(p._dt).toLocaleDateString()));
    const latestClose = computed(()=> prices.value.length ? prices.value[prices.value.length-1] : null);
    const prevClose = computed(()=> prices.value.length>1 ? prices.value[prices.value.length-2] : null);
    const pctChange = computed(()=> (latestClose.value!=null && prevClose.value!=null) ? ((latestClose.value - prevClose.value)/prevClose.value*100) : null);
    const pctChangeClass = computed(()=> pctChange.value==null ? '' : (pctChange.value>0 ? 'up' : (pctChange.value<0 ? 'down' : 'flat')));

    const returns = computed(()=> {
      const arr = [];
      for (let i=1;i<prices.value.length;i++){
        const p0 = prices.value[i-1];
        const p1 = prices.value[i];
        if (p0!=null && p1!=null && p0!==0){ arr.push((p1-p0)/p0*100); }
      }
      return arr;
    });
    const volatility = computed(()=> {
      if (!returns.value.length) return null;
      const mean = returns.value.reduce((a,b)=>a+b,0)/returns.value.length;
      const variance = returns.value.reduce((a,b)=> a + (b-mean)**2, 0)/(returns.value.length);
      return Math.sqrt(variance);
    });

    const returnBuckets = computed(()=> {
      if (!returns.value.length) return [];
      // bucket edges
      const buckets = [
        {min:-100, max:-5},
        {min:-5, max:-2},
        {min:-2, max:-1},
        {min:-1, max:0},
        {min:0, max:1},
        {min:1, max:2},
        {min:2, max:5},
        {min:5, max:100}
      ];
      const out = buckets.map(b=> ({...b, count:0}));
      returns.value.forEach(r=> {
        const b = out.find(x=> r>=x.min && r < x.max) || out[out.length-1];
        b.count++;
      });
      const total = returns.value.length || 1;
      return out.map(b=> ({
        ...b,
        pct: (b.count/total*100),
        label: `${b.min}% to ${b.max}%`,
        short: `${b.min}–${b.max}`
      }));
    });

    function bucketColor(b){
      if (b.max<=0) return '#ef4444';
      if (b.min>=0) return '#10b981';
      return '#9ca3af';
    }

    const dateRange = computed(()=> {
      if (!sortedPrices.value.length) return '';
      const first = sortedPrices.value[0]._dt.toLocaleDateString();
      const last = sortedPrices.value[sortedPrices.value.length-1]._dt.toLocaleDateString();
      return `${first} – ${last}`;
    });

    const trimmedMeta = computed(()=> {
      const m = props.meta || {};
      const out = {};
      const keys = Object.keys(m).filter(k=> typeof m[k] !== 'object');
      keys.slice(0,12).forEach(k=> out[k]=m[k]);
      return out;
    });

    function formatMeta(v){
      if (v==null) return '-';
      if (typeof v === 'number'){ return v.toLocaleString(); }
      if (typeof v === 'string'){ return v.slice(0,40); }
      return JSON.stringify(v).slice(0,40);
    }

    function formatNumber(n){
      const x = Number(n||0);
      if (x >= 1e9) return (x/1e9).toFixed(2)+'B';
      if (x >= 1e6) return (x/1e6).toFixed(2)+'M';
      if (x >= 1e3) return (x/1e3).toFixed(1)+'k';
      return x.toFixed(2);
    }

    async function ensureChartJs(){
      if (Chart) return;
      try {
        const m = await import('chart.js/auto');
        Chart = m.default || m;
        chartJsLoaded.value = true;
      } catch(e){
        chartJsLoaded.value = false;
        console.warn('Chart.js missing for stock charts', e);
      }
    }

    function buildOrUpdate(){
      if (!chartJsLoaded.value) return;
      if (!priceChart && priceCanvas.value){
        priceChart = new Chart(priceCanvas.value.getContext('2d'), {
          type:'line',
            data:{ labels: labels.value, datasets:[{ label:'Close', data: prices.value, borderColor:'#0f766e', backgroundColor:'rgba(15,118,110,0.15)', tension:0.25, pointRadius:0 }]},
          options:{ responsive:true, maintainAspectRatio:false, plugins:{ legend:{display:false}}, scales:{ y:{ beginAtZero:false } } }
        });
      } else if (priceChart){
        priceChart.options.animation = false;
        priceChart.data.labels = labels.value;
        priceChart.data.datasets[0].data = prices.value;
        priceChart.update();
      }
      requestAnimationFrame(()=> { chartsBuilding.value = false; });
    }

    watch(()=> props.prices, async () => {
      if (!Chart) await ensureChartJs();
      await nextTick();
      if (!props.prices.length){
        if (priceChart){ priceChart.options.animation=false; priceChart.data.labels=[]; priceChart.data.datasets[0].data=[]; priceChart.update(); }
        chartsBuilding.value = false;
        return;
      }
      chartsBuilding.value = true;
      buildOrUpdate();
    }, { deep:true });

    onMounted(async () => {
      await ensureChartJs();
      await nextTick();
      buildOrUpdate();
      try {
        resizeObs = new ResizeObserver(()=> { if (priceChart) priceChart.resize(); });
        const host = priceCanvas.value?.parentElement?.parentElement;
        if (host) resizeObs.observe(host);
      } catch {}
    });
    onBeforeUnmount(()=> { if (resizeObs) try { resizeObs.disconnect(); } catch{} if (priceChart) try { priceChart.destroy(); } catch{} });

    return { chartJsLoaded, chartsBuilding, priceCanvas, latestClose, pctChange, pctChangeClass, prices: sortedPrices, returns, returnBuckets, bucketColor, volatility, dateRange, trimmedMeta, formatMeta, formatNumber };
  }
};
</script>

<style scoped>
.st-container { display:grid; gap:16px; }
.st-header { display:grid; gap:10px; }
.st-title { display:flex; align-items:baseline; gap:10px; }
.st-title h3 { margin:0; color:#0f766e; font-size:16px; font-weight:700; }
.st-sub { color:#6b7280; font-size:12px; }
.st-kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:8px; }
.kpi { background:#f9fafb; border:1px solid #e5e7eb; border-radius:10px; padding:10px; display:grid; gap:6px; }
.kpi-label { color:#6b7280; font-size:12px; }
.kpi-value { color:#111827; font-weight:700; font-size:18px; }
.kpi-value.up { color:#059669; }
.kpi-value.down { color:#dc2626; }
.kpi-value.flat { color:#374151; }
.st-refresh { padding:6px 10px; font-size:12px; color:#0f766e; font-weight:600; }
.st-empty { padding:14px; background:#f8fafc; border:1px dashed #d1d5db; border-radius:8px; color:#374151; }
.st-charts { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; }
.chart-card { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:12px; display:grid; gap:10px; }
.chart-title { color:#0f766e; font-size:14px; font-weight:700; }
.chart-wrap { position:relative; height:260px; }
.chart-loader { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; background:linear-gradient(135deg,#f8fafc,#eef2f7); font-size:13px; color:#0f766e; font-weight:600; z-index:2; }
.chart-fallback { color:#6b7280; font-size:13px; }
.dist-wrap { display:flex; align-items:flex-end; gap:6px; height:200px; }
.dist-bar { flex:1; display:flex; flex-direction:column; align-items:center; justify-content:flex-end; gap:4px; }
.dist-bar .bar { width:100%; border-radius:3px 3px 0 0; transition:height .3s; }
.bar-label { font-size:10px; color:#475569; text-align:center; }
.meta-grid { display:grid; gap:6px; font-size:12px; }
.meta-row { display:flex; justify-content:space-between; gap:8px; }
.meta-key { color:#6b7280; font-weight:600; }
.meta-val { color:#111827; font-weight:600; }
</style>
