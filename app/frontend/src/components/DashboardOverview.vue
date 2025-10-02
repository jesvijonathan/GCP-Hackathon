<template>
  <div class="card dashboard-overview-card">
    <div class="top-bar">
      <div class="title-block">
        <h3>Dashboard Analytics</h3>
        <div class="subline">Live merchant risk & multi-metric overview</div>
      </div>
      <div class="controls">
        <div class="interval-select">
          <label>Interval:</label>
          <select v-model="interval" @change="reloadAll">
            <option value="30m">30m</option>
            <option value="1h">1h</option>
            <option value="1d">1d</option>
          </select>
        </div>
        <div class="interval-select">
          <label>Series Windows:</label>
          <select v-model.number="lookback" @change="fetchSeries">
            <option :value="24">24</option>
            <option :value="48">48</option>
            <option :value="96">96</option>
          </select>
        </div>
        <button class="btn" @click="reloadAll" :disabled="loading || seriesLoading">Reload</button>
      </div>
    </div>

    <div class="tabs">
      <div
        v-for="t in tabs"
        :key="t.key"
        :class="['tab', { active: activeTab===t.key }]"
        @click="activeTab = t.key; buildChart()"
      >{{ t.label }}</div>
    </div>

    <div v-if="loading" class="loading">Loading...</div>
    <div v-else-if="error" class="error">{{ error }}</div>

    <table v-else class="overview-table">
      <thead>
        <tr>
          <th>#</th>
          <th>Merchant</th>
          <th>Risk</th>
          <th>Auto</th>
          <th>WL</th>
          <th>Market</th>
          <th>Sentiment</th>
          <th>Vol</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(m, idx) in topMerchants" :key="m.merchant" @click="rowClick(m.merchant)" class="row-click">
          <td>{{ idx + 1 }}</td>
          <td class="name-cell">
            <span class="merchant-name" style="display:flex;align-items:center;gap:6px;">
              <span v-if="m.activation_flag === false" class="inactive-dot" title="Inactive"></span>
              {{ m.merchant }}
            </span>
            <span v-if="m.confidence != null" class="confidence" :title="'Confidence ' + m.confidence">{{ (m.confidence * 100).toFixed(0) }}%</span>
          </td>
          <td>
            <template v-if="jobs[m.merchant+':'+interval] && ['queued','running'].includes(jobs[m.merchant+':'+interval].status)">
              <div class="progress-wrap">
                <div class="progress-label">
                  {{ jobs[m.merchant+':'+interval].status === 'queued' ? 'Queued' : 'Processing' }}
                  <span v-if="jobs[m.merchant+':'+interval].percent != null"> {{ jobs[m.merchant+':'+interval].percent }}%</span>
                </div>
                <div class="progress-bar"><span :style="{width: (jobs[m.merchant+':'+interval].percent||0)+'%'}"></span></div>
              </div>
            </template>
            <template v-else>
              <span :class="riskClass(m.risk_score)" class="score-pill">{{ fmt(m.risk_score) }}</span>
            </template>
          </td>
          <td><span :class="['auto-pill', m.auto_action ? 'on':'off']">{{ m.auto_action ? 'On':'Off' }}</span></td>
          <td>{{ fmt(m.scores?.wl) }}</td>
          <td>{{ fmt(m.scores?.market) }}</td>
          <td>{{ fmt(m.scores?.sentiment) }}</td>
          <td>{{ fmt(m.scores?.volume) }}</td>
          <td>{{ timeAgo(m.window_end) }}</td>
        </tr>
      </tbody>
    </table>

    <div class="big-chart-wrapper" v-if="series.length || isProcessing">
      <div class="chart-header">
        <h4>{{ currentMetricLabel }}</h4>
        <div v-if="isProcessing" class="processing-indicator">
          <div class="proc-label">Processing risk windows… <span v-if="overallPercent != null">{{ overallPercent.toFixed(1) }}%</span></div>
          <div class="progress-bar mini"><span :style="{width: (overallPercent||0)+'%'}"></span></div>
        </div>
        <div class="legend" v-if="series.length">
          <span v-for="s in series" :key="s.merchant" class="legend-item">
            <span class="dot" :style="{ background: colorFor(s.merchant) }"></span>{{ s.merchant }}
          </span>
        </div>
      </div>
      <canvas ref="seriesCanvas" height="260"></canvas>
      <div v-if="seriesLoading && !isProcessing" class="overlay-loading">Loading series…</div>
      <div v-if="isProcessing && !series.length" class="overlay-progress">
        <div class="op-inner">
          <div style="font-weight:600;margin-bottom:6px;color:#065f5b;">Generating evaluation windows</div>
          <div class="progress-bar large"><span :style="{width: (overallPercent||0)+'%'}"></span></div>
          <div v-if="overallCounts.planned" style="font-size:12px;margin-top:4px;color:#036d69;font-weight:500;">
            {{ overallCounts.processed }}/{{ overallCounts.planned }} windows
          </div>
          <div class="op-meta" v-if="activeJobs.length">
            <div v-for="j in activeJobs" :key="j.job_id" class="op-job">
              <strong>{{ j.merchant }}</strong>
              <span>{{ (j.percent||0).toFixed(1) }}% ({{ j.processed||0 }}/{{ j.planned||j.missing_planned||0 }})</span>
            </div>
          </div>
          <div style="font-size:11px;color:#0f766e;margin-top:6px;">Chart will auto-populate as windows finish…</div>
        </div>
      </div>
    </div>
    <div v-else-if="!seriesLoading && !loading" class="empty-series">No evaluation windows yet (auto-ensure attempted). Trigger risk evaluation or seed data.</div>
  </div>
</template>

<script>
let colorCache = {};
const palette = ['#0d9488','#2563eb','#7c3aed','#dc2626','#d97706','#059669','#4f46e5','#0891b2','#be123c','#047857'];
export default {
  name: 'DashboardOverview',
  props: {
    apiBase: { type: String, default: 'http://localhost:8000' },
    top: { type: Number, default: 5 }
  },
  emits: ['select-merchant-by-name','loaded','deselect-merchant'],
  data() {
    return {
      interval: '30m',
      loading: false,
      error: null,
      topMerchants: [],
      activeTab: 'risk',
      lookback: 48,
      series: [],
      seriesLoading: false,
      jobs: {},
      jobPollTimer: null,
      seriesRefreshTimer: null,
      lastSelected: null,
  ensuredIntervals: {}, // interval => true once we've ensured
      tabs: [
        { key:'risk', label:'Risk Total', metric:'scores.total' },
        { key:'wl', label:'WL', metric:'scores.wl' },
        { key:'market', label:'Market', metric:'scores.market' },
        { key:'sentiment', label:'Sentiment', metric:'scores.sentiment' },
        { key:'volume', label:'Volume', metric:'scores.volume' },
        // Raw underlying activity (sum of counts) – not capped at 100
        { key:'activity', label:'Activity Sum', metric:'activity_total' },
        { key:'tw', label:'Tweets Ct', metric:'counts.tweets' },
        { key:'rd', label:'Reddit Ct', metric:'counts.reddit' },
        { key:'nw', label:'News Ct', metric:'counts.news' },
        { key:'rv', label:'Reviews Ct', metric:'counts.reviews' },
        // Distinguish WL score vs WL count key (wlct)
        { key:'wlct', label:'WL Ct', metric:'counts.wl' },
        { key:'pr', label:'Prices Ct', metric:'counts.stock_prices' }
      ]
    }
  },
  methods: {
    async fetchData() {
      this.loading = true; this.error = null;
      try {
  const r = await fetch(`${this.apiBase}/v1/dashboard/overview?interval=${this.interval}&top=${this.top}&include_inactive=false`);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const j = await r.json();
        this.topMerchants = j.top_merchants || [];
  // Safety: ensure no inactive merchants leak into visible top list
  this.topMerchants = this.topMerchants.filter(m => m.activation_flag !== false);
        this.$emit('loaded', this.topMerchants);
        // After top list, refresh series
        this.fetchSeries();
        this.fetchJobs();
      } catch (e) {
        this.error = 'Failed to load: ' + e.message;
      } finally {
        this.loading = false;
      }
    },
    async fetchSeries(){
      this.seriesLoading = true;
      try {
        // Load ensured intervals from localStorage (persist between navigations)
        if(Object.keys(this.ensuredIntervals).length === 0){
          try { this.ensuredIntervals = JSON.parse(localStorage.getItem('dashEnsuredIntervals')||'{}')||{}; } catch(_) {}
        }
        const alreadyEnsured = !!this.ensuredIntervals[this.interval];
        // Only trigger ensure if:
        //  - we have not ensured this interval before
        //  - AND there are currently no active jobs (avoid duplicates)
        const ensureFlag = !alreadyEnsured && this.activeJobs.length === 0;
        const r = await fetch(`${this.apiBase}/v1/dashboard/series?interval=${this.interval}&top=${this.top}&lookback=${this.lookback}&ensure=${ensureFlag}&async_ensure=true`);
        if(!r.ok) throw new Error('Series HTTP '+r.status);
        const j = await r.json();
        this.series = j.merchants || [];
        if (Array.isArray(j.jobs) && j.jobs.length){
          this.integrateJobs(j.jobs);
        } else if(ensureFlag){
          // If we requested ensure but no jobs returned, mark as ensured to prevent future progress bars
          this.ensuredIntervals[this.interval] = true;
          localStorage.setItem('dashEnsuredIntervals', JSON.stringify(this.ensuredIntervals));
        }
        // If ensure triggered and jobs active, once they finish we mark ensured
        if(ensureFlag){
          const checkDone = () => {
            if(!this.isProcessing){
              this.ensuredIntervals[this.interval] = true;
              localStorage.setItem('dashEnsuredIntervals', JSON.stringify(this.ensuredIntervals));
            } else {
              setTimeout(checkDone, 1500);
            }
          };
          setTimeout(checkDone, 1500);
        }
        this.$nextTick(()=> this.buildChart());
      } catch(e){
        console.warn('series error', e);
      } finally { this.seriesLoading = false; }
      // If processing, schedule another fetch to progressively fill chart
      this.setupSeriesAutoRefresh();
    },
    integrateJobs(jobArray){
      const map = { ...this.jobs };
      jobArray.forEach(job => {
        const key = job.merchant+':'+job.interval;
        const existing = map[key];
        if(existing){
          // Preserve higher processed count and planned if existing job has progress
            const merged = { ...existing, ...job };
            if(typeof existing.processed === 'number' && typeof job.processed === 'number'){
              merged.processed = Math.max(existing.processed, job.processed);
            }
            if((existing.planned||existing.missing_planned||0) > (job.planned||job.missing_planned||0)){
              // keep larger planned (likely the original)
              if(existing.planned) merged.planned = existing.planned;
              if(existing.missing_planned) merged.missing_planned = existing.missing_planned;
            }
            map[key] = merged;
        } else {
          map[key] = job;
        }
      });
      this.jobs = map;
    },
    async fetchJobs(){
      try {
        const r = await fetch(`${this.apiBase}/v1/risk-eval/jobs?active=true`);
        if(!r.ok) return;
        const j = await r.json();
        const map = {};
        (j.jobs||[]).forEach(job => { map[job.merchant+':'+job.interval] = job; });
        this.jobs = map;
        // If any running, schedule next poll
        const running = Object.values(map).some(v => ['queued','running'].includes(v.status));
        if(running){
          if(this.jobPollTimer) clearTimeout(this.jobPollTimer);
          this.jobPollTimer = setTimeout(()=> this.fetchJobs(), 4000);
        }
      } catch(e){ /* silent */ }
    },
    reloadAll(){ this.fetchData(); },
    setupSeriesAutoRefresh(){
      if(this.seriesRefreshTimer){ clearTimeout(this.seriesRefreshTimer); this.seriesRefreshTimer = null; }
      if(this.isProcessing){
        // Back off slightly if no data yet, faster if empty
        const delay = this.series.length ? 10000 : 5000;
        this.seriesRefreshTimer = setTimeout(()=> this.fetchSeries(), delay);
      }
    },
    fmt(v) { return (v === null || v === undefined) ? '—' : Number(v).toFixed(0); },
    riskClass(v) {
      if (v == null) return 'neutral';
      if (v >= 80) return 'high';
      if (v >= 50) return 'med';
      return 'low';
    },
    timeAgo(iso) {
      if (!iso) return '—';
      try {
        const d = new Date(iso);
        const diff = (Date.now() - d.getTime())/1000;
        if (diff < 60) return Math.floor(diff) + 's ago';
        if (diff < 3600) return Math.floor(diff/60) + 'm ago';
        if (diff < 86400) return Math.floor(diff/3600) + 'h ago';
        return Math.floor(diff/86400) + 'd ago';
      } catch { return iso; }
    },
    metricPath(){
      const tab = this.tabs.find(t=> t.key===this.activeTab);
      return tab ? tab.metric : 'scores.total';
    },
    extractMetric(row){
      const metric = this.metricPath();
      if(metric === 'activity_total'){
        const c = (row.counts)||{};
        const total = ['tweets','reddit','news','reviews','wl','stock_prices']
          .map(k => Number(c[k]||0))
          .reduce((a,b)=>a+b,0);
        return total > 0 ? total : 0; // allow 0 so chart shows baseline
      }
      if(metric.startsWith('counts.')){
        const part = metric.split('.')[1];
        return Number((row.counts||{})[part]||0);
      }
      const path = metric.split('.');
      let cur = row;
      for(const p of path){ if(!cur) return null; cur = cur[p]; }
      return (typeof cur === 'number') ? cur : null;
    },
    colorFor(name){
      if(!colorCache[name]){
        colorCache[name] = palette[Object.keys(colorCache).length % palette.length];
      }
      return colorCache[name];
    },
    riskColor(val){
      if(val == null) return '#9ca3af';
      if(val >= 80) return '#dc2626';
      if(val >= 50) return '#f59e0b';
      return '#059669';
    },
    async buildChart(){
      if(!this.$refs.seriesCanvas) return;
      const canvas = this.$refs.seriesCanvas.getContext('2d');
      const { default: Chart } = await import('chart.js/auto');

      // Destroy previous instance to avoid leaks
      if(this._chart){ this._chart.destroy(); }

      // Build unified x-axis of all timestamps
      const tsSet = new Set();
      this.series.forEach(s => s.series.forEach(r => tsSet.add(r.window_end_ts)));
      const xs = Array.from(tsSet).sort((a,b)=> a-b);
      const labels = xs.map(ts => new Date(ts*1000).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}));

      // Map counts per merchant per ts for tooltip enrichment
      const countsMap = {};
      this.series.forEach(s => {
        countsMap[s.merchant] = {};
        s.series.forEach(r => { countsMap[s.merchant][r.window_end_ts] = r.counts || {}; });
      });

      // Collect all numeric values for scaling
      const allValsRaw = [];
      this.series.forEach(s => s.series.forEach(r => {
        const v = this.extractMetric(r); if(typeof v === 'number') allValsRaw.push(v);
      }));
      const globalMin = allValsRaw.length ? Math.min(...allValsRaw) : 0;
      const globalMax = allValsRaw.length ? Math.max(...allValsRaw) : 100;
      const span = Math.max(1, globalMax - globalMin);

      // Stable merchant color mapping (hash-based, persists across refreshes for same palette)
      function hashName(name){ let h=0; for(let i=0;i<name.length;i++){ h=((h<<5)-h)+name.charCodeAt(i); h|=0; } return Math.abs(h); }
      function stableColor(name){
        if(colorCache[name]) return colorCache[name];
        const idx = hashName(name) % palette.length;
        const col = palette[idx];
        colorCache[name] = col;
        return col;
      }

      // Build datasets with stable merchant-specific colors
      const ds = this.series.map(s => {
        const mapVals = {}; s.series.forEach(r => { mapVals[r.window_end_ts] = this.extractMetric(r); });
        const arr = xs.map(ts => mapVals[ts] ?? null);
        const baseColor = stableColor(s.merchant);
        return {
          label: s.merchant,
          data: arr,
          tension: .3,
          spanGaps: true,
          borderWidth: 2,
          pointRadius: 0,
          borderColor: baseColor,
          fill: false,
          backgroundColor: 'transparent'
        };
      });

      // Suggested max scaling
      const metricKey = this.metricPath();
      const isCountMetric = this.activeTab === 'activity' || metricKey.startsWith('counts.');
      const isPercentageMetric = !isCountMetric && metricKey !== 'activity_total';
      let suggestedMax = globalMax || 100;
      if(isCountMetric){
        suggestedMax = Math.max(10, Math.ceil((globalMax||10) * 1.1));
      } else if(isPercentageMetric){
        suggestedMax = 100; // force full 0-100 view for risk-style metrics
      }

      // Plugins: risk bands + glow on hover + nicer background
      const RiskBandsPlugin = {
        id: 'riskBands',
        beforeDraw: (chart) => {
          const {ctx, chartArea, scales:{y}} = chart; if(!y) return;
          const bands = [
            {from:90,to:100,color:'rgba(127,29,29,0.10)'}, // maroon critical
            {from:80,to:90,color:'rgba(220,38,38,0.08)'},  // red high
            {from:70,to:80,color:'rgba(234,88,12,0.07)'},  // orange warm
            {from:55,to:70,color:'rgba(245,158,11,0.06)'}, // amber elevating
            {from:40,to:55,color:'rgba(5,150,105,0.05)'},  // green low
            {from:0,to:40,color:'rgba(110,231,183,0.04)'}  // light green very low
          ];
          ctx.save();
          bands.forEach(b => {
            if(y.max < b.from || y.min > b.to) return; // band out of view
            const top = y.getPixelForValue(Math.min(b.to, y.max));
            const bottom = y.getPixelForValue(Math.max(b.from, y.min));
            const h = bottom - top;
            if(h <= 0) return;
            ctx.fillStyle = b.color;
            ctx.fillRect(chartArea.left, top, chartArea.width, h);
          });
          ctx.restore();
        }
      };
      const GlowPlugin = {
        id:'glowHover',
        afterDatasetsDraw: (chart) => {
          const active = chart.getActiveElements(); if(!active.length) return;
          const {ctx} = chart;
          active.forEach(pt => {
            const {x,y} = pt.element; const ds = chart.data.datasets[pt.datasetIndex];
            ctx.save();
            ctx.shadowColor = '#ffffff';
            ctx.shadowBlur = 12;
            ctx.lineWidth = 3;
            ctx.strokeStyle = ds.borderColor || '#14b8a6';
            ctx.beginPath(); ctx.arc(x,y,5,0,Math.PI*2); ctx.stroke();
            ctx.restore();
          });
        }
      };

      Chart.register(RiskBandsPlugin, GlowPlugin);

      // Global style tweaks (idempotent-ish)
      Chart.defaults.font.family = 'Segoe UI, system-ui, sans-serif';
      Chart.defaults.color = '#036d69';

      this._chart = new Chart(canvas, {
        type: 'line',
        data: { labels, datasets: ds },
        options: {
          animation: false,
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode:'nearest', intersect:false },
          scales: {
            x: {
              grid: { color: 'rgba(6,95,91,0.08)', drawBorder:false },
              ticks: { color:'#065f5b', maxRotation:0 }
            },
            y: {
              beginAtZero: true,
              // Force 0-100 scale for percentage-like metrics (risk, wl, market, sentiment, volume)
              min: isPercentageMetric ? 0 : undefined,
              max: isPercentageMetric ? 100 : undefined,
              suggestedMax: isPercentageMetric ? 100 : suggestedMax,
              grid: { color: ctx => ctx.tick.value>=80? 'rgba(220,38,38,0.15)': ctx.tick.value>=50? 'rgba(245,158,11,0.12)':'rgba(6,95,91,0.08)', lineWidth:1, drawBorder:false },
              ticks: { color:'#065f5b' }
            }
          },
          plugins: {
            legend: { position: 'bottom', labels:{ usePointStyle:true, pointStyle:'circle', padding:14 } },
            tooltip: {
              backgroundColor:'rgba(6,95,91,0.92)',
              borderColor:'#14b8a6', borderWidth:1, cornerRadius:10, padding:12,
              titleColor:'#f0fdfa', bodyColor:'#fff', displayColors:false,
              callbacks: {
                label: (item) => `${item.dataset.label}: ${item.formattedValue}`,
                afterBody: (items) => {
                  const isActivity = this.activeTab === 'activity';
                  const isCounts = this.metricPath().startsWith('counts.');
                  if(!items || !items.length || (!isActivity && !isCounts)) return '';
                  const idx = items[0].dataIndex; const ts = xs[idx];
                  return items.map(it=>{
                    const c=(countsMap[it.dataset.label]||{})[ts]||{};
                    return ['tweets','reddit','news','reviews','wl','stock_prices']
                      .map(k=>`${k[0].toUpperCase()+k.slice(1,2)}:${c[k]||0}`).join(' ');
                  });
                }
              }
            }
          }
        }
      });
    },
    rowClick(merchant){
      // If user clicks the same merchant again, emit deselect to go back to overview
      if(this.lastSelected === merchant){
        this.lastSelected = null;
        this.$emit('deselect-merchant');
      } else {
        this.lastSelected = merchant;
        this.$emit('select-merchant-by-name', merchant);
      }
    }
  },
  computed: {
    currentMetricLabel(){
      const t = this.tabs.find(t=> t.key===this.activeTab);
      return t ? t.label+' Time Series' : 'Time Series';
    },
    activeJobs(){
      return Object.values(this.jobs).filter(j => ['queued','running'].includes(j.status));
    },
    overallCounts(){
      if(!this.activeJobs.length) return { processed:0, planned:0 };
      let processed = 0, planned = 0;
      this.activeJobs.forEach(j => {
        processed += (j.processed||0);
        planned += (j.planned || j.missing_planned || 0);
      });
      return { processed, planned };
    },
    overallPercent(){
      if(!this.activeJobs.length) return null;
      let totalPlanned = 0; let totalProcessed = 0; let fallbackPerc = 0; let withPlan = 0;
      this.activeJobs.forEach(j => {
        const planned = j.planned || j.missing_planned || 0;
        const processed = j.processed || 0;
        const percent = j.percent != null ? j.percent : (planned? (processed/planned*100):0);
        if(planned){ totalPlanned += planned; totalProcessed += processed; withPlan++; }
        else fallbackPerc += percent;
      });
      if(totalPlanned>0) return Math.min(100, (totalProcessed/totalPlanned)*100);
      return fallbackPerc / Math.max(1, this.activeJobs.length);
    },
    isProcessing(){ return this.activeJobs.length > 0; }
  },
  mounted() { this.fetchData(); },
  watch: {
    activeTab(){ this.buildChart(); },
    isProcessing(){
      // start/stop series auto refresh
      this.setupSeriesAutoRefresh();
    }
  },
  beforeUnmount(){
    if(this._chart) this._chart.destroy();
    if(this.jobPollTimer) clearTimeout(this.jobPollTimer);
    if(this.seriesRefreshTimer) clearTimeout(this.seriesRefreshTimer);
  }
}
</script>

<style scoped>
.dashboard-overview-card { width: 100%; }
.top-bar { display:flex; justify-content:space-between; gap:20px; align-items:flex-start; flex-wrap:wrap; }
.title-block h3 { margin:0; font-size:18px; color:#008080; }
.subline { font-size:12px; color:#6b7280; margin-top:4px; }
.controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
.btn { background:#008080; color:#fff; border:none; padding:6px 14px; border-radius:6px; cursor:pointer; font-size:13px; font-weight:500; }
.btn:disabled { opacity:.5; cursor:not-allowed; }
.interval-select select { padding:4px 8px; border:1px solid #14b8a6; border-radius:6px; }
.loading, .error { padding: 15px; font-size:14px; }
.error { color:#b91c1c; }
.overview-table { width:100%; border-collapse: collapse; font-size:13px; }
.overview-table th, .overview-table td { padding:8px 10px; border-bottom:1px solid #e5e7eb; text-align:left; }
.overview-table tbody tr.row-click { cursor:pointer; transition: background .15s; }
.overview-table tbody tr.row-click:hover { background:#f0fdfa; }
/* Progress bar styles */
.progress-wrap { display:flex; flex-direction:column; gap:4px; }
.progress-label { font-size:10px; font-weight:600; color:#065f5b; }
.progress-bar { width:100%; height:6px; background:#e5e7eb; border-radius:4px; overflow:hidden; position:relative; }
.progress-bar > span { display:block; height:100%; background:linear-gradient(90deg,#14b8a6,#0d9488); width:0; transition:width .4s ease; }
.score-pill { display:inline-block; padding:2px 8px; border-radius:12px; font-weight:600; font-size:12px; }
.score-pill.high { background:#991b1b; color:#fff; }
.score-pill.med { background:#f59e0b; color:#222; }
.score-pill.low { background:#10b981; color:#fff; }
.score-pill.neutral { background:#9ca3af; color:#fff; }
.inactive-dot { width:8px; height:8px; border-radius:50%; background:#dc2626; display:inline-block; box-shadow:0 0 0 2px #fee2e2; }
.auto-pill { display:inline-block; padding:2px 6px; font-size:10px; border-radius:10px; font-weight:600; }
.auto-pill.on { background:#d1fae5; color:#065f46; border:1px solid #10b981; }
.auto-pill.off { background:#f1f5f9; color:#64748b; border:1px solid #cbd5e1; }
.name-cell { display:flex; align-items:center; gap:6px; }
.confidence { font-size:10px; background:#e6fbf8; color:#036d69; padding:2px 6px; border-radius:8px; }
.tabs { display:flex; gap:6px; margin-top:14px; border-bottom:1px solid #e5e7eb; }
.tab { padding:6px 12px; font-size:12px; cursor:pointer; font-weight:600; color:#036d69; border:1px solid transparent; border-top-left-radius:6px; border-top-right-radius:6px; }
.tab.active { background:#f0fdfa; border-color:#14b8a6 #14b8a6 #ffffff; }
.big-chart-wrapper { position:relative; margin-top:24px; height:340px; background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:14px 16px 10px; display:flex; flex-direction:column; gap:10px; }
.chart-header { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:16px; }
.chart-header h4 { margin:0; font-size:15px; color:#036d69; font-weight:600; }
.legend { display:flex; flex-wrap:wrap; gap:10px; font-size:11px; }
.legend-item { display:flex; align-items:center; gap:5px; background:#f0fdfa; padding:4px 8px; border-radius:12px; border:1px solid #14b8a6; }
.legend-item .dot { width:10px; height:10px; border-radius:50%; }
.overlay-loading { position:absolute; inset:0; background:rgba(255,255,255,0.6); display:flex; align-items:center; justify-content:center; font-size:13px; font-weight:600; color:#036d69; border-radius:12px; }
.empty-series { margin-top:18px; padding:16px; background:#f8fafc; border:1px dashed #14b8a6; border-radius:10px; font-size:13px; color:#065f5b; text-align:center; }
</style>
