<template>
  <section class="risk-container">
    <header class="risk-header">
      <div class="risk-title">
        <h3>Risk Analytics</h3>
        <span class="sub">On-demand computed windows <span v-if="isSynthetic" class="fake-badge" title="Synthetic fallback (set RISK_SYNTHETIC_FALLBACK=0 to disable)">SYNTH</span></span>
      </div>
      <div class="risk-actions">
        <select v-model="interval" @change="refresh()">
          <option value="30m">30m</option>
          <option value="1h">1h</option>
          <option value="1d">1d</option>
          <option value="auto">Auto</option>
        </select>
        <button class="btn" @click="refresh" :disabled="loading">Refresh</button>
      </div>
    </header>

    <div v-if="error" class="alert error">{{ error }}</div>
    <div v-if="loading" class="alert info">Loading risk summary…</div>

    <div v-if="summary && summary.count" class="grid">
      <div class="card stat" :class="{synthetic:isSynthetic}">
        <div class="stat-label">Latest Risk Score</div>
        <div class="stat-value" :class="riskClass">{{ latestScoreDisplay }}</div>
        <div class="stat-delta" :class="deltaClass" v-if="summary.delta!=null">
          Δ {{ summary.delta > 0 ? '+' : '' }}{{ summary.delta }}
        </div>
        <div class="confidence">Confidence: {{ (summary.latest?.confidence??0).toFixed(2) }}</div>
        <div class="confidence-bar-wrap" v-if="summary.latest">
          <div class="confidence-bar-bg">
            <div class="confidence-bar-fg" :style="{width: ((summary.latest.confidence||0)*100)+'%'}"></div>
          </div>
        </div>
      </div>

      <div class="card components" v-if="summary.component_latest">
        <div class="comp-title">Component Contributions (latest normalized 0..1)</div>
        <div class="comp-list">
          <div class="comp-row" v-for="(v,k) in summary.component_latest" :key="k">
            <span class="comp-key">{{ k }}</span>
            <div class="bar-wrap" :title="v == null ? 'No data' : v.toFixed(3)">
              <div class="bar" :style="barStyle(v)"></div>
            </div>
            <span class="comp-val">{{ v==null? '—' : v.toFixed(2) }}</span>
          </div>
        </div>
      </div>

      <div class="card trend" v-if="summary.trend?.length">
        <canvas ref="trendCanvas"></canvas>
        <div class="mini-legend">Chronological window end vs score</div>
      </div>

      <div class="card conf" v-if="summary.trend_confidence?.length">
        <canvas ref="confCanvas"></canvas>
        <div class="mini-legend">Confidence trend</div>
      </div>

      <div class="card stats">
        <h4>Aggregates</h4>
        <div class="agg-row"><span>Windows</span><b>{{ summary.count }}</b></div>
        <div class="agg-row"><span>Average Score</span><b>{{ summary.avg_score ?? '—' }}</b></div>
        <div class="agg-row"><span>Average Confidence</span><b>{{ summary.avg_confidence ?? '—' }}</b></div>
        <div class="agg-row" v-for="(v,k) in summary.component_avg || {}" :key="k">
          <span>{{ k }} avg</span><b>{{ v.toFixed(2) }}</b>
        </div>
      </div>
    </div>

    <!-- Recent windows table -->
    <div v-if="recentScores.length" class="card table-card">
      <div class="table-head">
        <h4>Recent Windows ({{ recentScores.length }})</h4>
        <div class="mini-controls">
          <label>Show
            <select v-model.number="tableLimit" @change="fetchScores()">
              <option v-for="n in [15,30,60,120]" :key="n" :value="n">{{ n }}</option>
            </select>
          </label>
          <button class="btn xs" @click="fetchScores" :disabled="loadingScores">Reload</button>
        </div>
      </div>
      <div class="table-wrap">
        <table class="risk-table">
          <thead>
            <tr>
              <th>End</th>
              <th>Score</th>
              <th>Conf</th>
              <th>Twt</th>
              <th>Red</th>
              <th>News</th>
              <th>Rev</th>
              <th>WL</th>
              <th>Vol</th>
              <th>Δ</th>
              <th title="Synthetic?">S</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(r,i) in recentScores" :key="r.window_start_ts">
              <td :title="formatIso(r.window_end_iso)">{{ shortTime(r.window_end_ts) }}</td>
              <td :class="scoreClass(r.score)" class="mono">{{ r.score==null?'—':r.score.toFixed(1) }}</td>
              <td class="mono" :title="(r.confidence||0).toFixed(3)">{{ (r.confidence||0).toFixed(2) }}</td>
              <td class="mono" :title="compTooltip(r,'tweet_sentiment')">{{ compCell(r,'tweet_sentiment') }}</td>
              <td class="mono" :title="compTooltip(r,'reddit_sentiment')">{{ compCell(r,'reddit_sentiment') }}</td>
              <td class="mono" :title="compTooltip(r,'news_sentiment')">{{ compCell(r,'news_sentiment') }}</td>
              <td class="mono" :title="compTooltip(r,'reviews_rating')">{{ compCell(r,'reviews_rating') }}</td>
              <td class="mono" :title="compTooltip(r,'wl_flag_ratio')">{{ compCell(r,'wl_flag_ratio') }}</td>
              <td class="mono" :title="compTooltip(r,'stock_volatility')">{{ compCell(r,'stock_volatility') }}</td>
              <td class="mono" :class="deltaCellClass(r,i)">{{ deltaPrev(r,i) }}</td>
              <td class="mono" :title="r.synthetic ? 'Synthetic fallback window' : ''">{{ r.synthetic ? '✓' : '' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-else-if="!loading && !error" class="empty">
      No risk scores yet. Click Refresh to compute windows.
    </div>
  </section>
</template>

<script>
import { ref, onMounted, watch, computed } from 'vue';

export default {
  name: 'MerchantRisk',
  props: { merchant: { type: String, required: true }, apiBase: { type: String, default: 'http://localhost:8000' }, windowRange: { type: String, default: null }, nowEpoch: { type: Number, default: null } },
  setup(props){
  const interval = ref('auto');
  const loading = ref(false);
  const loadingScores = ref(false);
  const error = ref('');
  const summary = ref(null);
  const recentScores = ref([]);
  const tableLimit = ref(30);
  const trendCanvas = ref(null);
  const confCanvas = ref(null);
  let Chart = null; let trendChart = null; let confChart = null;
  const activeWindow = ref(props.windowRange);
  const simNowTs = ref(props.nowEpoch);

  const isSynthetic = computed(()=> {
    if(summary.value?.latest?.synthetic) return true;
    return recentScores.value.some(r=> r.synthetic);
  });

  const intervalLabel = computed(()=> (summary.value?.interval_selected) || interval.value);

  async function ensureChart(){ if (Chart) return; try { const m = await import('chart.js/auto'); Chart = m.default || m; } catch(e){ console.warn('Chart.js missing',e);} }

  async function fetchSummary(){
    if(!props.merchant) return; loading.value = true; error.value='';
    try {
      const qp = new URLSearchParams();
      qp.set('interval', interval.value);
      if(activeWindow.value) qp.set('window', activeWindow.value);
      if(simNowTs.value) qp.set('now', simNowTs.value.toString());
      const url = `${props.apiBase}/v1/${encodeURIComponent(props.merchant)}/risk-eval/summary?${qp.toString()}`;
      const r = await fetch(url);
      if(!r.ok){ throw new Error(await r.text()); }
      summary.value = await r.json();
      await fetchScores();
      await ensureChart();
      buildCharts();
    } catch(e){ error.value = e.message || String(e); }
    finally { loading.value = false; }
  }

  async function fetchScores(){
    if(!props.merchant) return; loadingScores.value = true;
    try {
      const qp = new URLSearchParams();
      qp.set('interval', interval.value==='auto'? (summary.value?.interval_selected || '30m') : interval.value);
      qp.set('limit', tableLimit.value.toString());
      const url = `${props.apiBase}/v1/${encodeURIComponent(props.merchant)}/risk-eval/scores?${qp.toString()}`;
      const r = await fetch(url);
      if(r.ok){ const data = await r.json(); recentScores.value = (data.scores||[]).sort((a,b)=> b.window_start_ts - a.window_start_ts); }
    } catch(e){ /* ignore */ }
    finally { loadingScores.value = false; }
  }

  function buildCharts(){
    if(!Chart || !summary.value) return;
    const s = summary.value;
    const trendData = (s.trend||[]).map(d=> d.s);
    const trendLabels = (s.trend||[]).map(d=> new Date(d.t*1000).toLocaleString());
    const confData = (s.trend_confidence||[]).map(d=> d.c);
    if(trendCanvas.value){ if(trendChart) trendChart.destroy(); trendChart = new Chart(trendCanvas.value.getContext('2d'), { type:'line', data:{ labels: trendLabels, datasets:[{label:'Score', data: trendData, borderColor:'#0f766e', backgroundColor:'rgba(20,184,166,.15)', tension:.2 }]}, options:{responsive:true, scales:{y:{beginAtZero:true,max:100}}}}); }
    if(confCanvas.value){ if(confChart) confChart.destroy(); confChart = new Chart(confCanvas.value.getContext('2d'), { type:'line', data:{ labels: trendLabels, datasets:[{label:'Confidence', data: confData, borderColor:'#1d4ed8', backgroundColor:'rgba(59,130,246,.15)', tension:.2 }]}, options:{responsive:true, scales:{y:{beginAtZero:true,max:1}}}}); }
  }

  function refresh(){ fetchSummary(); }
  function barStyle(v){ if(v==null) return { background:'#e5e7eb', width:'4px'}; const pct=Math.round(v*100); let color='#14b8a6'; if(pct>66) color='#f87171'; else if(pct>33) color='#fbbf24'; return { width:pct+'%', background:color }; }
  function formatIso(x){ return x||''; }
  function shortTime(ts){ if(!ts) return ''; try { return new Date(ts*1000).toLocaleString(); } catch { return ts; } }
  function compCell(r,key){ const v=(r.components||{})[key]; if(v==null) return '·'; return v.toFixed(2); }
  function compTooltip(r,key){ const v=(r.components||{})[key]; return v==null? 'No data' : `${key}: ${v.toFixed(4)}`; }
  function scoreClass(s){ if(s==null) return ''; if(s>=70) return 'high'; if(s>=40) return 'med'; return 'low'; }
  function deltaPrev(r,i){ if(i===recentScores.value.length-1) return ''; const prev = recentScores.value[i+1]; if(!prev || r.score==null || prev.score==null) return ''; const d = r.score - prev.score; if(Math.abs(d)<0.01) return '0'; return (d>0? '+' : '')+d.toFixed(1); }
  function deltaCellClass(r,i){ const txt = deltaPrev(r,i); if(!txt) return ''; return txt.startsWith('+')? 'delta-up' : (txt.startsWith('-')? 'delta-down' : ''); }

  const latestScoreDisplay = computed(()=>{ if(!summary.value?.latest?.score && summary.value?.latest?.score!==0) return '—'; return summary.value.latest.score.toFixed(1); });
  const riskClass = computed(()=>{ const s = summary.value?.latest?.score; if(s==null) return ''; if(s>=70) return 'high'; if(s>=40) return 'med'; return 'low'; });
  const deltaClass = computed(()=> summary.value?.delta != null ? (summary.value.delta>0 ? 'delta-up':'delta-down') : '' );

  watch(()=> interval.value, ()=> fetchSummary());
  watch(()=> props.windowRange, v => { activeWindow.value = v; });
  watch(()=> props.nowEpoch, v => { simNowTs.value = v; });
  watch(activeWindow, ()=> { fetchSummary(); });
  watch(simNowTs, ()=> { fetchSummary(); });
  onMounted(()=> { fetchSummary(); });

  return { interval, loading, error, summary, refresh, trendCanvas, confCanvas, barStyle, latestScoreDisplay, riskClass, deltaClass, recentScores, tableLimit, loadingScores, formatIso, shortTime, compCell, compTooltip, scoreClass, deltaPrev, deltaCellClass, fetchScores, isSynthetic };
  }
};
</script>

<style scoped>
.fake-badge{background:#fde68a;color:#92400e;padding:2px 6px;border-radius:6px;font-size:10px;margin-left:4px;font-weight:600;}
.stat.synthetic::after{content:'Synthetic';position:absolute;top:6px;right:8px;font-size:10px;background:#fde68a;color:#92400e;padding:2px 4px;border-radius:4px;}
.risk-container{display:grid;gap:16px;}
.risk-header{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;}
.risk-title{display:flex;align-items:baseline;gap:8px;}
.risk-title h3{margin:0;font-size:16px;color:#0f766e;font-weight:700;}
.sub{font-size:12px;color:#6b7280;}
.risk-actions{display:flex;gap:8px;align-items:center;}
select{border:1px solid #d1d5db;border-radius:6px;padding:6px 8px;font-size:13px;}
.btn{background:#0f766e;color:#f0fdfa;border:1px solid #0f766e;border-radius:6px;padding:6px 12px;font-size:12px;font-weight:600;cursor:pointer;}
.btn:hover{background:#115e59;}
.alert{padding:8px 12px;border-radius:8px;font-size:13px;}
.alert.info{background:#f0fdfa;color:#0f766e;border:1px solid #99f6e4;}
.alert.error{background:#fef2f2;color:#b91c1c;border:1px solid #fecaca;}
.empty{padding:14px;font-size:13px;background:#f8fafc;border:1px dashed #cbd5e1;border-radius:8px;color:#475569;}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px;}
.card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:12px;display:grid;gap:12px;position:relative;}
.stat-value{font-size:32px;font-weight:700;}
.stat-value.low{color:#10b981;}
.stat-value.med{color:#fbbf24;}
.stat-value.high{color:#ef4444;}
.stat-delta{font-size:12px;font-weight:600;}
.delta-up{color:#ef4444;}
.delta-down{color:#10b981;}
.confidence{font-size:11px;color:#6b7280;}
.confidence-bar-wrap{width:100%;}
.confidence-bar-bg{height:6px;background:#e2e8f0;border-radius:4px;overflow:hidden;}
.confidence-bar-fg{height:100%;background:linear-gradient(90deg,#14b8a6,#0f766e);transition:width .6s ease;}
.components .comp-title{font-size:13px;font-weight:600;color:#0f766e;}
.comp-list{display:grid;gap:6px;}
.comp-row{display:grid;grid-template-columns:120px 1fr 40px;align-items:center;gap:6px;font-size:12px;}
.comp-key{color:#475569;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.bar-wrap{height:8px;background:#f1f5f9;border-radius:4px;overflow:hidden;}
.bar{height:100%;border-radius:4px;transition:width .4s ease;}
.comp-val{text-align:right;font-weight:600;}
.trend,.conf{height:260px;}
.mini-legend{font-size:11px;color:#6b7280;}
.stats h4{margin:0;font-size:14px;color:#0f766e;}
.table-card{display:flex;flex-direction:column;gap:8px;}
.table-head{display:flex;justify-content:space-between;align-items:center;}
.table-head h4{margin:0;font-size:14px;color:#0f766e;}
.table-head .mini-controls{display:flex;gap:12px;font-size:12px;align-items:center;}
.table-head select{padding:2px 4px;font-size:12px;}
.btn.xs{padding:4px 8px;font-size:11px;}
.table-wrap{max-height:260px;overflow:auto;border:1px solid #e2e8f0;border-radius:6px;}
.risk-table{width:100%;border-collapse:collapse;font-size:11px;}
.risk-table thead th{position:sticky;top:0;background:#f1f5f9;font-weight:600;padding:4px 6px;border-bottom:1px solid #cbd5e1;z-index:1;}
.risk-table tbody td{padding:4px 6px;border-bottom:1px solid #f1f5f9;white-space:nowrap;}
.risk-table tbody tr:last-child td{border-bottom:none;}
.risk-table td.high{color:#ef4444;font-weight:600;}
.risk-table td.med{color:#d97706;font-weight:600;}
.risk-table td.low{color:#059669;font-weight:600;}
.risk-table td.delta-up{color:#ef4444;font-weight:600;}
.risk-table td.delta-down{color:#059669;font-weight:600;}
.mono{font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;}
.agg-row{display:flex;justify-content:space-between;font-size:12px;padding:2px 0;border-bottom:1px solid #f1f5f9;}
.agg-row:last-child{border-bottom:none;}
@media (max-width:700px){.comp-row{grid-template-columns:90px 1fr 32px;} }
</style>
