<template>
  <section class="rv-container">
    <header class="rv-header">
      <div class="rv-title">
        <h3>Reviews Analytics</h3>
        <span class="rv-sub" v-if="summaryRange">{{ summaryRange }}</span>
      </div>
      <div class="rv-kpis">
        <div class="kpi">
          <div class="kpi-label">Reviews</div>
          <div class="kpi-value">{{ totalReviews }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Avg Rating</div>
          <div class="kpi-value">{{ avgRating != null ? avgRating.toFixed(2) : '-' }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Positive %</div>
          <div class="kpi-value">{{ positivePct != null ? positivePct.toFixed(1) + '%' : '-' }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Negative %</div>
          <div class="kpi-value">{{ negativePct != null ? negativePct.toFixed(1) + '%' : '-' }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Users</div>
          <div class="kpi-value">{{ uniqueUsers }}</div>
        </div>
      </div>
    </header>

    <div v-if="loading" class="rv-refresh">Refreshing… (previous data shown)</div>
    <div v-else-if="!normalized.length" class="rv-empty">No reviews in this range.</div>

    <section class="rv-charts" v-if="normalized.length">
      <div class="chart-card">
        <div class="chart-title">Rating Distribution</div>
        <div v-if="chartJsLoaded" class="chart-wrap" style="height:200px;">
          <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
          <canvas ref="ratingCanvas" height="120"></canvas>
        </div>
        <div v-else class="chart-fallback">Count by rating (Chart.js unavailable)</div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Sentiment Mix</div>
        <div v-if="chartJsLoaded" class="chart-wrap" style="height:200px;">
          <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
          <canvas ref="sentimentCanvas" height="120"></canvas>
        </div>
        <div v-else class="chart-fallback">Pos: {{ sentimentCounts.positive }} / Neu: {{ sentimentCounts.neutral }} / Neg: {{ sentimentCounts.negative }}</div>
      </div>
      <div class="chart-card" style="grid-column:span 2;">
        <div class="chart-title">Top Keywords (simple)</div>
        <div class="keywords" v-if="topKeywords.length">
          <span v-for="k in topKeywords" :key="k.word" class="kw" :title="k.word + ' ('+k.count+')'">{{ k.word }} <small>{{ k.count }}</small></span>
        </div>
        <div v-else class="chart-fallback">Not enough textual content.</div>
      </div>
    </section>

  </section>
</template>

<script>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue';
export default {
  name: 'MerchantReviews',
  props: {
    reviews: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false },
  },
  setup(props){
    let Chart = null;
    const chartJsLoaded = ref(false);
    const chartsBuilding = ref(true);

    const ratingCanvas = ref(null);
    const sentimentCanvas = ref(null);
    let ratingChart = null;
    let sentimentChart = null;

    const normalized = computed(()=> {
      const out = [];
      for (const r of (props.reviews||[])) {
        try {
          if (!r) continue;
          // fields: rating, text/body/content, sentiment_label, user_id
          const rating = r.rating != null ? Number(r.rating) : (r.stars != null ? Number(r.stars) : null);
          const txt = r.text || r.body || r.content || '';
          let sentiment = (r.sentiment_label || r.sentiment || '').toString().toLowerCase();
          if (!['positive','neutral','negative'].includes(sentiment)) sentiment = 'neutral';
          const user = r.user_id || r.author_id || r.customer_id || r.user || '';
          const dt = r.dt || r.created_at || r.created || r.timestamp || null;
          out.push({ rating, text: txt, sentiment, user, dt });
        } catch {}
      }
      return out;
    });

    const totalReviews = computed(()=> normalized.value.length);
    const uniqueUsers = computed(()=> new Set(normalized.value.map(r=>r.user).filter(Boolean)).size);
    const avgRating = computed(()=> {
      const vals = normalized.value.map(r=> r.rating).filter(v=> typeof v === 'number' && !isNaN(v));
      if (!vals.length) return null;
      return vals.reduce((a,b)=>a+b,0)/vals.length;
    });

    const sentimentCounts = computed(()=> {
      const c = { positive:0, neutral:0, negative:0 };
      normalized.value.forEach(r=> { c[r.sentiment] = (c[r.sentiment]||0)+1; });
      return c;
    });
    const positivePct = computed(()=> totalReviews.value ? sentimentCounts.value.positive/totalReviews.value*100 : null);
    const negativePct = computed(()=> totalReviews.value ? sentimentCounts.value.negative/totalReviews.value*100 : null);

    const ratingCounts = computed(()=> {
      const map = new Map();
      normalized.value.forEach(r=> { if (typeof r.rating === 'number') map.set(r.rating, (map.get(r.rating)||0)+1); });
      return Array.from(map.entries()).sort((a,b)=> a[0]-b[0]);
    });

    const topKeywords = computed(()=> {
      const freq = new Map();
      normalized.value.forEach(r=> {
        const words = (r.text||'').toLowerCase().split(/[^a-z0-9]+/g).filter(w=> w.length>=4 && w.length<=18);
        words.forEach(w=> freq.set(w, (freq.get(w)||0)+1));
      });
      const arr = Array.from(freq.entries()).filter(([w,c])=> c>=2).sort((a,b)=> b[1]-a[1]).slice(0,20);
      return arr.map(([word,count])=> ({ word, count }));
    });

    const summaryRange = computed(()=> {
      const dts = normalized.value.map(r=> r.dt).filter(Boolean);
      if (!dts.length) return '';
      try {
        const conv = dts.map(d=> new Date(d).getTime()).filter(x=> !isNaN(x)).sort((a,b)=> a-b);
        if (!conv.length) return '';
        return new Date(conv[0]).toLocaleDateString() + ' – ' + new Date(conv[conv.length-1]).toLocaleDateString();
      } catch { return ''; }
    });

    async function ensureChartJs(){
      if (Chart) return;
      try {
        const m = await import('chart.js/auto');
        Chart = m.default || m;
        chartJsLoaded.value = true;
      } catch(e){ chartJsLoaded.value = false; }
    }

    function buildOrUpdate(){
      if (!chartJsLoaded.value) return;

      // Rating distribution
      const rc = ratingCounts.value;
      if (!ratingChart && ratingCanvas.value){
        ratingChart = new Chart(ratingCanvas.value.getContext('2d'), {
          type:'bar',
          data:{ labels: rc.map(r=> r[0]), datasets:[{ label:'Count', data: rc.map(r=> r[1]), backgroundColor:'#6366f1' }]},
          options:{ responsive:true, maintainAspectRatio:false, plugins:{ legend:{display:false} }, scales:{ y:{ beginAtZero:true } } }
        });
      } else if (ratingChart){
        ratingChart.options.animation = false;
        ratingChart.data.labels = rc.map(r=> r[0]);
        ratingChart.data.datasets[0].data = rc.map(r=> r[1]);
        ratingChart.update();
      }

      // Sentiment doughnut
      const sc = sentimentCounts.value;
      if (!sentimentChart && sentimentCanvas.value){
        sentimentChart = new Chart(sentimentCanvas.value.getContext('2d'), {
          type:'doughnut',
          data:{ labels:['Positive','Neutral','Negative'], datasets:[{ data:[sc.positive, sc.neutral, sc.negative], backgroundColor:['#10b981','#9ca3af','#ef4444'] }]},
          options:{ plugins:{ legend:{ position:'bottom'} } }
        });
      } else if (sentimentChart){
        sentimentChart.options.animation = false;
        sentimentChart.data.datasets[0].data = [sc.positive, sc.neutral, sc.negative];
        sentimentChart.update();
      }

      requestAnimationFrame(()=> chartsBuilding.value=false);
    }

    watch(()=> props.reviews, async () => {
      if (!Chart) await ensureChartJs();
      await nextTick();
      if (!props.reviews.length){
        [ratingChart, sentimentChart].forEach(ch=> { if (ch) { ch.options.animation=false; ch.data.labels=[]; ch.data.datasets.forEach(ds=> ds.data=[]); ch.update(); }});
        chartsBuilding.value = false;
        return;
      }
      chartsBuilding.value = true;
      buildOrUpdate();
    }, { deep:true });

    let resizeObs = null;
    onMounted(async () => {
      await ensureChartJs();
      await nextTick();
      buildOrUpdate();
      try {
        resizeObs = new ResizeObserver(()=> { [ratingChart, sentimentChart].forEach(ch=> ch && ch.resize()); });
        const host = ratingCanvas.value?.parentElement?.parentElement;
        if (host) resizeObs.observe(host);
      } catch {}
    });
    onBeforeUnmount(()=> { if (resizeObs) try { resizeObs.disconnect(); } catch{} [ratingChart, sentimentChart].forEach(ch=> { if (ch) try { ch.destroy(); } catch{} }); });

    return { chartJsLoaded, chartsBuilding, ratingCanvas, sentimentCanvas, totalReviews, avgRating, positivePct, negativePct, uniqueUsers, sentimentCounts, ratingCounts, topKeywords, normalized, summaryRange };
  }
};
</script>

<style scoped>
.rv-container { display:grid; gap:16px; }
.rv-header { display:grid; gap:10px; }
.rv-title { display:flex; align-items:baseline; gap:10px; }
.rv-title h3 { margin:0; color:#0f766e; font-size:16px; font-weight:700; }
.rv-sub { color:#6b7280; font-size:12px; }
.rv-kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:8px; }
.kpi { background:#f9fafb; border:1px solid #e5e7eb; border-radius:10px; padding:10px; display:grid; gap:6px; }
.kpi-label { color:#6b7280; font-size:12px; }
.kpi-value { color:#111827; font-weight:700; font-size:18px; }
.rv-refresh { padding:6px 10px; font-size:12px; color:#0f766e; font-weight:600; }
.rv-empty { padding:14px; background:#f8fafc; border:1px dashed #d1d5db; border-radius:8px; color:#374151; }
.rv-charts { display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:12px; }
.chart-card { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:12px; display:grid; gap:10px; }
.chart-title { color:#0f766e; font-size:14px; font-weight:700; }
.chart-wrap { position:relative; height:200px; }
.chart-loader { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; background:linear-gradient(135deg,#f8fafc,#eef2f7); font-size:13px; color:#0f766e; font-weight:600; z-index:2; }
.chart-fallback { color:#6b7280; font-size:13px; }
.keywords { display:flex; flex-wrap:wrap; gap:6px; }
.kw { background:#f0fdfa; border:1px solid #14b8a6; color:#008080; padding:4px 8px; border-radius:16px; font-size:11px; font-weight:600; }
.kw small { color:#0f766e; font-size:10px; margin-left:4px; }
</style>
