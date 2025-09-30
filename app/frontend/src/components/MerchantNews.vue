<template>
  <div class="news-wrapper">
    <!-- Header + Controls -->
    <div class="toolbar-card">
      <div class="toolbar-top">
        <h4 class="section-title">News Intelligence Feed</h4>
        <div class="toolbar-actions">
          <button class="btn small" :disabled="!news.length || summarizing" @click="generateSummary">
            <span v-if="!summarizing && !summaryText">⚡ Summarize</span>
            <span v-else-if="summarizing">Summarizing…</span>
            <span v-else>↻ Re-Summarize</span>
          </button>
          <button class="btn outline small" :disabled="!summaryText" @click="copySummary">Copy</button>
          <button class="btn outline small" :disabled="!news.length" @click="toggleLayout">{{ layout==='grid' ? 'List' : 'Grid' }}</button>
        </div>
      </div>
      <div class="metrics" v-if="news.length">
        <div class="metric">
          <div class="m-label">Articles</div>
          <div class="m-value">{{ news.length }}</div>
        </div>
        <div class="metric">
          <div class="m-label">Sources</div>
          <div class="m-value">{{ distinctSources }}</div>
        </div>
        <div class="metric">
          <div class="m-label">Avg Sentiment</div>
          <div class="m-value" :class="sentimentClass(avgSentiment)">{{ avgSentimentLabel }}</div>
        </div>
        <div class="metric">
          <div class="m-label">Span</div>
          <div class="m-value">{{ spanRange }}</div>
        </div>
        <div class="metric dist" v-if="sentimentDist.total">
          <div class="m-label">Sentiment Mix</div>
          <div class="dist-bar">
            <div class="pos" :style="{width: sentimentDist.posPct + '%'}"></div>
            <div class="neu" :style="{width: sentimentDist.neuPct + '%'}"></div>
            <div class="neg" :style="{width: sentimentDist.negPct + '%'}"></div>
          </div>
          <div class="dist-legend">+
            {{ sentimentDist.pos }} / {{ sentimentDist.neu }} / {{ sentimentDist.neg }}
          </div>
        </div>
      </div>
      <div v-if="topKeywords.length" class="keywords-row">
        <span v-for="k in topKeywords" :key="k.word" class="kw-chip" :title="k.count + ' mentions'">#{{ k.word }}</span>
      </div>
    </div>

    <!-- Summary Panel -->
    <transition name="fade">
      <div v-if="summaryText" class="summary-card">
        <div class="summary-header">
          <h5>AI Summary</h5>
          <button class="link-btn" @click="summaryExpanded = !summaryExpanded">{{ summaryExpanded ? 'Collapse' : 'Expand' }}</button>
        </div>
        <div v-show="summaryExpanded" class="summary-body">
          <ul class="summary-points">
            <li v-for="(p,i) in summaryPoints" :key="i">{{ p }}</li>
          </ul>
          <div v-if="summaryMeta.keywords.length" class="summary-tags">
            <span v-for="k in summaryMeta.keywords" :key="k" class="tag keyword">{{ k }}</span>
          </div>
        </div>
      </div>
    </transition>

    <div v-if="loading" class="loading">Loading news...</div>
    <div v-else-if="!news.length" class="empty">No news items.</div>
    <div v-else class="feed-wrapper">
      <div class="feed-header-mini" v-if="news.length > visibleLimit">
        <button class="mini-toggle" @click="expanded = !expanded">{{ expanded ? 'Collapse' : 'Expand' }}</button>
      </div>
      <div :class="['feed-container', layout, expanded? 'expanded':'collapsed']">
        <div v-for="(n,i) in visibleNews" :key="i" :class="['news-card', sentimentClass(n.sentiment||0)]" @click="openModal(n)">
        <div class="card-top">
          <div class="headline" :title="n.title">{{ n.title || 'Untitled' }}</div>
          <div class="meta-row">
            <span class="source" v-if="n.source">{{ n.source }}</span>
            <span class="time" v-if="n.published_at">{{ relativeTime(n.published_at) }}</span>
          </div>
        </div>
        <div class="snippet">{{ truncated(n) }}</div>
        <div class="tags" v-if="computedTags(n).length">
          <span v-for="t in computedTags(n).slice(0,5)" :key="t" class="tag">{{ t }}</span>
        </div>
        <div class="footer-row">
          <span v-if="typeof n.sentiment==='number'" class="sentiment-pill" :class="sentimentClass(n.sentiment)">{{ sentimentLabel(n.sentiment) }}</span>
          <a v-if="n.url" :href="n.url" target="_blank" class="open-link" @click.stop>Open ↗</a>
        </div>
        </div>
      </div>
    </div>

    <!-- Detail Modal -->
    <transition name="fade">
      <div v-if="modalItem" class="modal-overlay" @click="closeModal">
        <div class="modal" @click.stop>
          <div class="modal-header">
            <h4>{{ modalItem.title }}</h4>
            <button class="close" @click="closeModal">×</button>
          </div>
            <div class="modal-meta">
              <span v-if="modalItem.source">{{ modalItem.source }}</span>
              <span v-if="modalItem.published_at">{{ formatTime(modalItem.published_at) }}</span>
              <span v-if="typeof modalItem.sentiment==='number'" :class="['sentiment-pill', sentimentClass(modalItem.sentiment)]">{{ sentimentLabel(modalItem.sentiment) }}</span>
            </div>
          <div class="modal-body">
            <p class="full-text">{{ modalItem.summary || modalItem.description || 'No content available.' }}</p>
            <div v-if="computedTags(modalItem).length" class="modal-tags">
              <span v-for="t in computedTags(modalItem)" :key="t" class="tag">{{ t }}</span>
            </div>
            <div class="modal-actions">
              <a v-if="modalItem.url" :href="modalItem.url" target="_blank" class="btn small">Open Original</a>
              <button class="btn outline small" @click="includeInSummary(modalItem)">Boost Summary</button>
            </div>
          </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script>
import { computed, ref } from 'vue';
export default {
  name: 'MerchantNews',
  props: {
    news: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false },
    unit: { type: String, default: 'hour' }
  },
  setup(props) {
    function formatTime(ts) {
      try { return new Date(ts).toLocaleString(undefined,{hour:'2-digit',minute:'2-digit',month:'short',day:'numeric'}); } catch { return ts; }
    }
    function sentimentLabel(s) {
      if (s == null) return 'N/A';
      if (typeof s === 'number') {
        if (s > 0.2) return 'Positive';
        if (s < -0.2) return 'Negative';
        return 'Neutral';
      }
      return String(s);
    }
    function sentimentClass(s) {
      if (typeof s !== 'number') return 'neutral';
      if (s > 0.2) return 'positive';
      if (s < -0.2) return 'negative';
      return 'neutral';
    }
  const visibleLimit = 12; // initial rows/cards before scroll
  const expanded = ref(false);
  const newsLimited = computed(()=> props.news.slice(0,60));
  const visibleNews = computed(()=> expanded.value ? newsLimited.value : newsLimited.value.slice(0, visibleLimit));

    // Layout state
    const layout = ref('grid');
    function toggleLayout(){ layout.value = layout.value === 'grid' ? 'list' : 'grid'; }

    // Modal
    const modalItem = ref(null);
    function openModal(item){ modalItem.value = item; }
    function closeModal(){ modalItem.value = null; }

    // Derived metrics
    const sentiments = computed(()=> props.news.map(n=> typeof n.sentiment==='number'? n.sentiment : null).filter(v=> v!=null));
    const avgSentiment = computed(()=> sentiments.value.length ? sentiments.value.reduce((a,b)=>a+b,0)/sentiments.value.length : 0);
    const avgSentimentLabel = computed(()=> sentimentLabel(avgSentiment.value));
    const distinctSources = computed(()=> new Set(props.news.map(n=> n.source).filter(Boolean)).size);
    const times = computed(()=> props.news.map(n=> new Date(n.published_at||n.time||n.created_at||Date.now()).getTime()).sort((a,b)=>a-b));
    const spanRange = computed(()=> times.value.length? formatSpan(times.value[0], times.value[times.value.length-1]) : '—');

    function formatSpan(a,b){
      const diffMs = b-a; if(diffMs<=0) return '≤1m';
      const diffH = diffMs/3600000; if(diffH<24) return diffH.toFixed(1)+'h';
      const diffD = diffH/24; return diffD.toFixed(1)+'d';
    }

    const sentimentDist = computed(()=>{
      let pos=0,neg=0,neu=0; sentiments.value.forEach(s=>{ if(s>0.2) pos++; else if(s<-0.2) neg++; else neu++; });
      const total = pos+neg+neu;
      return { pos, neg, neu, total, posPct: total? (pos/total*100).toFixed(1):0, neuPct: total?(neu/total*100).toFixed(1):0, negPct: total?(neg/total*100).toFixed(1):0 };
    });

    // Keywords extraction (simple)
    const stop = new Set(['the','a','an','for','and','or','of','to','in','on','by','with','at','from','is','are','be','as','vs','its','their','it','this','that','into','over','after','amid','new']);
    function extractWords(text){
      return (text||'').toLowerCase().replace(/[^a-z0-9\s]/g,' ').split(/\s+/).filter(w=> w && !stop.has(w) && w.length>2);
    }
    const keywordMap = computed(()=>{
      const m = new Map();
      props.news.forEach(n=>{
        const words = extractWords((n.title||'')+' '+(n.summary||n.description||''));
        words.forEach(w=> m.set(w,(m.get(w)||0)+1));
      });
      return m;
    });
    const topKeywords = computed(()=> Array.from(keywordMap.value.entries()).sort((a,b)=>b[1]-a[1]).slice(0,12).map(([word,count])=>({word,count})) );

    // Summarization (heuristic placeholder)
    const summarizing = ref(false);
    const summaryText = ref('');
    const summaryPoints = ref([]);
    const summaryExpanded = ref(true);
    const summaryMeta = ref({ keywords: [] });

    function generateSummary(){
      if(!props.news.length) return;
      summarizing.value = true;
      setTimeout(()=>{ // simulate async / placeholder for real AI call
        const articles = props.news.slice(0,80);
        const allText = articles.map(a=> (a.title||'')+'. '+(a.summary||a.description||'')).join(' ');
        const words = extractWords(allText);
        const freq = {};
        words.forEach(w=> freq[w]=(freq[w]||0)+1);
        const top = Object.entries(freq).sort((a,b)=>b[1]-a[1]).slice(0,8).map(([w])=>w);
        const clusters = top.map(k=>({k, items: articles.filter(a=> ((a.title||'')+(a.summary||a.description||'')).toLowerCase().includes(k))}));
        const pts = clusters.filter(c=>c.items.length).map(c=> `${c.k}: ${c.items.length} related update${c.items.length>1?'s':''}`);
        if(!pts.length) pts.push('General: No dominant themes extracted.');
        summaryPoints.value = pts;
        summaryMeta.value.keywords = top;
        summaryText.value = pts.join('\n');
        summarizing.value = false;
      }, 400);
    }
    function copySummary(){
      if(!summaryText.value) return;
      try { navigator.clipboard.writeText(summaryText.value); } catch {}
    }
    function includeInSummary(item){
      // Add item top words to meta and regenerate quickly
      const words = extractWords((item.title||'')+' '+(item.summary||item.description||''));
      summaryMeta.value.keywords = Array.from(new Set([...(summaryMeta.value.keywords||[]), ...words.slice(0,5)])).slice(0,12);
      if(!summaryText.value) generateSummary();
    }

    // Utilities
    function truncated(n){
      const txt = (n.summary || n.description || '').trim();
      return txt.length>200 ? txt.slice(0,200)+'…' : txt || 'No snippet available.';
    }
    function computedTags(n){
      const base = [];
      if(n.category) base.push(n.category);
      if(Array.isArray(n.tags)) base.push(...n.tags);
      return base;
    }
    function relativeTime(ts){
      try { const d = new Date(ts).getTime(); const diff = Date.now()-d; if(diff<60000) return 'just now'; if(diff<3600000) return Math.floor(diff/60000)+'m ago'; if(diff<86400000) return Math.floor(diff/3600000)+'h ago'; return Math.floor(diff/86400000)+'d ago'; } catch { return ts; }
    }

  return { formatTime, sentimentLabel, sentimentClass, newsLimited, visibleNews, visibleLimit, expanded, layout, toggleLayout, modalItem, openModal, closeModal, avgSentiment, avgSentimentLabel, distinctSources, spanRange, sentimentDist, topKeywords, summarizing, summaryText, summaryPoints, summaryExpanded, summaryMeta, generateSummary, copySummary, includeInSummary, truncated, computedTags, relativeTime };
  }
};
</script>

<style scoped>
.news-wrapper { display:flex; flex-direction:column; gap:18px; }
.toolbar-card { background:#ffffff; border:1px solid #e2e8f0; border-radius:14px; padding:14px 18px; box-shadow:0 2px 6px rgba(0,0,0,0.04); display:flex; flex-direction:column; gap:12px; }
.toolbar-top { display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap; }
.section-title { margin:0; font-size:16px; font-weight:700; color:#0f766e; letter-spacing:.5px; }
.toolbar-actions { display:flex; gap:8px; }
.btn { background:#0d9488; color:#fff; border:none; padding:8px 14px; border-radius:8px; font-size:13px; font-weight:600; cursor:pointer; display:inline-flex; align-items:center; gap:6px; }
.btn.small { padding:6px 10px; font-size:12px; }
.btn.outline { background:#ffffff; color:#0d9488; border:1px solid #0d9488; }
.btn.outline:hover { background:#0d9488; color:#ffffff; }
.btn:disabled { opacity:.4; cursor:not-allowed; }
.metrics { display:grid; gap:10px; grid-template-columns:repeat(auto-fit,minmax(110px,1fr)); }
.metric { background:#f0fdfa; border:1px solid #a7f3d0; padding:8px 10px; border-radius:10px; display:flex; flex-direction:column; gap:4px; }
.metric .m-label { font-size:10px; font-weight:700; color:#0f766e; letter-spacing:.6px; }
.metric .m-value { font-size:14px; font-weight:700; color:#0f172a; }
.metric.dist { grid-column: span 2; }
.dist-bar { display:flex; width:100%; height:10px; border-radius:6px; overflow:hidden; box-shadow:inset 0 0 0 1px rgba(0,0,0,0.06); }
.dist-bar .pos { background:#059669; }
.dist-bar .neu { background:#6b7280; }
.dist-bar .neg { background:#b91c1c; }
.dist-legend { font-size:10px; color:#475569; margin-top:4px; font-weight:600; }
.keywords-row { display:flex; flex-wrap:wrap; gap:6px; }
.kw-chip { background:#ecfeff; color:#155e75; padding:4px 10px; border-radius:20px; font-size:11px; font-weight:600; border:1px solid #67e8f9; }

/* Summary */
.summary-card { background:#ffffff; border:1px solid #e2e8f0; border-radius:14px; padding:14px 16px; box-shadow:0 4px 14px -2px rgba(0,0,0,.05); }
.summary-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; }
.summary-header h5 { margin:0; font-size:14px; font-weight:700; color:#0f766e; }
.link-btn { background:none; border:none; color:#0d9488; font-size:12px; font-weight:600; cursor:pointer; }
.summary-body { display:flex; flex-direction:column; gap:10px; }
.summary-points { margin:0; padding-left:18px; display:flex; flex-direction:column; gap:4px; }
.summary-points li { font-size:13px; line-height:1.3; color:#1e293b; }
.summary-tags { display:flex; gap:6px; flex-wrap:wrap; }
.tag.keyword { background:#f0fdfa; color:#0f766e; border:1px solid #99f6e4; }

/* Feed */
.feed-container.grid { display:grid; gap:16px; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); }
.feed-container.list { display:flex; flex-direction:column; gap:14px; }
.feed-wrapper { display:flex; flex-direction:column; gap:8px; }
.feed-header-mini { display:flex; justify-content:flex-end; }
.mini-toggle { background:#ffffff; border:1px solid #0d9488; color:#0d9488; font-size:11px; font-weight:600; padding:4px 10px; border-radius:14px; cursor:pointer; }
.mini-toggle:hover { background:#0d9488; color:#ffffff; }
.feed-container.collapsed { max-height: 420px; overflow-y: auto; padding-right:4px; }
.feed-container.expanded { max-height: none; }
.feed-container.collapsed::-webkit-scrollbar { width:6px; }
.feed-container.collapsed::-webkit-scrollbar-track { background:transparent; }
.feed-container.collapsed::-webkit-scrollbar-thumb { background:#cbd5e1; border-radius:4px; }
.news-card { cursor:pointer; background:#f9fafb; border:1px solid #e2e8f0; border-radius:12px; padding:14px 16px; display:flex; flex-direction:column; gap:10px; position:relative; transition:.25s; }
.news-card:hover { background:#ffffff; box-shadow:0 6px 18px -4px rgba(0,0,0,0.12); transform:translateY(-2px); }
.news-card.positive { border-color:#34d39955; }
.news-card.neutral { border-color:#94a3b855; }
.news-card.negative { border-color:#f8717160; }
.card-top { display:flex; flex-direction:column; gap:6px; }
.headline { font-size:15px; font-weight:700; color:#0f172a; line-height:1.25; display:-webkit-box; -webkit-line-clamp:2; line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }
.meta-row { display:flex; gap:8px; font-size:11px; color:#64748b; font-weight:600; flex-wrap:wrap; }
.snippet { font-size:13px; color:#334155; line-height:1.35; min-height:40px; }
.tags { display:flex; flex-wrap:wrap; gap:6px; }
.tag { background:#ecfeff; color:#047481; border:1px solid #a5f3fc; font-size:10px; padding:3px 8px; border-radius:14px; font-weight:600; letter-spacing:.3px; }
.sentiment-pill { font-size:10px; font-weight:700; padding:4px 8px; border-radius:14px; background:#cbd5e1; color:#1e293b; text-transform:uppercase; letter-spacing:.5px; }
.sentiment-pill.positive { background:#059669; color:#ffffff; }
.sentiment-pill.neutral { background:#64748b; color:#ffffff; }
.sentiment-pill.negative { background:#b91c1c; color:#ffffff; }
.footer-row { display:flex; align-items:center; justify-content:space-between; }
.open-link { font-size:12px; color:#0d9488; font-weight:600; text-decoration:none; }
.open-link:hover { text-decoration:underline; }

/* Modal */
.modal-overlay { position:fixed; inset:0; background:rgba(15,23,42,0.55); backdrop-filter:blur(4px); display:flex; align-items:center; justify-content:center; z-index:400; padding:20px; }
.modal { width:min(760px,100%); background:#ffffff; border-radius:18px; border:1px solid #e2e8f0; box-shadow:0 18px 40px -10px rgba(0,0,0,0.3); display:flex; flex-direction:column; max-height:90vh; }
.modal-header { padding:18px 22px; border-bottom:1px solid #e2e8f0; display:flex; justify-content:space-between; align-items:flex-start; gap:18px; }
.modal-header h4 { margin:0; font-size:18px; line-height:1.25; color:#0f172a; }
.close { background:none; border:none; font-size:26px; line-height:1; cursor:pointer; color:#64748b; }
.close:hover { color:#0f172a; }
.modal-meta { display:flex; flex-wrap:wrap; gap:10px; font-size:12px; color:#475569; font-weight:600; padding:12px 22px 0; }
.modal-body { padding:12px 22px 24px; overflow-y:auto; display:flex; flex-direction:column; gap:18px; }
.full-text { font-size:14px; line-height:1.45; color:#1e293b; margin:0; }
.modal-tags { display:flex; gap:6px; flex-wrap:wrap; }
.modal-actions { display:flex; gap:10px; flex-wrap:wrap; }

/* Helper */
.loading { font-size:13px; color:#6b7280; }
.empty { font-size:13px; color:#9ca3af; font-style:italic; }

/* Chips / Tags reuse variations */
.news-card .tag { background:#f0fdfa; border-color:#99f6e4; color:#0f766e; }
.news-card .tag:hover { background:#ccfbf188; }

/* Animations */
.fade-enter-active, .fade-leave-active { transition:opacity .25s ease; }
.fade-enter-from, .fade-leave-to { opacity:0; }

@media (max-width:640px){
  .metrics { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .metric.dist { grid-column:1 / -1; }
  .feed-container.grid { grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); }
  .headline { -webkit-line-clamp:3; line-clamp:3; }
}
</style>
