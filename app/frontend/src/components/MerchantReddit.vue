<template>
  <section class="tw-container">
    <header class="tw-header">
      <div class="tw-title">
        <h3>Reddit Analytics</h3>
        <span class="tw-sub">Granularity: {{ unitLabel }}</span>
      </div>

      <div class="tw-kpis">
        <div class="kpi">
          <div class="kpi-label">Posts</div>
          <div class="kpi-value">{{ totalPosts }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Unique Authors</div>
          <div class="kpi-value">{{ uniqueAuthors }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Likes</div>
          <div class="kpi-value">{{ formatNumber(sumLikes) }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Comments</div>
          <div class="kpi-value">{{ formatNumber(sumComments) }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Impressions</div>
          <div class="kpi-value">{{ formatNumber(sumImpressions) }}</div>
        </div>
      </div>
    </header>

    <div v-if="loading" class="tw-refresh">Refreshing… (previous data shown)</div>
    <div v-else-if="!normalizedPosts.length" class="tw-empty">No posts in this range.</div>
      <section class="tw-charts">
        <!-- NEW: Throughput & Sentiment Trend -->
        <div class="chart-card">
          <div class="chart-title">Throughput & Sentiment Trend</div>
          <div v-if="chartJsLoaded" class="chart-wrap" style="height: 240px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="throughputCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Avg Sentiment / Count not available (Chart.js missing).
          </div>
        </div>

        <div class="chart-card">
          <div class="chart-title">Activity timeline (stacked by sentiment)</div>
          <div v-if="chartJsLoaded" class="chart-wrap" style="height: 240px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="timelineCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Chart.js not available — showing summary only.
          </div>
        </div>

        <div class="chart-card">
          <div class="chart-title">Sentiment mix</div>
          <div v-if="chartJsLoaded" class="chart-wrap" style="height: 240px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="sentimentCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Positive: {{ sentimentCounts.positive }} —
            Neutral: {{ sentimentCounts.neutral }} —
            Negative: {{ sentimentCounts.negative }}
          </div>
        </div>

        <div class="chart-card">
          <div class="chart-title">Engagement totals</div>
          <div v-if="chartJsLoaded" class="chart-wrap" style="height: 240px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="engagementCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Likes: {{ formatNumber(sumLikes) }},
            Comments: {{ formatNumber(sumComments) }},
            Impressions: {{ formatNumber(sumImpressions) }}
          </div>
        </div>
      </section>

      <section class="tw-meta">
        <div class="meta-card">
          <div class="meta-title">Top Languages</div>
          <div class="meta-list">
            <div v-for="(v, k) in topLanguages" :key="k" class="meta-row">
              <span class="meta-key">{{ k }}</span>
              <span class="meta-val">{{ v }}</span>
            </div>
          </div>
        </div>

        <div class="meta-card" style="display:grid;gap:10px;">
          <div class="meta-title">Insights</div>
          <div style="font-size:12px;line-height:1.4;display:grid;gap:6px;">
            <div><strong>Peak interval:</strong> {{ peakInterval?.label || '-' }} <span v-if="peakInterval">({{ peakInterval.count }} posts)</span></div>
            <div><strong>Avg sentiment:</strong> <span :class="avgSentLabel">{{ avgSentLabel }}</span> <small v-if="avgSentIndex!=null" style="color:#6b7280;">({{ avgSentIndex.toFixed(2) }})</small></div>
            <div><strong>Engagement / post:</strong> {{ avgEngagement }}</div>
            <div><strong>Top language:</strong> {{ firstTopLanguage }}</div>
          </div>
          <div style="display:flex;flex-wrap:wrap;gap:6px;">
            <button type="button" @click="setSentimentFilter('all')" :class="['flt-chip', sentimentFilter==='all' && 'on']">All</button>
            <button type="button" @click="setSentimentFilter('positive')" :class="['flt-chip','positive', sentimentFilter==='positive' && 'on']">Positive</button>
            <button type="button" @click="setSentimentFilter('neutral')" :class="['flt-chip','neutral', sentimentFilter==='neutral' && 'on']">Neutral</button>
            <button type="button" @click="setSentimentFilter('negative')" :class="['flt-chip','negative', sentimentFilter==='negative' && 'on']">Negative</button>
          </div>
        </div>
      </section>

  <section class="tw-table" style="max-height: 40vh; overflow: auto;">
        <div class="table-header">
          <div class="table-title">Top Posts (by engagement score)</div>
          <input class="search" v-model="search" placeholder="Search content..." />
        </div>
        <table class="table">
          <thead>
            <tr>
              <th style="width:180px;">Time</th>
              <th>Content</th>
              <th style="width:110px;">Subreddit</th>
              <th style="width:120px;">Sentiment</th>
              <th style="width:80px;">Likes</th>
              <th style="width:90px;">Comments</th>
              <th style="width:110px;">Impressions</th>
              <th style="width:110px;">Score</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="t in filteredTopPosts" :key="t.post_id">
              <td>{{ formatDate(t.dt) }}</td>
              <td class="content-cell">{{ t.content }}</td>
              <td>{{ t.subreddit || '-' }}</td>
              <td>
                <span :class="['pill', t.sentiment_label || 'neutral']">
                  {{ (t.sentiment_label || 'neutral') }}
                </span>
              </td>
              <td>{{ t.public_metrics.like_count || 0 }}</td>
              <td>{{ t.public_metrics.comment_count || 0 }}</td>
              <td>{{ t.non_public_metrics?.impression_count || 0 }}</td>
              <td>{{ Math.round(t.__score) }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    
  </section>
</template>

<script>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from "vue";

export default {
  name: "MerchantReddit",
  props: {
    posts: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false },
    unit: { type: String, default: "hour" },
  },
  setup(props) {
    // Chart.js glue
    let Chart = null;
    const chartJsLoaded = ref(false);
    const chartsBuilding = ref(true);

    const timelineCanvas = ref(null);
    const sentimentCanvas = ref(null);
    const engagementCanvas = ref(null);
    const throughputCanvas = ref(null);

  let timelineChart = null;
  let sentimentChart = null;
  let engagementChart = null;
  let throughputChart = null;
  let resizeObs = null;

    const normalizedPosts = computed(() => {
      const rows = [];
      for (const raw of (props.posts || [])) {
        try {
          // Resolve datetime
          let dt = raw.dt || raw.created_at || raw.created || raw.createdAt || raw.timestamp || raw.time || null;
          if (raw.created_utc) dt = new Date(raw.created_utc * 1000).toISOString();
          if (!dt) continue;
          // Combine title + body for richer content if present
            const title = raw.title || "";
            const body = raw.selftext || raw.body || raw.text || raw.content || "";
            let content = raw.content || raw.text || `${title}${title && body ? " — " : ""}${body}`.trim();
            if (!content && title) content = title;
          // Metrics mapping (Reddit common fields: ups, score, num_comments)
          const likes =
            raw.like_count ?? raw.likes ?? raw.ups ?? raw.upvotes ?? raw.score ?? raw.public_metrics?.like_count ?? 0;
          const comments =
            raw.comment_count ?? raw.comments ?? raw.num_comments ?? raw.public_metrics?.comment_count ?? 0;
          const impressions =
            raw.impression_count ?? raw.non_public_metrics?.impression_count ?? Math.round((likes + comments) * 3.7);
          // Sentiment mapping / fallback
          let sentimentLabel = raw.sentiment_label || raw.sentiment || raw.sentimentLabel || null;
          if (sentimentLabel) sentimentLabel = String(sentimentLabel).toLowerCase();
          const sentimentScore = raw.sentiment_score ?? raw.sentimentScore ?? raw.score_sentiment ?? null;
          if (!sentimentLabel && typeof sentimentScore === 'number') {
            sentimentLabel = sentimentScore > 0.2 ? 'positive' : (sentimentScore < -0.2 ? 'negative' : 'neutral');
          }
          if (!['positive','neutral','negative'].includes(sentimentLabel)) sentimentLabel = 'neutral';
          const lang = raw.lang || raw.language || raw.locale || 'UNK';
          const subreddit = raw.subreddit || raw.subreddit_name || raw.sub || '';
          const author_id = raw.author_id || raw.author || raw.username || '';
          const postId = raw.post_id || raw.id || raw.reddit_id || raw.tweet_id || Math.random().toString(36).slice(2);

          rows.push({
            post_id: postId,
            author_id,
            subreddit,
            dt,
            content,
            lang: lang || 'UNK',
            sentiment_label: sentimentLabel,
            sentiment_score: sentimentScore,
            public_metrics: {
              like_count: Number(likes) || 0,
              comment_count: Number(comments) || 0,
              share_count: raw.public_metrics?.share_count ?? raw.share_count ?? 0,
            },
            non_public_metrics: {
              impression_count: Number(impressions) || 0,
              url_clicks: raw.url_clicks || raw.non_public_metrics?.url_link_clicks || 0,
              user_profile_clicks: raw.user_profile_clicks || raw.non_public_metrics?.user_profile_clicks || 0,
            },
          });
        } catch {
          // skip bad row
        }
      }
      return rows;
    });

    const totalPosts = computed(() => normalizedPosts.value.length);
    const uniqueAuthors = computed(() => {
      const s = new Set();
      for (const p of normalizedPosts.value) if (p.author_id) s.add(p.author_id);
      return s.size;
    });
    const sumLikes = computed(() => normalizedPosts.value.reduce((a, p) => a + (p.public_metrics.like_count || 0), 0));
    const sumComments = computed(() =>
      normalizedPosts.value.reduce((a, p) => a + (p.public_metrics.comment_count || 0), 0)
    );
    const sumImpressions = computed(() => normalizedPosts.value.reduce((a, p) => a + (p.non_public_metrics.impression_count || 0), 0));

    const sentimentCounts = computed(() => {
      const out = { positive: 0, neutral: 0, negative: 0 };
      for (const p of normalizedPosts.value) {
        out[p.sentiment_label] = (out[p.sentiment_label] || 0) + 1;
      }
      return out;
    });

    const topLanguages = computed(() => {
      const map = new Map();
      for (const p of normalizedPosts.value) {
        const k = (p.lang || "UNK").toUpperCase();
        map.set(k, (map.get(k) || 0) + 1);
      }
      const entries = Array.from(map.entries()).sort((a, b) => b[1] - a[1]).slice(0, 6);
      const obj = {};
      for (const [k, v] of entries) obj[k] = v;
      return obj;
    });

    const topPosts = computed(() => {
      const arr = normalizedPosts.value.map((p) => {
        const likes = p.public_metrics?.like_count || 0;
        const comments = p.public_metrics?.comment_count || 0;
        const impressions = p.non_public_metrics?.impression_count || 0;
        // Weighted engagement heuristic: Reddit often values comments more heavily
        const baseScore = likes * 1.5 + comments * 3 + impressions * 0.0008;
        // Sentiment slight boost / penalty
        const sentimentBoost = p.sentiment_label === 'positive' ? 1.08 : (p.sentiment_label === 'negative' ? 0.95 : 1);
        return { ...p, __score: baseScore * sentimentBoost };
      });
      return arr.sort((a,b)=>b.__score - a.__score).slice(0, 200);
    });

    const search = ref("");
    const sentimentFilter = ref('all');
    const filteredTopPosts = computed(() => {
      const q = search.value.trim().toLowerCase();
      let base = topPosts.value;
      if (sentimentFilter.value !== 'all') {
        base = base.filter(p => (p.sentiment_label||'neutral') === sentimentFilter.value);
      }
      if (!q) return base.slice(0,50);
      return base.filter(p => {
        const blob = [p.content, p.post_id, p.subreddit, p.author_id, p.sentiment_label, p.lang]
          .filter(Boolean).join(' ').toLowerCase();
        return blob.includes(q);
      }).slice(0,50);
    });
    function setSentimentFilter(v){ sentimentFilter.value = v; }

    function formatNumber(n) {
      const x = Number(n || 0);
      if (x >= 1e9) return (x / 1e9).toFixed(2) + "B";
      if (x >= 1e6) return (x / 1e6).toFixed(2) + "M";
      if (x >= 1e3) return (x / 1e3).toFixed(1) + "k";
      return String(x);
    }
    function formatDate(iso) {
      if (!iso) return "-";
      try {
        return new Date(iso).toLocaleString();
      } catch {
        return iso;
      }
    }

    const unitLabel = computed(() => {
      const u = (props.unit || "").toLowerCase();
      if (u === "hour") return "hourly";
      if (u === "week") return "weekly";
      return "daily";
    });

    // Chart helpers
    async function ensureChartJs() {
      if (Chart) return;
      try {
        const m = await import("chart.js/auto");
        Chart = m.default || m;
        chartJsLoaded.value = true;
      } catch (e) {
        console.warn("Chart.js not installed. Charts will be hidden.", e);
        chartJsLoaded.value = false;
      }
    }

    function destroyCharts() {
      [timelineChart, sentimentChart, engagementChart, throughputChart].forEach((ch) => {
        if (ch) { try { ch.destroy(); } catch(_){} }
      });
      timelineChart = sentimentChart = engagementChart = throughputChart = null;
    }

    // Timeline batching & aggregated sentiment per bucket
    const timelineStack = computed(() => {
      const unit = (props.unit || "hour").toLowerCase();
      const buckets = new Map();
      for (const p of normalizedPosts.value) {
        const d = new Date(p.dt);
        let bucket;
        if (unit === "hour") {
          bucket = new Date(d);
          bucket.setMinutes(0, 0, 0);
        } else if (unit === "week") {
          const x = new Date(d);
            const day = x.getUTCDay();
            const diff = (day === 0 ? -6 : 1 - day);
            x.setDate(x.getDate() + diff);
            x.setHours(0, 0, 0, 0);
            bucket = x;
        } else {
          bucket = new Date(d);
          bucket.setHours(0, 0, 0, 0);
        }
        const key = bucket.toISOString();
        if (!buckets.has(key)) buckets.set(key, { neg: 0, neu: 0, pos: 0, total: 0, dt: bucket });
        const slot = buckets.get(key);
        const lab = p.sentiment_label;
        if (lab === 'positive') slot.pos++;
        else if (lab === 'negative') slot.neg++;
        else slot.neu++;
        slot.total++;
      }
      const rows = Array.from(buckets.values()).sort((a,b)=>a.dt - b.dt);
      const labels = rows.map(r => r.dt.toLocaleString());
      const neg = rows.map(r => r.neg);
      const neu = rows.map(r => r.neu);
      const pos = rows.map(r => r.pos);
      const total = rows.map(r => r.total);
      const avgSent = rows.map(r => {
        if (!r.total) return 0;
        // crude sentiment average: (pos - neg) / total
        return (r.pos - r.neg) / r.total;
      });
      return { labels, neg, neu, pos, total, avgSent };
    });

    // Throughput (counts + avg sentiment paired with timeline buckets)
    const throughputData = computed(() => {
      const tl = timelineStack.value;
      return {
        labels: tl.labels,
        counts: tl.total,
        avgSent: tl.avgSent,
      };
    });

    // Chart builder
    function buildOrUpdateCharts() {
      if (!chartJsLoaded.value) return;

      const th = throughputData.value;
      const tl = timelineStack.value;

      // Timeline (stacked by sentiment)
      if (!timelineChart && timelineCanvas.value) {
        timelineChart = new Chart(timelineCanvas.value.getContext("2d"), {
          type: "bar",
          data: {
            labels: tl.labels,
            datasets: [
              { label: "Negative", data: tl.neg, backgroundColor: "#ef4444", stack: "s" },
              { label: "Neutral", data: tl.neu, backgroundColor: "#9ca3af", stack: "s" },
              { label: "Positive", data: tl.pos, backgroundColor: "#10b981", stack: "s" },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: "bottom" } },
            scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } },
          },
        });
      } else if (timelineChart) {
        timelineChart.options.animation = false;
        timelineChart.data.labels = tl.labels;
        timelineChart.data.datasets[0].data = tl.neg;
        timelineChart.data.datasets[1].data = tl.neu;
        timelineChart.data.datasets[2].data = tl.pos;
        timelineChart.update();
      }

      // Sentiment doughnut
      const sc = sentimentCounts.value;
      if (!sentimentChart && sentimentCanvas.value) {
        sentimentChart = new Chart(sentimentCanvas.value.getContext("2d"), {
          type: "doughnut",
          data: {
            labels: ["Positive", "Neutral", "Negative"],
            datasets: [{ data: [sc.positive, sc.neutral, sc.negative], backgroundColor: ["#10b981", "#9ca3af", "#ef4444"] }],
          },
          options: { plugins: { legend: { position: "bottom" } } },
        });
      } else if (sentimentChart) {
        sentimentChart.options.animation = false;
        sentimentChart.data.datasets[0].data = [sentimentCounts.value.positive, sentimentCounts.value.neutral, sentimentCounts.value.negative];
        sentimentChart.update();
      }

      // Engagement totals
      const totals = [sumLikes.value, sumComments.value, sumImpressions.value];
      if (!engagementChart && engagementCanvas.value) {
        engagementChart = new Chart(engagementCanvas.value.getContext("2d"), {
          type: "bar",
          data: {
            labels: ["Likes", "Comments", "Impressions"],
            datasets: [{ label: "Total", data: totals, backgroundColor: "#3b82f6" }],
          },
          options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true } },
          },
        });
      } else if (engagementChart) {
        engagementChart.options.animation = false;
        engagementChart.data.datasets[0].data = totals;
        engagementChart.update();
      }

      // Throughput line: tweets count + avg sentiment
      if (!throughputChart && throughputCanvas.value) {
        throughputChart = new Chart(throughputCanvas.value.getContext("2d"), {
          data: {
            labels: th.labels,
            datasets: [
              {
                type: "bar",
                label: "Post Count",
                data: th.counts,
                backgroundColor: "#6366f1",
                yAxisID: "yCount",
              },
              {
                type: "line",
                label: "Avg Sentiment",
                data: th.avgSent,
                borderColor: "#10b981",
                backgroundColor: "rgba(16,185,129,0.15)",
                tension: 0.25,
                yAxisID: "ySent",
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: "index", intersect: false },
            plugins: {
              legend: { position: "bottom" },
              tooltip: {
                callbacks: {
                  label(ctx) {
                    if (ctx.dataset.label === "Avg Sentiment") return `Avg Sentiment: ${ctx.parsed.y.toFixed(2)}`;
                    return `Count: ${ctx.parsed.y}`;
                  },
                },
              },
            },
            scales: {
              yCount: { position: "left", beginAtZero: true, title: { display: true, text: "Count" } },
              ySent: {
                position: "right",
                min: -1,
                max: 1,
                grid: { drawOnChartArea: false },
                title: { display: true, text: "Avg Sentiment" },
              },
            },
          },
        });
      } else if (throughputChart) {
        throughputChart.options.animation = false;
        throughputChart.data.labels = th.labels;
        throughputChart.data.datasets[0].data = th.counts;
        throughputChart.data.datasets[1].data = th.avgSent;
        throughputChart.update();
      }

      // Layout: end loading after frame
      requestAnimationFrame(() => { chartsBuilding.value = false; });
    }

    // Watchers
    onMounted(async () => {
      await ensureChartJs();
      await nextTick();
      requestAnimationFrame(() => { buildOrUpdateCharts(); });
      try {
        resizeObs = new ResizeObserver(() => {
          [timelineChart, sentimentChart, engagementChart, throughputChart].forEach(ch => { if (ch) ch.resize(); });
        });
        const host = timelineCanvas.value?.parentElement?.parentElement; // chart-wrap
        if (host) resizeObs.observe(host);
      } catch(_){}
    });

  onBeforeUnmount(() => { if (resizeObs) try { resizeObs.disconnect(); } catch(_){} destroyCharts(); });

    // React to data changes
    watch([normalizedPosts, () => props.unit], async () => {
      if (!Chart) await ensureChartJs();
      await nextTick();
      if (!normalizedPosts.value.length) {
        // Clear data but keep instances
        [timelineChart, sentimentChart, engagementChart, throughputChart].forEach(ch => {
          if (ch) {
            ch.options.animation = false;
            ch.data.labels = [];
            ch.data.datasets.forEach(ds => ds.data = []);
            ch.update();
          }
        });
        chartsBuilding.value = false;
        return;
      }
      chartsBuilding.value = true;
      buildOrUpdateCharts();
    }, { deep: true });

    // Insights computations
    const peakInterval = computed(() => {
      const tl = timelineStack.value; if (!tl.total?.length) return null;
      let max=-1, idx=-1; tl.total.forEach((v,i)=>{ if(v>max){max=v;idx=i;} });
      return idx>=0 ? { label: tl.labels[idx], count: tl.total[idx] } : null;
    });
    const avgSentIndex = computed(() => {
      const total = sentimentCounts.value.positive + sentimentCounts.value.neutral + sentimentCounts.value.negative;
      if (!total) return 0;
      return (sentimentCounts.value.positive - sentimentCounts.value.negative)/total;
    });
    const avgSentLabel = computed(() => {
      const v = avgSentIndex.value;
      if (v>0.15) return 'Positive'; if (v<-0.15) return 'Negative'; return 'Neutral';
    });
    const avgEngagement = computed(() => {
      const total = totalPosts.value || 1;
      const engagement = (sumLikes.value + sumComments.value + sumImpressions.value*0.001)/total;
      return Math.round(engagement);
    });
    const firstTopLanguage = computed(()=> Object.keys(topLanguages.value)[0] || '-');

    // (Legacy) Export helpers retained though not surfaced in UI now
    function toCsv(rows) {
      if (!rows?.length) return "post_id,dt,author_id,subreddit,lang,sentiment,content,likes,comments,impressions\n";
      const head = ["post_id","dt","author_id","subreddit","lang","sentiment","content","likes","comments","impressions"];
      const lines = [head.join(",")];
      for (const r of rows) {
        const p = r.public_metrics || {};
        const n = r.non_public_metrics || {};
        const fields = [
          r.post_id, r.dt, r.author_id, r.subreddit || '', r.lang, r.sentiment_label,
          (r.content || "").replace(/\n/g, " ").replace(/"/g, '""'),
          p.like_count || 0, p.comment_count || 0, n.impression_count || 0,
        ];
        const safe = fields.map((v, i) => typeof v === "string" && i < 6 ? `"${v}"` : v);
        lines.push(safe.join(","));
      }
      return lines.join("\n");
    }

    async function copyJson() {
      try {
        await navigator.clipboard.writeText(JSON.stringify(normalizedPosts.value, null, 2));
      } catch {
        const blob = new Blob([JSON.stringify(normalizedPosts.value, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "reddit_posts.json";
        a.click();
        URL.revokeObjectURL(url);
      }
    }

    async function downloadCsv() {
      const csv = toCsv(topPosts.value);
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "reddit_posts.csv";
      a.click();
      URL.revokeObjectURL(url);
    }

    const copyJsonRef = () => copyJson();
    const downloadCsvRef = () => downloadCsv();

    return {
      unitLabel,
      chartJsLoaded,
      chartsBuilding,
      timelineCanvas,
      sentimentCanvas,
      engagementCanvas,
      throughputCanvas,
      totalPosts,
      uniqueAuthors,
      sentimentCounts,
      sumLikes,
      sumComments,
      sumImpressions,
      topLanguages,
      topPosts,
      filteredTopPosts,
      search,
      sentimentFilter,
      setSentimentFilter,
      formatNumber,
      formatDate,
      copyJson: copyJsonRef,
      downloadCsv: downloadCsvRef,
      normalizedPosts,
      peakInterval,
      avgSentIndex,
      avgSentLabel,
      avgEngagement,
      firstTopLanguage,
    };
  },
};
</script>

<style scoped>
.tw-container { display:grid; gap:16px; }
.tw-header { display:grid; gap:10px; }
.tw-title { display:flex; align-items:baseline; gap:10px; }
.tw-title h3 { margin:0; color:#0f766e; font-size:16px; font-weight:700; }
.tw-sub { color:#6b7280; font-size:12px; }
.tw-kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:8px; }
.kpi { background:#f9fafb; border:1px solid #e5e7eb; border-radius:10px; padding:10px; display:grid; gap:6px; }
.kpi-label { color:#6b7280; font-size:12px; }
.kpi-value { color:#111827; font-weight:700; font-size:18px; }

.tw-loading, .tw-empty { padding:14px; background:#f8fafc; border:1px dashed #d1d5db; border-radius:8px; color:#374151; }
.tw-refresh { padding:6px 10px; font-size:12px; color:#0f766e; font-weight:600; }
.tw-charts { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; }
.chart-card { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:12px; display:grid; gap:10px; }
.chart-title { color:#0f766e; font-size:14px; font-weight:700; }
.chart-wrap { position:relative; height:260px; }
.chart-fallback { color:#6b7280; font-size:13px; }
.chart-loader {
    position:absolute; inset:0;
    display:flex; align-items:center; justify-content:center;
    background:linear-gradient(135deg,#f8fafc 0%,#eef2f7 100%);
    font-size:13px; color:#0f766e; font-weight:600;
    z-index:2;
}
.tw-meta { display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:12px; }
.meta-card { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:12px; display:grid; gap:10px; }
.meta-title { color:#0f766e; font-size:14px; font-weight:700; }
.meta-list { display:grid; gap:8px; }
.meta-row { display:flex; justify-content:space-between; gap:8px; font-size:13px; }
.meta-key { color:#6b7280; font-weight:600; }
.meta-val { color:#111827; font-weight:700; }
.meta-actions { display:flex; gap:8px; }
.btn { background:#f0fdfa; color:#0f766e; border:1px solid #14b8a6; border-radius:6px; padding:6px 10px; cursor:pointer; font-size:12px; }
.btn:hover { background:#0f766e; color:#f0fdfa; }
/* Filter chip styles */
.flt-chip { background:#f1f5f9; border:1px solid #cbd5e1; color:#334155; padding:4px 10px; border-radius:24px; font-size:11px; font-weight:600; cursor:pointer; }
.flt-chip.on { background:#0f766e; color:#f0fdfa; border-color:#0f766e; }
.flt-chip.positive { border-color:#10b981; }
.flt-chip.negative { border-color:#ef4444; }
.flt-chip.neutral { border-color:#9ca3af; }
.tw-table { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:12px; display:grid; gap:10px; }
.table-header { display:flex; align-items:center; justify-content:space-between; }
.table-title { color:#0f766e; font-size:14px; font-weight:700; }
.search { border:1px solid #e5e7eb; border-radius:6px; padding:6px 8px; font-size:13px; width:260px; }
.table { border-collapse:collapse; width:100%; font-size:13px; }
.table th,.table td { border-top:1px solid #e5e7eb; padding:8px 6px; vertical-align:top; }
.content-cell { max-width:520px; white-space:pre-wrap; word-break:break-word; }
.pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; color:white; text-transform:capitalize; }
.pill.positive { background:#10b981; }
.pill.neutral { background:#9ca3af; color:#111827; }
.pill.negative { background:#ef4444; }
</style>
