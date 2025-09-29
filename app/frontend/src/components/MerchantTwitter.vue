
<template>
  <section class="tw-container">
    <header class="tw-header">
      <div class="tw-title">
        <h3>Twitter Analytics</h3>
        <span class="tw-sub">Granularity: {{ unitLabel }}</span>
      </div>

      <div class="tw-kpis">
        <div class="kpi">
          <div class="kpi-label">Tweets</div>
          <div class="kpi-value">{{ totalTweets }}</div>
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
          <div class="kpi-label">Retweets</div>
          <div class="kpi-value">{{ formatNumber(sumRetweets) }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Impressions</div>
          <div class="kpi-value">{{ formatNumber(sumImpressions) }}</div>
        </div>
      </div>
    </header>

    <div v-if="loading" class="tw-loading">Loading tweets...</div>
    <div v-else-if="!tweets || !tweets.length" class="tw-empty">
      No tweets in this range.
    </div>
    <template v-else>
      <section class="tw-charts">
        <div class="chart-card">
          <div class="chart-title">Activity timeline (stacked by sentiment)</div>
          <div v-if="chartJsLoaded" class="chart-wrap">
            <canvas ref="timelineCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Chart.js not available — showing summary only.
          </div>
        </div>

        <div class="chart-card">
          <div class="chart-title">Sentiment mix</div>
          <div v-if="chartJsLoaded" class="chart-wrap">
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
          <div v-if="chartJsLoaded" class="chart-wrap">
            <canvas ref="engagementCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Likes: {{ formatNumber(sumLikes) }},
            Retweets: {{ formatNumber(sumRetweets) }},
            Replies: {{ formatNumber(sumReplies) }},
            Quotes: {{ formatNumber(sumQuotes) }},
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

        <div class="meta-card">
          <div class="meta-title">Export</div>
          <div class="meta-actions">
            <button class="btn" @click="copyJson">Copy JSON</button>
            <button class="btn" @click="downloadCsv">Download CSV</button>
          </div>
        </div>
      </section>

      <section class="tw-table">
        <div class="table-header">
          <div class="table-title">Top Tweets (by engagement score)</div>
          <input class="search" v-model="search" placeholder="Search content..." />
        </div>
        <table class="table">
          <thead>
            <tr>
              <th style="width:180px;">Time</th>
              <th>Content</th>
              <th style="width:120px;">Sentiment</th>
              <th style="width:80px;">Likes</th>
              <th style="width:90px;">Retweets</th>
              <th style="width:90px;">Replies</th>
              <th style="width:80px;">Quotes</th>
              <th style="width:110px;">Impressions</th>
              <th style="width:110px;">Score</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="t in filteredTopTweets" :key="t.tweet_id">
              <td>{{ formatDate(t.dt) }}</td>
              <td class="content-cell">{{ t.content }}</td>
              <td>
                <span :class="['pill', t.sentiment_label || 'neutral']">
                  {{ (t.sentiment_label || 'neutral') }}
                </span>
              </td>
              <td>{{ t.public_metrics.like_count || 0 }}</td>
              <td>{{ t.public_metrics.retweet_count || 0 }}</td>
              <td>{{ t.public_metrics.reply_count || 0 }}</td>
              <td>{{ t.public_metrics.quote_count || 0 }}</td>
              <td>{{ t.non_public_metrics?.impression_count || 0 }}</td>
              <td>{{ Math.round(t.__score) }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </template>
  </section>
</template>

<script>
import { ref, computed, watch, onMounted, onBeforeUnmount } from "vue";

export default {
  name: "MerchantTwitter",
  props: {
    tweets: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false },
    unit: { type: String, default: "hour" }, // hour | day | week
  },
  setup(props) {
    // Chart.js dynamic load (graceful fallback if not installed)
    let Chart = null;
    const chartJsLoaded = ref(false);
    const timelineCanvas = ref(null);
    const sentimentCanvas = ref(null);
    const engagementCanvas = ref(null);
    let timelineChart = null;
    let sentimentChart = null;
    let engagementChart = null;

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

    // Formatting helpers
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
        const d = new Date(iso);
        return d.toLocaleString();
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

    // Core transforms
    function parseDate(iso) {
      try {
        return new Date(iso);
      } catch {
        return null;
      }
    }
    function startOfHourUTC(d) {
      const x = new Date(d);
      x.setUTCMinutes(0, 0, 0);
      return x;
    }
    function startOfDayUTC(d) {
      const x = new Date(d);
      x.setUTCHours(0, 0, 0, 0);
      return x;
    }
    function startOfWeekUTC(d) {
      const x = startOfDayUTC(d);
      // Make Monday the first day of week
      const day = x.getUTCDay(); // 0..6 (Sun..Sat)
      const diff = (day === 0 ? -6 : 1 - day);
      x.setUTCDate(x.getUTCDate() + diff);
      return x;
    }
    function bucketStart(d, unit) {
      if (unit === "hour") return startOfHourUTC(d);
      if (unit === "week") return startOfWeekUTC(d);
      return startOfDayUTC(d);
    }
    function keyUTC(d) {
      // ISO date w/o ms for axis label purposes
      return d.toISOString().replace(".000Z", "Z");
    }

    // Sentiment mapping
    const sVal = (lab) => (lab === "positive" ? 1 : lab === "negative" ? -1 : 0);

    // Derived metrics
    const normalizedTweets = computed(() => {
      return (props.tweets || [])
        .map((t) => {
          const d = parseDate(t.dt || t.created_at);
          if (!d || isNaN(d.getTime())) return null;
          return {
            tweet_id: t.tweet_id || t.id || Math.random().toString(36).slice(2),
            author_id: t.author_id || "",
            dt: t.dt || t.created_at,
            dateObj: d,
            content: t.content || t.text || "",
            sentiment_label:
              (t.sentiment_label && String(t.sentiment_label).toLowerCase()) ||
              "neutral",
            public_metrics: {
              like_count: t?.public_metrics?.like_count || 0,
              retweet_count: t?.public_metrics?.retweet_count || 0,
              reply_count: t?.public_metrics?.reply_count || 0,
              quote_count: t?.public_metrics?.quote_count || 0,
            },
            non_public_metrics: {
              impression_count: t?.non_public_metrics?.impression_count || 0,
              url_link_clicks: t?.non_public_metrics?.url_link_clicks || 0,
              user_profile_clicks: t?.non_public_metrics?.user_profile_clicks || 0,
            },
            lang: t.lang || "UNK",
          };
        })
        .filter(Boolean);
    });

    const totalTweets = computed(() => normalizedTweets.value.length);
    const uniqueAuthors = computed(() => {
      const s = new Set();
      for (const t of normalizedTweets.value) {
        if (t.author_id) s.add(t.author_id);
      }
      return s.size;
    });

    const sentimentCounts = computed(() => {
      const out = { positive: 0, neutral: 0, negative: 0 };
      for (const t of normalizedTweets.value) {
        out[t.sentiment_label] = (out[t.sentiment_label] || 0) + 1;
      }
      return out;
    });

    const sumLikes = computed(() =>
      normalizedTweets.value.reduce((a, t) => a + (t.public_metrics.like_count || 0), 0)
    );
    const sumRetweets = computed(() =>
      normalizedTweets.value.reduce((a, t) => a + (t.public_metrics.retweet_count || 0), 0)
    );
    const sumReplies = computed(() =>
      normalizedTweets.value.reduce((a, t) => a + (t.public_metrics.reply_count || 0), 0)
    );
    const sumQuotes = computed(() =>
      normalizedTweets.value.reduce((a, t) => a + (t.public_metrics.quote_count || 0), 0)
    );
    const sumImpressions = computed(() =>
      normalizedTweets.value.reduce((a, t) => a + (t.non_public_metrics.impression_count || 0), 0)
    );

    // Languages
    const langCounts = computed(() => {
      const map = new Map();
      for (const t of normalizedTweets.value) {
        const k = (t.lang || "UNK").toUpperCase();
        map.set(k, (map.get(k) || 0) + 1);
      }
      return map;
    });
    const topLanguages = computed(() => {
      const entries = Array.from(langCounts.value.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 6);
      const obj = {};
      for (const [k, v] of entries) obj[k] = v;
      return obj;
    });

    // Timeline buckets and sentiment stacks
    const timelineStack = computed(() => {
      const unit = (props.unit || "hour").toLowerCase();
      const buckets = new Map(); // key -> { pos, neu, neg, total, dt }
      for (const t of normalizedTweets.value) {
        const b = bucketStart(t.dateObj, unit);
        const key = keyUTC(b);
        if (!buckets.has(key)) buckets.set(key, { pos: 0, neu: 0, neg: 0, total: 0, dt: b });
        const slot = buckets.get(key);
        const lab = t.sentiment_label;
        if (lab === "positive") slot.pos += 1;
        else if (lab === "negative") slot.neg += 1;
        else slot.neu += 1;
        slot.total += 1;
      }
      const rows = Array.from(buckets.values()).sort((a, b) => a.dt - b.dt);
      return {
        labels: rows.map((r) => r.dt.toLocaleString()),
        pos: rows.map((r) => r.pos),
        neu: rows.map((r) => r.neu),
        neg: rows.map((r) => r.neg),
        total: rows.map((r) => r.total),
      };
    });

    // Top tweets by engagement score
    function score(t) {
      const p = t.public_metrics || {};
      const s =
        (p.like_count || 0) * 1 +
        (p.retweet_count || 0) * 2 +
        (p.reply_count || 0) * 3 +
        (p.quote_count || 0) * 1.5;
      return s + (t.non_public_metrics?.impression_count || 0) * 0.001;
    }
    const topTweets = computed(() => {
      const arr = normalizedTweets.value.map((t) => ({ ...t, __score: score(t) }));
      arr.sort((a, b) => b.__score - a.__score);
      return arr.slice(0, 20);
    });

    // Simple search over content within top tweets table
    const search = ref("");
    const filteredTopTweets = computed(() => {
      const q = (search.value || "").trim().toLowerCase();
      if (!q) return topTweets.value;
      return topTweets.value.filter((t) => t.content?.toLowerCase().includes(q));
    });

    // CSV / JSON helpers
    function toCsv(rows) {
      if (!rows?.length) return "tweet_id,dt,author_id,lang,sentiment,content,likes,retweets,replies,quotes,impressions\n";
      const head = [
        "tweet_id",
        "dt",
        "author_id",
        "lang",
        "sentiment",
        "content",
        "likes",
        "retweets",
        "replies",
        "quotes",
        "impressions",
      ];
      const lines = [head.join(",")];
      for (const r of rows) {
        const p = r.public_metrics || {};
        const n = r.non_public_metrics || {};
        const fields = [
          r.tweet_id,
          r.dt,
          r.author_id,
          r.lang,
          r.sentiment_label,
          (r.content || "").replace(/\n/g, " ").replace(/"/g, '""'),
          p.like_count || 0,
          p.retweet_count || 0,
          p.reply_count || 0,
          p.quote_count || 0,
          n.impression_count || 0,
        ];
        // Quote fields that may contain commas
        const safe = fields.map((v, i) =>
          typeof v === "string" && i !== 6 && i !== 7 && i !== 8 && i !== 9 && i !== 10 ? `"${v}"` : v
        );
        lines.push(safe.join(","));
      }
      return lines.join("\n");
    }
    function downloadCsv() {
      const csv = toCsv(normalizedTweets.value);
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "tweets.csv";
      a.click();
      URL.revokeObjectURL(url);
    }
    async function copyJson() {
      try {
        await navigator.clipboard.writeText(JSON.stringify(normalizedTweets.value, null, 2));
      } catch {
        // fallback
        const blob = new Blob([JSON.stringify(normalizedTweets.value, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "tweets.json";
        a.click();
        URL.revokeObjectURL(url);
      }
    }

    // Charts
    function destroyCharts() {
      if (timelineChart) {
        timelineChart.destroy();
        timelineChart = null;
      }
      if (sentimentChart) {
        sentimentChart.destroy();
        sentimentChart = null;
      }
      if (engagementChart) {
        engagementChart.destroy();
        engagementChart = null;
      }
    }

    function buildOrUpdateCharts() {
      if (!chartJsLoaded.value) return;
      const tl = timelineStack.value;

      // Timeline (stacked bar)
      if (!timelineChart && timelineCanvas.value) {
        timelineChart = new Chart(timelineCanvas.value.getContext("2d"), {
          type: "bar",
          data: {
            labels: tl.labels,
            datasets: [
              { label: "Negative", data: tl.neg, backgroundColor: "#ef4444" },
              { label: "Neutral", data: tl.neu, backgroundColor: "#9ca3af" },
              { label: "Positive", data: tl.pos, backgroundColor: "#10b981" },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: "bottom" } },
            scales: {
              x: { stacked: true, ticks: { maxRotation: 45, minRotation: 0 } },
              y: { stacked: true, beginAtZero: true },
            },
          },
        });
      } else if (timelineChart) {
        timelineChart.data.labels = tl.labels;
        timelineChart.data.datasets[0].data = tl.neg;
        timelineChart.data.datasets[1].data = tl.neu;
        timelineChart.data.datasets[2].data = tl.pos;
        timelineChart.update();
      }

      // Sentiment mix (doughnut)
      const sc = sentimentCounts.value;
      if (!sentimentChart && sentimentCanvas.value) {
        sentimentChart = new Chart(sentimentCanvas.value.getContext("2d"), {
          type: "doughnut",
          data: {
            labels: ["Positive", "Neutral", "Negative"],
            datasets: [
              {
                data: [sc.positive, sc.neutral, sc.negative],
                backgroundColor: ["#10b981", "#9ca3af", "#ef4444"],
              },
            ],
          },
          options: { plugins: { legend: { position: "bottom" } } },
        });
      } else if (sentimentChart) {
        sentimentChart.data.datasets[0].data = [sc.positive, sc.neutral, sc.negative];
        sentimentChart.update();
      }

      // Engagement totals (horizontal bar)
      const totals = [
        sumLikes.value,
        sumRetweets.value,
        sumReplies.value,
        sumQuotes.value,
        sumImpressions.value,
      ];
      if (!engagementChart && engagementCanvas.value) {
        engagementChart = new Chart(engagementCanvas.value.getContext("2d"), {
          type: "bar",
          data: {
            labels: ["Likes", "Retweets", "Replies", "Quotes", "Impressions"],
            datasets: [
              {
                label: "Total",
                data: totals,
                backgroundColor: "#3b82f6",
              },
            ],
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
        engagementChart.data.datasets[0].data = totals;
        engagementChart.update();
      }
    }

    // React to data changes
    watch(
      () => [props.tweets, props.unit],
      async () => {
        // Recompute and update charts
        if (!Chart) await ensureChartJs();
        buildOrUpdateCharts();
      },
      { immediate: true, deep: false }
    );

    onMounted(async () => {
      await ensureChartJs();
      buildOrUpdateCharts();
    });

    onBeforeUnmount(() => {
      destroyCharts();
    });

    return {
      // props
      unitLabel,
      // state for charts
      chartJsLoaded,
      timelineCanvas,
      sentimentCanvas,
      engagementCanvas,
      // metrics
      totalTweets,
      uniqueAuthors,
      sentimentCounts,
      sumLikes,
      sumRetweets,
      sumReplies,
      sumQuotes,
      sumImpressions,
      // lists
      topLanguages,
      topTweets,
      filteredTopTweets,
      search,
      // utils
      formatNumber,
      formatDate,
      // exports
      copyJson,
      downloadCsv,
    };
  },
};
</script>

<style scoped>
.tw-container {
  display: grid;
  gap: 16px;
}
.tw-header {
  display: grid;
  gap: 10px;
}
.tw-title {
  display: flex;
  align-items: baseline;
  gap: 10px;
}
.tw-title h3 {
  margin: 0;
  color: #0f766e;
  font-size: 16px;
  font-weight: 700;
}
.tw-sub {
  color: #6b7280;
  font-size: 12px;
}
.tw-kpis {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 8px;
}
.kpi {
  background: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 10px;
}
.kpi-label {
  color: #6b7280;
  font-size: 12px;
}
.kpi-value {
  color: #111827;
  font-weight: 700;
  font-size: 18px;
}

.tw-loading,
.tw-empty {
  padding: 14px;
  background: #f8fafc;
  border: 1px dashed #d1d5db;
  border-radius: 8px;
  color: #374151;
}

.tw-charts {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 12px;
}
.chart-card {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 12px;
  display: grid;
  gap: 10px;
}
.chart-title {
  color: #0f766e;
  font-size: 14px;
  font-weight: 700;
}
.chart-wrap {
  position: relative;
  height: 220px;
}
.chart-fallback {
  color: #6b7280;
  font-size: 13px;
}

.tw-meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 12px;
}
.meta-card {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 12px;
  display: grid;
  gap: 10px;
}
.meta-title {
  color: #0f766e;
  font-size: 14px;
  font-weight: 700;
}
.meta-list {
  display: grid;
  gap: 8px;
}
.meta-row {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  font-size: 13px;
}
.meta-key {
  color: #6b7280;
  font-weight: 600;
}
.meta-val {
  color: #111827;
  font-weight: 700;
}
.meta-actions {
  display: flex;
  gap: 8px;
}
.btn {
  background: #f0fdfa;
  color: #0f766e;
  border: 1px solid #14b8a6;
  border-radius: 6px;
  padding: 6px 10px;
  cursor: pointer;
  font-size: 12px;
}
.btn:hover {
  background: #0f766e;
  color: #f0fdfa;
}

.tw-table {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 12px;
  display: grid;
  gap: 10px;
}
.table-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.table-title {
  color: #0f766e;
  font-size: 14px;
  font-weight: 700;
}
.search {
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  padding: 6px 8px;
  font-size: 13px;
  width: 260px;
}
.table {
  border-collapse: collapse;
  width: 100%;
  font-size: 13px;
}
.table th, .table td {
  border-top: 1px solid #e5e7eb;
  padding: 8px 6px;
  vertical-align: top;
}
.content-cell {
  max-width: 520px;
  white-space: pre-wrap;
  word-break: break-word;
}
.pill {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
  color: white;
  text-transform: capitalize;
}
.pill.positive { background: #10b981; }
.pill.neutral { background: #9ca3af; color: #111827; }
.pill.negative { background: #ef4444; }
</style>