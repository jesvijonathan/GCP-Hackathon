<template>
  <section class="tw-container">
    <header class="tw-header">
      <div class="tw-title">
        <h3>WL Analytics</h3>
        <span class="tw-sub">Granularity: {{ unitLabel }}</span>
      </div>

      <div class="tw-kpis">
        <div class="kpi">
          <div class="kpi-label">Transactions</div>
          <div class="kpi-value">{{ totalTxs }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Unique Users</div>
          <div class="kpi-value">{{ uniqueUsers }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Total Amount</div>
          <div class="kpi-value">{{ formatNumber(sumAmount) }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Avg Amount</div>
          <div class="kpi-value">{{ formatNumber(avgAmount) }}</div>
        </div>
        <div class="kpi">
          <div class="kpi-label">Fraud Transactions</div>
          <div class="kpi-value">{{ totalFraud }}</div>
        </div>
      </div>
    </header>

    <div v-if="loading" class="tw-refresh">Refreshing… (previous data shown)</div>
    <div v-else-if="!normalizedWl.length" class="tw-empty">No WL transactions in this range.</div>
      <section class="tw-charts">
        <!-- Throughput + Fraud Trend -->
        <div class="chart-card">
          <div class="chart-title">Throughput & Fraud Trend</div>
          <div v-if="chartJsLoaded" class="chart-wrap" style="height: 300px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="throughputCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Throughput chart unavailable (Chart.js missing).
          </div>
        </div>

        <!-- Activity timeline (stacked by risk) -->
        <div class="chart-card">
          <div class="chart-title">Activity timeline (stacked by risk)</div>
          <div v-if="chartJsLoaded" class="chart-wrap" style="height: 300px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="timelineCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Chart.js not available — showing summary only.
          </div>
        </div>

        <!-- Fraud mix -->
        <div class="chart-card">
          <div class="chart-title">Fraud mix</div>
          <div v-if="chartJsLoaded" class="chart-wrap square-chart" style="height: 260px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="fraudCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            Low / Medium / High fraud distribution
          </div>
        </div>

        <!-- MCC distribution -->
        <div class="chart-card">
          <div class="chart-title">Top MCCs</div>
          <div v-if="chartJsLoaded" class="chart-wrap" style="height: 260px;">
            <div v-if="chartsBuilding" class="chart-loader">Processing...</div>
            <canvas ref="mccCanvas" height="140"></canvas>
          </div>
          <div v-else class="chart-fallback">
            MCC distribution unavailable
          </div>
        </div>
      </section>

      <section class="tw-meta">
        <div class="meta-card" style="display:grid;gap:10px;">
          <div class="meta-title">Insights</div>
          <div style="font-size:12px;line-height:1.4;display:grid;gap:6px;">
            <div><strong>Peak interval:</strong> {{ peakInterval?.label || '-' }} <span v-if="peakInterval">({{ peakInterval.count }} txns)</span></div>
            <div><strong>Fraud rate:</strong> {{ fraudRate }}%</div>
            <div><strong>Avg amount:</strong> {{ formatNumber(avgAmount) }}</div>
            <div><strong>Top MCC:</strong> {{ firstTopMcc }}</div>
          </div>
          <div style="display:flex;flex-wrap:wrap;gap:6px;">
            <button type="button" @click="setRiskFilter('all')" :class="['flt-chip', riskFilter==='all' && 'on']">All</button>
            <button type="button" @click="setRiskFilter('low')" :class="['flt-chip','low', riskFilter==='low' && 'on']">Low</button>
            <button type="button" @click="setRiskFilter('medium')" :class="['flt-chip','medium', riskFilter==='medium' && 'on']">Medium</button>
            <button type="button" @click="setRiskFilter('high')" :class="['flt-chip','high', riskFilter==='high' && 'on']">High</button>
            <button type="button" @click="setRiskFilter('fraud')" :class="['flt-chip','high', riskFilter==='fraud' && 'on']">Fraud</button>
          </div>
        </div>

        <div class="meta-card">
          <div class="meta-title">Top MCCs</div>
          <div class="meta-list">
            <div v-for="(count, mcc) in topMcc" :key="mcc" class="meta-row">
              <span class="meta-key">{{ mcc }}</span>
              <span class="meta-val">{{ count }}</span>
            </div>
          </div>
        </div>
      </section>

  <section class="tw-table" style="max-height: 48vh; overflow: auto;">
        <div class="table-header">
          <div class="table-title">Transactions (Top by risk score)</div>
          <input class="search" v-model="search" placeholder="Search by txn_id, MCC, merchant, user..." />
        </div>
        <table class="table">
          <thead>
            <tr>
              <th style="width:140px;">Time</th>
              <th>Txn ID</th>
              <th>Merchant</th>
              <th style="width:100px;">MCC</th>
              <th style="width:100px;">Amount</th>
              <th style="width:90px;">Currency</th>
              <th style="width:100px;">Status</th>
              <th style="width:120px;">Risk Score</th>
              <th>Risk Factors</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="t in filteredWl" :key="t.txn_id">
              <td>{{ formatDate(t.dt) }}</td>
              <td>{{ t.txn_id }}</td>
              <td>{{ t.merchant || "" }}</td>
              <td>{{ t.mcc || "" }}</td>
              <td>{{ t.amount != null ? t.amount.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) : "" }}</td>
              <td>{{ t.currency_code || "" }}</td>
              <td>{{ t.status || "" }}</td>
              <td>{{ t.risk_score != null ? t.risk_score : "" }}</td>
              <td>
                <span v-if="Array.isArray(t.risk_factors) && t.risk_factors.length">
                  {{ t.risk_factors.join(", ") }}
                </span>
                <span v-else-if="t.fraudFlag">fraud</span>
                <span v-else>-</span>
              </td>
            </tr>
          </tbody>
        </table>
      </section>
    
  </section>
</template>

<script>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from "vue";

export default {
  name: "MerchantWl_Data",
  props: {
    txns: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false },
    unit: { type: String, default: "hour" },
  },
  setup(props) {
    // Chart.js loader
  const chartJsLoaded = ref(false);
    const chartsBuilding = ref(true);

    const throughputCanvas = ref(null);
    const timelineCanvas = ref(null);
    const fraudCanvas = ref(null);
    const mccCanvas = ref(null);

    let throughputChart = null;
    let timelineChart = null;
    let fraudChart = null;
    let mccChart = null;

    // Normalize WL transactions to a common shape
    const normalizedWl = computed(() => {
      const arr = (props.txns || []).map((t) => {
        const dt = t.dt ?? t.txn_time ?? t.ts ?? t.created_at ?? t.time;
        const riskFlags = t.risk_flags || {};
        const riskScore = t.risk_score ?? riskFlags?.score ?? 0;
        const mcc = t.mcc ?? t.mcc_code ?? t.mccCode ?? t["mcc"] ?? null;
        const isFraud = !!riskFlags?.fraud_suspected || (riskScore > 60);
        const riskLabel = riskScore >= 60 ? "high" : riskScore >= 30 ? "medium" : "low";

        return {
          txn_id: t.txn_id ?? t.tx_id ?? "",
          dt,
          amount: t.amount != null ? Number(t.amount) : 0,
          currency_code: t.currency_code ?? "",
          merchant: t.merchant ?? "",
          mcc: mcc,
          status: t.status ?? t.txn_type ?? "",
          risk_score: riskScore,
          risk_factors: t.risk_factors || [],
          isFraud,
          risk_label: riskLabel,
          user_name: t.user_name ?? t.username ?? "",
          country_code: t.country_code ?? "",
        };
      });
      // Filter out invalid dates
      return arr.filter((x) => x && x.dt);
    });

    const totalTxs = computed(() => normalizedWl.value.length);
    const uniqueUsers = computed(() => {
      const set = new Set();
      normalizedWl.value.forEach((t) => {
        if (t.user_name) set.add(t.user_name);
      });
      return set.size;
    });

    const sumAmount = computed(() => normalizedWl.value.reduce((a, t) => a + (t.amount || 0), 0));
    const avgAmount = computed(() => (normalizedWl.value.length ? sumAmount.value / normalizedWl.value.length : 0));
    const totalFraud = computed(() => normalizedWl.value.filter((t) => t.isFraud).length);

    const topMcc = computed(() => {
      const map = new Map();
      for (const t of normalizedWl.value) {
        if (!t.mcc) continue;
        map.set(t.mcc, (map.get(t.mcc) || 0) + 1);
      }
      const arr = Array.from(map.entries()).sort((a, b) => b[1] - a[1]).slice(0, 6);
      const out = {};
      arr.forEach(([k, v]) => (out[k] = v));
      return out;
    });

    // Fraud distribution by risk label
    const fraudMix = computed(() => {
      const counts = { low: 0, medium: 0, high: 0 };
      for (const t of normalizedWl.value) {
        const rl = t.risk_label || "low";
        counts[rl] = (counts[rl] || 0) + 1;
      }
      return counts;
    });

    // Timeline by risk category (bucketed by unit)
    const timelineStack = computed(() => {
      const unit = (props.unit || "hour").toLowerCase();
      const buckets = new Map();
      for (const t of normalizedWl.value) {
        const d = new Date(t.dt);
        let bucket;
        if (unit === "hour") {
          bucket = new Date(d);
          bucket.setMinutes(0, 0, 0);
        } else if (unit === "week") {
          bucket = new Date(d);
          const day = bucket.getUTCDay();
          const diff = (day === 0 ? -6 : 1 - day);
          bucket.setDate(bucket.getDate() + diff);
          bucket.setHours(0, 0, 0, 0);
        } else {
          bucket = new Date(d);
          bucket.setHours(0, 0, 0, 0);
        }
        const key = bucket.toISOString();
        if (!buckets.has(key)) buckets.set(key, { low: 0, med: 0, high: 0, total: 0, dt: bucket });
        const slot = buckets.get(key);
        const lab = t.risk_label;
        if (lab === "high") slot.high++;
        else if (lab === "medium") slot.med++;
        else slot.low++;
        slot.total++;
      }
      const rows = Array.from(buckets.values()).sort((a, b) => a.dt - b.dt);
      return {
        labels: rows.map((r) => r.dt.toLocaleString()),
        low: rows.map((r) => r.low),
        med: rows.map((r) => r.med),
        high: rows.map((r) => r.high),
        total: rows.map((r) => r.total),
      };
    });

    // Throughput data (counts + fraud counts per bucket)
    const timelineFraud = computed(() => {
      // Similar bucketing as timelineStack to align charts
      const unit = (props.unit || "hour").toLowerCase();
      const buckets = new Map();
      for (const t of normalizedWl.value) {
        const d = new Date(t.dt);
        let bucket;
        if (unit === "hour") {
          bucket = new Date(d);
          bucket.setMinutes(0, 0, 0);
        } else if (unit === "week") {
          bucket = new Date(d);
          const day = bucket.getUTCDay();
          const diff = (day === 0 ? -6 : 1 - day);
          bucket.setDate(bucket.getDate() + diff);
          bucket.setHours(0, 0, 0, 0);
        } else {
          bucket = new Date(d);
          bucket.setHours(0, 0, 0, 0);
        }
        const key = bucket.toISOString();
        if (!buckets.has(key)) buckets.set(key, { total: 0, fraud: 0, dt: bucket });
        const slot = buckets.get(key);
        if (t.isFraud) slot.fraud++;
        slot.total++;
      }
      const rows = Array.from(buckets.values()).sort((a, b) => a.dt - b.dt);
      return {
        labels: rows.map((r) => r.dt.toLocaleString()),
        counts: rows.map((r) => r.total),
        frauds: rows.map((r) => r.fraud),
      };
    });

    // Helpers for rendering
    const unitLabel = computed(() => {
      const u = (props.unit || "").toLowerCase();
      if (u === "hour") return "hourly";
      if (u === "week") return "weekly";
      return "daily";
    });

    const search = ref("");
    const riskFilter = ref('all');
    const filteredWl = computed(() => {
      const q = (search.value || "").toLowerCase();
      let base = normalizedWl.value.slice();
      if (riskFilter.value !== 'all') {
        base = base.filter(t => {
          if (riskFilter.value === 'fraud') return t.isFraud;
          return t.risk_label === riskFilter.value;
        });
      }
      base.sort((a,b)=>(b.risk_score||0)-(a.risk_score||0));
      if (!q) return base.slice(0,500);
      return base.filter((t) => {
        const s = [t.txn_id, t.merchant, t.mcc, t.user_name, t.risk_score]
          .filter(Boolean).join(' ').toLowerCase();
        return s.includes(q);
      }).slice(0,500);
    });
    function setRiskFilter(v){ riskFilter.value = v; }

    // Chart helpers
  let Chart = null;

    const ensureChartJs = async () => {
      if (chartJsLoaded.value && Chart) return;
      try {
        const m = await import("chart.js/auto");
        Chart = m.default || m;
        chartJsLoaded.value = true;
      } catch (e) {
        console.warn("Chart.js not installed. WL charts hidden.", e);
        chartJsLoaded.value = false;
      }
    };

    function destroyCharts() {
      [timelineChart, throughputChart, fraudChart, mccChart].forEach((c) => {
        if (c && c.destroy) c.destroy();
      });
      timelineChart = throughputChart = fraudChart = mccChart = null;
    }

    function buildOrUpdateCharts() {
      if (!chartJsLoaded.value) return;

      // Timeline (stacked by risk)
      const tl = timelineStack.value;
      function createTickFormatter(allLabels) {
        const parsed = allLabels.map(l => {
          const d = new Date(l);
          return { raw: l, date: d, valid: !isNaN(d.getTime()), dayKey: d.getFullYear()+"-"+(d.getMonth()+1)+"-"+d.getDate() };
        });
        const distinctDays = new Set(parsed.filter(p=>p.valid).map(p=>p.dayKey));
        const multiDay = distinctDays.size > 1;
        return function(value, idx) {
          let raw;
          try { if (this && this.getLabelForValue) raw = this.getLabelForValue(value); } catch(_){}
          if (!raw) raw = allLabels[idx] || value;
          const p = parsed[idx];
          if (!p || !p.valid) return raw;
          const h = String(p.date.getHours()).padStart(2,'0');
          const m = String(p.date.getMinutes()).padStart(2,'0');
          if (!multiDay) return `${h}:${m}`;
          const prev = parsed[idx-1];
          if (idx === 0 || idx === parsed.length-1 || (prev && prev.dayKey !== p.dayKey)) {
            return `${p.date.getMonth()+1}/${p.date.getDate()}\n${h}:${m}`;
          }
          return `${h}:${m}`;
        };
      }

      const tickFormatterTimeline = createTickFormatter(tl.labels);

      if (!timelineChart && timelineCanvas.value) {
        const ctx = timelineCanvas.value.getContext("2d");
        timelineChart = new Chart(ctx, {
          type: "bar",
          data: {
            labels: tl.labels,
            datasets: [
              { label: "Low", data: tl.low, backgroundColor: "#a3e635", stack: "s" },
              { label: "Medium", data: tl.med, backgroundColor: "#f59e0b", stack: "s" },
              { label: "High", data: tl.high, backgroundColor: "#f87171", stack: "s" },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: { padding: 0 },
            plugins: { legend: { position: "bottom", labels: { boxWidth: 10, padding: 6, font: { size: 10 } } } },
            scales: {
              x: { stacked: true, ticks: { autoSkip: true, maxTicksLimit: Math.min(tl.labels.length, 12), font: { size: 10 }, callback: tickFormatterTimeline } },
              y: { stacked: true, beginAtZero: true, ticks: { font: { size: 10 } } },
            },
          },
        });
      } else if (timelineChart) {
        timelineChart.options.animation = false;
        timelineChart.data.labels = tl.labels;
        timelineChart.data.datasets[0].data = tl.low;
        timelineChart.data.datasets[1].data = tl.med;
        timelineChart.data.datasets[2].data = tl.high;
        timelineChart.update();
      }

      // Throughput: total vs fraud per bucket
      const tf = timelineFraud.value;
  const tickFormatterThroughput = createTickFormatter(tf.labels);
  if (!throughputChart && throughputCanvas.value) {
        const ctx = throughputCanvas.value.getContext("2d");
        throughputChart = new Chart(ctx, {
          type: "bar",
          data: {
            labels: tf.labels,
            datasets: [
              {
                label: "Txn Count",
                data: tf.counts,
                backgroundColor: "#6366f1",
              },
              {
                label: "Fraud Count",
                data: tf.frauds,
                backgroundColor: "#ef4444",
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: { padding: 0 },
            plugins: { legend: { position: "bottom", labels: { boxWidth: 10, padding: 6, font: { size: 10 } } } },
            scales: {
              x: { beginAtZero: true, ticks: { autoSkip: true, maxTicksLimit: Math.min(tf.labels.length, 12), font: { size: 10 }, callback: tickFormatterThroughput } },
              y: { beginAtZero: true, ticks: { font: { size: 10 } } },
            },
          },
        });
      } else if (throughputChart) {
        throughputChart.options.animation = false;
        throughputChart.data.labels = tf.labels;
        throughputChart.data.datasets[0].data = tf.counts;
        throughputChart.data.datasets[1].data = tf.frauds;
        throughputChart.update();
      }

      // Fraud mix doughnut
      const fm = fraudMix.value;
      if (!fraudChart && fraudCanvas.value) {
        const ctx = fraudCanvas.value.getContext("2d");
        fraudChart = new Chart(ctx, {
          type: "doughnut",
          data: {
            labels: ["Low", "Medium", "High"],
            datasets: [{
              data: [fm.low, fm.medium, fm.high],
              backgroundColor: ["#34d399", "#f59e0b", "#f87171"],
              borderColor: "#fff",
              borderWidth: 2,
            }],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '55%',
            layout: { padding: 0 },
            plugins: { legend: { position: "bottom" } }
          }
        });
        queueMicrotask(() => { enforceSquare(fraudCanvas.value, fraudChart); });
      } else if (fraudChart) {
        fraudChart.options.animation = false;
        fraudChart.data.datasets[0].data = [fm.low, fm.medium, fm.high];
        fraudChart.update();
        enforceSquare(fraudCanvas.value, fraudChart);
      }

      // MCC distribution (horizontal bar)
      const mc = topMcc;
      const keys = Object.keys(mc);
      if (!mccChart && mccCanvas.value) {
        const ctx = mccCanvas.value.getContext("2d");
        mccChart = new Chart(ctx, {
          type: "bar",
          data: {
            labels: keys,
            datasets: [{
              label: "Txn Count",
              data: keys.map((k) => mc[k]),
              backgroundColor: "#3b82f6",
            }],
          },
          options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true }, y: { ticks: { precision:0 } } }
          }
        });
      } else if (mccChart) {
        mccChart.options.animation = false;
        mccChart.data.labels = keys;
        mccChart.data.datasets[0].data = keys.map((k) => mc[k]);
        mccChart.update();
      }

      // End frame
      requestAnimationFrame(() => { chartsBuilding.value = false; });
    }

    // Init and reactivity
    let resizeObs = null;
    onMounted(async () => {
      await ensureChartJs();
      await nextTick();
      requestAnimationFrame(() => { buildOrUpdateCharts(); });
      try {
        resizeObs = new ResizeObserver(() => {
          [timelineChart, throughputChart, fraudChart, mccChart].forEach(ch => { if (ch) ch.resize(); });
          enforceSquare(fraudCanvas.value, fraudChart);
        });
        const host = timelineCanvas.value?.parentElement?.parentElement; // chart-wrap
        if (host) resizeObs.observe(host);
        const doughnutHost = fraudCanvas.value?.parentElement?.parentElement;
        if (doughnutHost && doughnutHost !== host) resizeObs.observe(doughnutHost);
      } catch(_){}
    });

  onBeforeUnmount(() => { if (resizeObs) try { resizeObs.disconnect(); } catch(_){} destroyCharts(); });

    watch([() => props.txns, () => props.unit], async () => {
      if (!chartJsLoaded.value) await ensureChartJs();
      await nextTick();
      if (!normalizedWl.value.length) {
        // Clear existing charts for fast repopulation instead of destroying
        [timelineChart, throughputChart, fraudChart, mccChart].forEach(ch => {
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

    // Insights
    const peakInterval = computed(() => {
      const tl = timelineStack.value; if (!tl.total?.length) return null;
      let max=-1, idx=-1; tl.total.forEach((v,i)=>{ if(v>max){max=v;idx=i;} });
      return idx>=0 ? { label: tl.labels[idx], count: tl.total[idx] } : null;
    });
    const fraudRate = computed(() => totalTxs.value ? ( (totalFraud.value/totalTxs.value)*100 ).toFixed(1) : '0.0');
    const firstTopMcc = computed(()=> Object.keys(topMcc.value)[0] || '-');

    // CSV export (legacy – triggers still present via keyboard if needed)
    function toCsv(rows) {
      if (!rows?.length) return "txn_id,dt,merchant,mcc,amount,currency,status,risk_score,risk_factors\n";
      const head = ["txn_id","dt","merchant","mcc","amount","currency","status","risk_score","risk_factors"];
      const lines = [head.join(",")];
      for (const r of rows) {
        const fields = [
          r.txn_id, r.dt, r.merchant, r.mcc, r.amount, r.currency_code, r.status,
          r.risk_score, (r.risk_factors || []).join("|"),
        ];
        const safe = fields.map((v, i) => typeof v === "string" && i < 3 ? `"${v}"` : v);
        lines.push(safe.join(","));
      }
      return lines.join("\n");
    }

    async function copyJson() {
      try {
        await navigator.clipboard.writeText(JSON.stringify(normalizedWl.value, null, 2));
      } catch {
        const blob = new Blob([JSON.stringify(normalizedWl.value, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url; a.download = "wl_transactions.json"; a.click();
        URL.revokeObjectURL(url);
      }
    }

    async function downloadCsv() {
      const csv = toCsv(filteredWl.value);
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = "wl_transactions.csv"; a.click();
      URL.revokeObjectURL(url);
    }

    // Expose
    function enforceSquare(canvasEl, chartInstance) {
      if (!canvasEl || !chartInstance) return;
      const wrap = canvasEl.parentElement;
      if (!wrap) return;
      const w = wrap.clientWidth;
      if (w > 0) {
        canvasEl.style.width = w + 'px';
        canvasEl.style.height = w + 'px';
        try { chartInstance.resize(); } catch(_){}
      }
    }

    return {
      unitLabel,
      chartJsLoaded,
      chartsBuilding,
      throughputCanvas,
      timelineCanvas,
      fraudCanvas,
      mccCanvas,
      totalTxs,
      uniqueUsers,
      sumAmount,
      avgAmount,
      totalFraud,
      topMcc,
      filteredWl,
      search,
      riskFilter,
      setRiskFilter,
      formatNumber: (n) => {
        const x = Number(n || 0);
        if (x >= 1e9) return (x / 1e9).toFixed(2) + "B";
        if (x >= 1e6) return (x / 1e6).toFixed(2) + "M";
        if (x >= 1e3) return (x / 1e3).toFixed(1) + "k";
        return String(x);
      },
      formatDate: (iso) => {
        if (!iso) return "-";
        try { return new Date(iso).toLocaleString(); } catch { return iso; }
      },
      copyJson,
      downloadCsv,
      normalizedWl,
      peakInterval,
      fraudRate,
      firstTopMcc,
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
.kpi { background:#f9fafb; border:1px solid #e5e7eb; border-radius:10px; padding:10px; display:grid; gap:4px; }
.kpi-label { color:#6b7280; font-size:12px; }
.kpi-value { color:#111827; font-weight:700; font-size:18px; }

.tw-loading, .tw-empty { padding:14px; background:#f8fafc; border:1px dashed #d1d5db; border-radius:8px; color:#374151; }
.tw-refresh { padding:6px 10px; font-size:12px; color:#0f766e; font-weight:600; }
.tw-charts { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; }
.chart-card { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:12px; display:grid; gap:10px; }
.chart-title { color:#0f766e; font-size:14px; font-weight:700; }
.chart-wrap { position:relative; height:260px; }
.square-chart { aspect-ratio:1/1; height:auto !important; min-height:220px; }
.chart-fallback { color:#6b7280; font-size:13px; }
.chart-loader {
  position:absolute; inset:0; display:flex; align-items:center; justify-content:center;
  background:linear-gradient(135deg,#f8fafc 0%,#eef2f7 100%);
  font-size:13px; color:#0f766e; font-weight:600; z-index:2;
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
.flt-chip.low { border-color:#34d399; }
.flt-chip.medium { border-color:#f59e0b; }
.flt-chip.high { border-color:#f87171; }
.tw-table { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:12px; display:grid; gap:10px; }
.table-header { display:flex; align-items:center; justify-content:space-between; }
.table-title { color:#0f766e; font-size:14px; font-weight:700; }
.search { border:1px solid #e5e7eb; border-radius:6px; padding:6px 8px; font-size:13px; width:260px; }
.table { border-collapse:collapse; width:100%; font-size:13px; }
.table th,.table td { border-top:1px solid #e5e7eb; padding:8px 6px; vertical-align:top; }
.content-cell { max-width:520px; white-space:pre-wrap; word-break:break-word; }
.pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:700; color:white; text-transform:capitalize; }
.pill.low { background:#34d399; }
.pill.medium { background:#f59e0b; }
.pill.high { background:#f87171; }
</style>