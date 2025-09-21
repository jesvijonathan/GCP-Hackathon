<template>
  <div class="card risk-metrics-card" v-if="merchant">
    <div class="card-header">
      <h3>Risk Metrics Dashboard</h3>
    </div>
    <div class="card-content metrics-grid">
      <!-- Donut Gauge: Risk Score -->
      <div class="grid-item gauge-item">
        <svg
          width="180"
          height="120"
          viewBox="0 0 180 120"
          role="img"
          aria-label="Risk score gauge"
        >
          <circle
            cx="90"
            cy="60"
            r="50"
            stroke="#e5e7eb"
            stroke-width="14"
            fill="none"
          />
          <circle
            cx="90"
            cy="60"
            r="50"
            stroke="#14b8a6"
            stroke-width="14"
            fill="none"
            :stroke-dasharray="gaugeCircumference"
            :stroke-dashoffset="gaugeCircumference * (1 - riskScore / 100)"
            stroke-linecap="round"
            transform="rotate(-90 90 60)"
          />
          <text
            x="90"
            y="68"
            text-anchor="middle"
            font-size="20"
            fill="#374151"
          >
            {{ riskScore }}
          </text>
        </svg>
        <div class="legend">
          <span class="legend-dot" style="background: #14b8a6"></span>
          Risk Score
        </div>
      </div>

      <!-- Alerts Bar (based on alerts length) -->
      <div class="grid-item bar-item" aria-label="Alerts bar">
        <svg width="180" height="90" viewBox="0 0 180 90" role="img">
          <rect
            x="70"
            :y="90 - alertsHeight"
            width="40"
            :height="alertsHeight"
            fill="#14b8a6"
            rx="6"
            ry="6"
          />
        </svg>
        <div class="legend">
          Alerts: {{ alertsCount }}
        </div>
      </div>

      <!-- Monthly Trend Sparkline -->
      <div class="grid-item sparkline-item" aria-label="Monthly trend">
        <svg
          width="180"
          height="60"
          viewBox="0 0 180 60"
          preserveAspectRatio="xMidYMid meet"
        >
          <polyline
            :points="sparklinePoints"
            fill="none"
            stroke="#14b8a6"
            stroke-width="2"
          />
        </svg>
        <div class="legend" style="margin-top: 6px">Monthly Trend</div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: "DashboardRiskMetrics",
  props: {
    merchant: {
      type: Object,
      required: true,
    },
  },
  computed: {
    riskScore() {
      return this.merchant?.riskMetrics?.riskScore ?? 0;
    },
    alertsCount() {
      return this.merchant?.alerts?.length ?? 0;
    },
    alertsHeight() {
      const count = this.alertsCount;
      return Math.min(count * 12, 60);
    },
    gaugeCircumference() {
      return 2 * Math.PI * 50;
    },
    sparklinePoints() {
      const volumeData = this.merchant?.transactions?.monthly?.volume;
      const v = volumeData ?? 100;
      const points = Array.isArray(v) ? v : [v * 0.8, v * 0.9, v, v * 1.05, v * 0.95, v * 1.08]; // Fallback if data not array
      const w = 180, h = 60;
      const max = Math.max(...points, 1);
      const min = Math.min(...points, 0);
      const range = Math.max(max - min, 1);
      const step = w / Math.max(points.length - 1, 1);

      return points
        .map((val, i) => {
          const x = i * step;
          const y = h - ((val - min) / range) * h;
          return `${x},${y}`;
        })
        .join(" ");
    },
  },
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

.card-content {
  font-size: 14px;
  color: #374151;
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 15px;
  align-items: start;
}

.grid-item {
  background: #f8f9fa;
  border-radius: 8px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  align-items: center;
  border: 1px solid #e5e7eb;
  transition: all 0.2s ease;
}

.grid-item:hover {
  background: #f0fdfa;
  border-color: #14b8a6;
}

.legend {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: #374151;
  margin-top: 8px;
  font-weight: 500;
}

.legend-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
}

/* Responsive styles for metrics-grid */
@media (max-width: 768px) {
  .metrics-grid {
    grid-template-columns: 1fr;
  }
}
</style>