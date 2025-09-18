```vue
<template>
  <div class="merchant-dashboard teal-theme">
    <!-- Left Sidebar: Merchant List with Search/Filter -->
    <aside class="sidebar">
      <div class="sidebar-header">
        <h2>Merchants</h2>
      </div>

      <div class="search-filter">
        <label class="sr-only" for="search">Search Box</label>
        <input
          id="search"
          v-model="searchQuery"
          placeholder="Search Box"
          class="search-input"
        />
        <div class="filters">
          <div class="filter-title">Filter Options:</div>
          <label
          class="actual-filters" ><input  type="checkbox" v-model="filters.riskHigh" /> Risk
            Level</label
          >
          <label
          class="actual-filters"><input type="checkbox" v-model="filters.statusActive" />
            Status</label
          >
          <label
          class="actual-filters"><input type="checkbox" v-model="filters.dateRangeThisMonth" /> Date
            Range</label
          >
        </div>
      </div>

      <div class="merchant-list" role="listbox" aria-label="Merchants">
        <div
          v-for="m in filteredMerchants"
          :key="m.id"
          class="merchant-item"
          :class="{
            selected: selectedMerchant && m.id === selectedMerchant.id,
          }"
          @click="selectMerchant(m)"
          role="option"
        >
          <div class="merchant-summary">
            <div class="merchant-name">{{ m.name }}</div>
            <div class="merchant-id">ID: {{ m.id }}</div>
            <div class="merchant-risk" v-if="m.riskMetrics">
              Risk: {{ m.riskMetrics.riskScore ?? "—" }}
            </div>
          </div>
        </div>
      </div>
    </aside>

    <!-- Right Details: Selected Merchant Details -->
    <section class="details-panel" v-if="selectedMerchant">
      <!-- Merchant Info Card -->
      <div class="card merchant-info-card">
        <div class="card-header">
          <h3>Merchant Info</h3>
        </div>
        <div class="card-content">
          <p><strong>Name:</strong> {{ selectedMerchant.name }}</p>
          <p><strong>ID:</strong> {{ selectedMerchant.id }}</p>
          <p>
            <strong>Business Type:</strong> {{ selectedMerchant.businessType }}
          </p>
        </div>
      </div>

      <!-- Risk Metrics Dashboard -->
      <div class="card risk-metrics-card">
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
              <!-- Base circle -->
              <circle
                cx="90"
                cy="60"
                r="50"
                stroke="#e5e7eb"
                stroke-width="14"
                fill="none"
              />
              <!-- Foreground arc (dynamic) -->
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
              <!-- Simple vertical bar whose height depends on number of alerts -->
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
              Alerts: {{ selectedMerchant.alerts?.length ?? 0 }}
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
                :points="
                  sparklinePoints(
                    selectedMerchant.transactions?.monthly?.volume
                  )
                "
                fill="none"
                stroke="#14b8a6"
                stroke-width="2"
              />
            </svg>
            <div class="legend" style="margin-top: 6px">Monthly Trend</div>
          </div>
        </div>
      </div>

      <!-- Transaction Analytics / Activities -->
      <div class="card transaction-analytics-card">
        <div class="card-header">
          <h3>Recent Activities & Alerts</h3>
        </div>
        <div class="card-content activities">
          <p>
            <strong>Last Activity:</strong>
            {{ formatDate(selectedMerchant.lastActivity) }}
          </p>
          <ul>
            <li v-for="(al, idx) in selectedMerchant.alerts" :key="idx">
              <strong>{{ al.type }}</strong
              >: {{ al.message }} <em>({{ al.date }})</em> - Severity:
              {{ al.severity }}
            </li>
          </ul>
        </div>
      </div>

      <!-- Action center -->
      <div class="card action-center-card">
        <h1 class="action-card-heading">Action Center for {{ selectedMerchant.name }}</h1>
        <button @click="notifyPermanentban" class="action-card-button">Parmanent Ban</button>
        <button @click="notifyShadowBan" class="action-card-button">Shadow Ban</button>
        <button @click="notifyContinueMerchant" class="action-card-button">Continue Merchant</button>
      </div>
    </section>
  </div>
</template>

<script>

import { toast } from 'vue3-toastify';
import 'vue3-toastify/dist/index.css';

export default {
  name: "MerchantDashboard",
  data() {
    return {
      merchants: [],
      selectedMerchant: null,
      searchQuery: "",
      filters: {
        riskHigh: false,
        statusActive: false,
        dateRangeThisMonth: false,
      },
      gaugeCircumference: 2 * Math.PI * 50, // for the donut gauge
    };
  },
  computed: {
    riskScore() {
      return this.selectedMerchant?.riskMetrics?.riskScore ?? 0;
    },
    alertsHeight() {
      // map number of alerts to a height (0-60)
      const count = this.selectedMerchant?.alerts?.length ?? 0;
      return Math.min(count * 12, 60);
    },
    filteredMerchants() {
      let list = this.merchants;
      if (this.searchQuery.trim()) {
        const q = this.searchQuery.toLowerCase();
        list = list.filter(
          (m) =>
            (m.name || "").toLowerCase().includes(q) ||
            (m.id || "").toString().toLowerCase().includes(q)
        );
      }
      if (this.filters.riskHigh) {
        list = list.filter((m) => (m.riskMetrics?.riskScore ?? 0) >= 70);
      }
      // Additional filters can be added similarly
      return list;
    },
  },
  methods: {
    selectMerchant(m) {
      this.selectedMerchant = m;
    },
    
    // Toastify notification functions
    notifyPermanentban() {
      if (!this.selectedMerchant) {
        toast.error("No merchant selected for action", {
          autoClose: 3000,
          position: "top-right",
        });
        return;
      }
      
      toast.error(`${this.selectedMerchant.name} has been permanently banned!`, {
        autoClose: 5000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
      
      // You can add actual ban logic here
      console.log(`Permanently banned merchant: ${this.selectedMerchant.id}`);
    },
    
    notifyShadowBan() {
      if (!this.selectedMerchant) {
        toast.error("No merchant selected for action", {
          autoClose: 3000,
          position: "top-right",
        });
        return;
      }
      
      toast.warning(`${this.selectedMerchant.name} has been shadow banned!`, {
        autoClose: 5000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
      
      // You can add actual shadow ban logic here
      console.log(`Shadow banned merchant: ${this.selectedMerchant.id}`);
    },
    
    notifyContinueMerchant() {
      if (!this.selectedMerchant) {
        toast.error("No merchant selected for action", {
          autoClose: 3000,
          position: "top-right",
        });
        return;
      }
      
      toast.success(`${this.selectedMerchant.name} will continue operating normally!`, {
        autoClose: 4000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
      
      // You can add actual continue logic here
      console.log(`Continued merchant operations: ${this.selectedMerchant.id}`);
    },
    
    formatCurrency(n) {
      if (typeof n !== "number") return n;
      return (
        "$" +
        n.toLocaleString(undefined, {
          minimumFractionDigits: 0,
          maximumFractionDigits: 0,
        })
      );
    },
    formatDate(dt) {
      if (!dt) return "";
      try {
        const d = new Date(dt);
        return d.toLocaleString();
      } catch {
        return dt;
      }
    },
    sparklinePoints(vol) {
      // Build a small 6-point trend from the given volume
      const v = vol ?? 0;
      const points = [v * 0.8, v * 0.9, v, v * 1.05, v * 0.95, v * 1.08];
      const w = 180,
        h = 60;
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
  mounted() {
    // Load merchants from a JSON file. Replace the path with your actual data URL.
    fetch("src/assets/merchant-data.json")
      .then((res) => res.json())
      .then((data) => {
        // Support multiple possible shapes
        const merchantsList =
          data?.merchants ?? data?.merchantsList ?? data ?? [];
        this.merchants = Array.isArray(merchantsList) ? merchantsList : [];
        if (this.merchants.length > 0) {
          this.selectedMerchant = this.merchants[0];
          console.log("Loaded merchants:", this.merchants[0]);
        }
      })
      .catch(() => {
        // Fallback: keep empty if fetch fails
        this.merchants = [];
      });
  },
};
</script>

<style scoped>
/* Teal, white and grey theme */
:root {
  /* Define a light teal/grey palette for consistency if used elsewhere */
  --teal-50: #e6fbf8;
  --teal-100: #bfeeea;
  --teal-200: #88e2d9;
  --teal-300: #5fd8c8;
  --teal-500: #14b8a6;
  --teal-600: #0f9a92;
  --teal-700: #0b7f7b;

  --grey-50: #fafafa;
  --grey-100: #f4f5f7;
  --grey-200: #e5e7eb;
  --grey-500: #6b7280;
  --grey-700: #374151;
  --bg: #ffffff;
}

.merchant-dashboard.teal-theme {
  /* Theme root styling applied to the layout */
  background: var(--bg);
  color: #374151;
  min-height: 100vh;
}

/* Layout */
.merchant-dashboard {
  display: grid;
  grid-template-columns: 320px 1fr;
  gap: 20px;
  padding: 16px;
  font-family: Arial, sans-serif;
  height: 100%;
}

/* Sidebar */
.sidebar {
  border: 1px solid var(--grey-200);
  border-radius: 12px;
  padding: 12px;
  background: #fff;
  overflow: hidden;
}
.sidebar-header h2 {
  margin: 0;
  font-size: 18px;
  color: rgb(11, 105, 105);
}
.sidebar-subtitle {
  margin: 6px 0 0;
  color: var(--grey-500);
  font-size: 12px;
}
.search-filter {
  margin-top: 5px;
  padding-top: 2px;
}
.search-input {
  width: 100%;
  padding: 8px 10px;
  border: 1px solid teal;
  border-radius: 6px;
  background: #fff;
  color: teal;
}

.search-input::placeholder{
    color:rgb(13, 184, 184);
}

.search-input:focus {
  outline: none;
  border-color: var(--teal-500);
  box-shadow: 0 0 0 3px rgba(20, 184, 166, 0.15);
}
.filters {
  margin-top: 8px;
  font-size: 12px;
  color: var(--grey-700);
}
.filter-title {
  font-weight: 600;
  margin-bottom: 6px;
  color: teal;
  font-size: 15px;
}
.actual-filters {
  margin-right: 6px;
  display: flex;
  flex-direction: row ;
  color: #034442;
  font-size: 15px;
  gap: 4px;
}
.merchant-list {
  margin-top: 12px;
  max-height: 520px;
  overflow: auto;
}
.merchant-summary{
    background-color: rgb(6, 124, 124);
    border-radius: 5px;
    color: white;
    padding-left: 5px;
}
.merchant-item {
  padding: 8px;
  border-bottom: 1px solid var(--grey-200);
  cursor: pointer;
}
.merchant-item.selected {
  background: #b3fde4;
  border-left: 3px solid var(--teal-500);
}
.merchant-name {
  font-weight: 600;
}
.merchant-id,
.merchant-risk {
  font-size: 12px;
  color: var(--grey-600);
}

/* Details Panel */
.details-panel {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  padding: 15px;
}
.card {
  border: 1px solid rgb(21, 156, 180);
  border-radius: 8px;
  background: #fff;
  padding: 12px;
}
.card-header h3 {
  margin: 0 0 8px 0;
  font-size: 16px;
  color: rgb(10, 109, 109);
  font-weight: 200px;
}
.card-content {
  font-size: 14px;
  color: var(--grey-700);
}
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
  align-items: start;
}
.grid-item {
  background: var(--grey-50);
  border-radius: 6px;
  padding: 8px;
  display: flex;
  flex-direction: column;
  align-items: center;
  border: 1px solid var(--grey-200);
}
.gauge-item {
  display: flex;
  flex-direction: column;
  align-items: center;
}
.bar-item {
  display: flex;
  flex-direction: column;
  align-items: center;
}
.sparkline-item {
  display: flex;
  flex-direction: column;
  align-items: center;
}
.legend {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--grey-700);
}
.legend-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
}
.activities ul {
  padding-left: 20px;
  margin: 0;
  color: var(--grey-700);
}
.merchant-info-card{
  padding: 20px;
}

.action-center-card{
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
}

.action-card-button{
  background-color: rgb(6, 124, 124);
  color: white;
  border: none;
  border-radius: 5px;
  padding: 10px;
  margin: 5px;
  width: 250px;
  cursor: pointer;
}

.action-card-heading{
  color: rgb(11, 105, 105);
  margin-bottom: 10px;
  font-size: 20px;
  font-weight: bold;
}
@media (max-width: 1024px) {
  .merchant-dashboard {
    grid-template-columns: 1fr;
  }
  .details-panel {
    grid-template-columns: 1fr;
  }
}
</style>
```
