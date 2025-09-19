<template>
  <div class="merchant-dashboard teal-theme">
    <!-- Left Sidebar: Merchant List with Search/Filter -->
    <aside class="sidebar">
      <div class="sidebar-header">
        <h2>Merchants</h2>
        <p class="sidebar-subtitle">Manage your merchants</p>
      </div>

      <div class="search-filter">
        <label class="sr-only" for="search">Search Box</label>
        <input
          id="search"
          v-model="searchQuery"
          placeholder="Search merchants..."
          class="search-input"
        />
        <div class="filters">
          <div class="filter-title">Filter Options:</div>
          <label class="actual-filters">
            <input type="checkbox" v-model="filters.riskHigh" /> 
            High Risk Level
          </label>
          <label class="actual-filters">
            <input type="checkbox" v-model="filters.statusActive" />
            Active Status
          </label>
          <label class="actual-filters">
            <input type="checkbox" v-model="filters.dateRangeThisMonth" /> 
            This Month
          </label>
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
            <div class="merchant-details-div">
              <div class="merchant-risk" v-if="m.riskMetrics">
                Risk: {{ m.riskMetrics.riskScore ?? "—" }}
              </div>
              <button 
                @click.stop="routeToMerchantPage(m.id)" 
                class="merchant-summary-button"
              >
                View Details
              </button>
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

      <!-- Action center -->
      <div class="card action-center-card">
        <h1 class="action-card-heading">
          Action Center for {{ selectedMerchant.name }}
        </h1>
        <button @click="notifyPermanentban" class="action-card-button">
          Permanent Ban
        </button>
        <button @click="notifyShadowBan" class="action-card-button">
          Shadow Ban
        </button>
        <button @click="notifyContinueMerchant" class="action-card-button">
          Continue Merchant
        </button>
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
          <ul v-if="selectedMerchant.alerts && selectedMerchant.alerts.length > 0">
            <li v-for="(al, idx) in selectedMerchant.alerts" :key="idx">
              <strong>{{ al.type }}</strong>: {{ al.message }} 
              <em>({{ al.date }})</em> - Severity: {{ al.severity }}
            </li>
          </ul>
          <p v-else class="no-alerts">No recent alerts</p>
        </div>
      </div>
    </section>

    <!-- Empty state when no merchant is selected -->
    <section class="details-panel empty-state" v-else>
      <div class="empty-message">
        <h3>Select a merchant to view details</h3>
        <p>Choose a merchant from the sidebar to see their information and risk metrics.</p>
      </div>
    </section>
  </div>
</template>

<script>
import { toast } from "vue3-toastify";
import "vue3-toastify/dist/index.css";
// Import merchant data directly
import merchantData from '@/assets/merchant-data.json';

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
      gaugeCircumference: 2 * Math.PI * 50,
    };
  },
  computed: {
    riskScore() {
      return this.selectedMerchant?.riskMetrics?.riskScore ?? 0;
    },
    alertsHeight() {
      const count = this.selectedMerchant?.alerts?.length ?? 0;
      return Math.min(count * 12, 60);
    },
    filteredMerchants() {
      let list = this.merchants;
      
      // Search filter
      if (this.searchQuery.trim()) {
        const q = this.searchQuery.toLowerCase();
        list = list.filter(
          (m) =>
            (m.name || "").toLowerCase().includes(q) ||
            (m.id || "").toString().toLowerCase().includes(q)
        );
      }
      
      // Risk filter
      if (this.filters.riskHigh) {
        list = list.filter((m) => (m.riskMetrics?.riskScore ?? 0) >= 70);
      }
      
      // Status filter
      if (this.filters.statusActive) {
        list = list.filter((m) => m.status === 'active');
      }
      
      // Date range filter
      if (this.filters.dateRangeThisMonth) {
        const thisMonth = new Date().getMonth();
        const thisYear = new Date().getFullYear();
        list = list.filter((m) => {
          if (!m.lastActivity) return false;
          const activityDate = new Date(m.lastActivity);
          return activityDate.getMonth() === thisMonth && 
                 activityDate.getFullYear() === thisYear;
        });
      }
      
      return list;
    },
  },
  methods: {
    selectMerchant(m) {
      this.selectedMerchant = m;
    },
    
    routeToMerchantPage(merchantId) {
      try {
        this.$router.push(`/merchant/${merchantId}`);
      } catch (error) {
        console.error('Navigation error:', error);
        toast.error('Failed to navigate to merchant details', {
          autoClose: 3000,
          position: "top-right",
        });
      }
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

      toast.error(
        `${this.selectedMerchant.name} has been permanently banned!`,
        {
          autoClose: 5000,
          position: "top-right",
          hideProgressBar: false,
          closeOnClick: true,
          pauseOnHover: true,
          draggable: true,
        }
      );

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

      toast.success(
        `${this.selectedMerchant.name} will continue operating normally!`,
        {
          autoClose: 4000,
          position: "top-right",
          hideProgressBar: false,
          closeOnClick: true,
          pauseOnHover: true,
          draggable: true,
        }
      );

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
      if (!dt) return "N/A";
      try {
        const d = new Date(dt);
        return d.toLocaleString();
      } catch {
        return dt;
      }
    },
    
    sparklinePoints(vol) {
      const v = vol ?? 100;
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

    loadMerchantData() {
      try {
        const merchantsList = merchantData?.merchants ?? merchantData?.merchantsList ?? merchantData ?? [];
        this.merchants = Array.isArray(merchantsList) ? merchantsList : [];
        
        if (this.merchants.length > 0) {
          this.selectedMerchant = this.merchants[0];
          console.log("Loaded merchants:", this.merchants.length);
        }
      } catch (error) {
        console.error("Error loading merchant data:", error);
        this.merchants = [];
      }
    },
  },
  
  mounted() {
    this.loadMerchantData();
  },
};
</script>

<style scoped>
/* Teal, white and grey theme */
:root {
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
  --grey-600: #9ca3af;
  --grey-700: #374151;
  --bg: #ffffff;
}

.merchant-dashboard.teal-theme {
  background: #f5f5f5;
  color: #374151;
  min-height: 100vh;
}

/* Layout */
.merchant-dashboard {
  display: grid;
  grid-template-columns: 350px 1fr;
  gap: 20px;
  padding: 20px;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  min-height: 100vh;
  box-sizing: border-box;
}

/* Enhanced Sidebar Panel */
.sidebar {
  background: #ffffff;
  border-radius: 12px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
  border: 1px solid #e5e7eb;
  padding: 20px;
  display: flex;
  flex-direction: column;
  height: fit-content;
  position: sticky;
  top: 20px;
}

.sidebar-header {
  text-align: center;
  margin-bottom: 20px;
  padding-bottom: 15px;
  border-bottom: 2px solid #e6fbf8;
}

.sidebar-header h2 {
  margin: 0 0 5px 0;
  font-size: 20px;
  color: #008080;
  font-weight: 600;
}

.sidebar-subtitle {
  margin: 0;
  color: #6b7280;
  font-size: 13px;
  font-style: italic;
}

.search-filter {
  margin-bottom: 20px;
}

.search-input {
  width: 100%;
  padding: 12px 16px;
  border: 1px solid #14b8a6;
  border-radius: 8px;
  background: #fff;
  color: #374151;
  font-size: 14px;
  box-sizing: border-box;
  transition: all 0.2s ease;
}

.search-input::placeholder {
  color: #9ca3af;
}

.search-input:focus {
  outline: none;
  border-color: #008080;
  box-shadow: 0 0 0 3px rgba(20, 184, 166, 0.15);
}

.filters {
  margin-top: 15px;
}

.filter-title {
  font-weight: 600;
  margin-bottom: 10px;
  color: #008080;
  font-size: 14px;
}

.actual-filters {
  display: flex;
  align-items: center;
  color: #374151;
  font-size: 13px;
  gap: 8px;
  margin-bottom: 8px;
  cursor: pointer;
  /* padding: 4px 0; */
  transition: color 0.2s ease;
}

.actual-filters:hover {
  color: #008080;
}

.actual-filters input[type="checkbox"] {
  cursor: pointer;
  accent-color: #14b8a6;
}

.merchant-list {
  flex: 1;
  max-height: 500px;
  overflow-y: auto;
}

.merchant-list::-webkit-scrollbar {
  width: 6px;
}

.merchant-list::-webkit-scrollbar-track {
  background: #f1f1f1;
  border-radius: 3px;
}

.merchant-list::-webkit-scrollbar-thumb {
  /* background: #14b8a6; */
  border-radius: 3px;
}

.merchant-item {
  margin-bottom: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
  border-radius: 8px;
}

.merchant-item:hover {
  transform: translateX(2px);
}

.merchant-item.selected {
  background: #e6fbf8;
  border-left: 3px solid #14b8a6;
}

.merchant-summary {
  background: linear-gradient(135deg, #008080, #0f9a92);
  border-radius: 8px;
  color: white;
  padding: 12px;
  box-shadow: 0 2px 6px rgba(0, 128, 128, 0.2);
}

.merchant-name {
  font-weight: 600;
  font-size: 16px;
  margin-bottom: 8px;
}

.merchant-details-div {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
}

.merchant-risk {
  font-size: 13px;
  color: rgba(255, 255, 255, 0.9);
  background: rgba(255, 255, 255, 0.1);
  padding: 2px 8px;
  border-radius: 10px;
  font-weight: 500;
}

.merchant-summary-button {
  background: #ffffff;
  color: #008080;
  border: none;
  border-radius: 5px;
  padding: 6px 12px;
  cursor: pointer;
  font-weight: 600;
  font-size: 12px;
  transition: all 0.2s ease;
}

.merchant-summary-button:hover {
  background: #f0fdfa;
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(0, 128, 128, 0.2);
}

/* Details Panel */
.details-panel {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  padding: 0;
}

.empty-state {
  grid-template-columns: 1fr;
  place-items: center;
}

.empty-message {
  text-align: center;
  color: #6b7280;
  padding: 60px 40px;
  background: white;
  border-radius: 12px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
}

.empty-message h3 {
  color: #008080;
  margin-bottom: 10px;
  font-size: 18px;
}

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

.activities ul {
  padding-left: 20px;
  margin: 10px 0;
  color: #374151;
}

.no-alerts {
  color: #9ca3af;
  font-style: italic;
  text-align: center;
  padding: 20px;
}

.action-center-card {
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
  padding: 25px;
}

.action-card-button {
  background: #008080;
  color: white;
  border: none;
  border-radius: 8px;
  padding: 12px 20px;
  margin: 6px;
  width: 200px;
  cursor: pointer;
  font-weight: 500;
  font-size: 14px;
  transition: all 0.2s ease;
}

.action-card-button:hover {
  background: #006666;
  transform: translateY(-1px);
  box-shadow: 0 4px 8px rgba(0, 128, 128, 0.3);
}

.action-card-heading {
  color: #008080;
  margin-bottom: 20px;
  font-size: 16px;
  text-align: center;
  font-weight: 600;
}

/* Screen reader only class */
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

/* Responsive Design */
@media (max-width: 1024px) {
  .merchant-dashboard {
    grid-template-columns: 1fr;
    gap: 15px;
  }
  
  .sidebar {
    position: static;
    max-height: 400px;
  }
  
  .details-panel {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 768px) {
  .merchant-dashboard {
    padding: 15px;
  }
  
  .sidebar {
    padding: 15px;
  }
  
  .card {
    padding: 15px;
  }
  
  .metrics-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 480px) {
  .merchant-dashboard {
    padding: 10px;
  }
  
  .sidebar {
    padding: 12px;
  }
  
  .action-card-button {
    width: 160px;
  }
}
</style>