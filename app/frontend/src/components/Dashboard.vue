<template>
  <div class="merchant-dashboard teal-theme">
    <!-- Left Sidebar -->
    <DashboardSideBar
      :merchants="merchants"
      :selected-merchant-id="selectedMerchant?.id"
      @select-merchant="selectMerchant"
    />

    <!-- Right Details Panel -->
    <section class="details-panel" v-if="selectedMerchant">
      <DashboardMerchantInfo :merchant="selectedMerchant" />
      <DashboardRiskMetrics :merchant="selectedMerchant" />
      <DashboardActionCenter 
        :merchant="selectedMerchant" 
        @merchant-action="handleMerchantAction"
      />
      <!-- Transaction Analytics moved to the end for better layout flow -->
      <DashboardTransactionAnalytics :merchant="selectedMerchant" /> 
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
import DashboardSideBar from './DashboardSideBar.vue';
import DashboardMerchantInfo from './DashboardMerchantInfo.vue';
import DashboardRiskMetrics from './DashboardRiskMetrics.vue';
import DashboardTransactionAnalytics from './DashboardTransactionAnalytics.vue';
import DashboardActionCenter from './DashboardActionCenter.vue'; // Corrected component name
import 'vue3-toastify/dist/index.css';
import merchantData from '@/assets/merchant-data.json'; // Assuming this path

export default {
  name: "Dashboard",
  components: {
    DashboardSideBar,
    DashboardMerchantInfo,
    DashboardRiskMetrics,
    DashboardTransactionAnalytics,
    DashboardActionCenter
  },
  data() {
    return {
      merchants: [],
      selectedMerchant: null,
      searchQuery: "", // Moved from Sidebar component (will be managed here)
      filters: { // Moved from Sidebar component (will be managed here)
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

    // Toastify notification functions (now handled by ActionCenter, but kept for reference if needed)
    notifyPermanentban() {
      if (!this.selectedMerchant) {
        toast.error("No merchant selected for action", { autoClose: 3000, position: "top-right" });
        return;
      }
      toast.error(`${this.selectedMerchant.name} has been permanently banned!`, { autoClose: 5000, position: "top-right", hideProgressBar: false, closeOnClick: true, pauseOnHover: true, draggable: true });
      console.log(`Permanently banned merchant: ${this.selectedMerchant.id}`);
    },
    notifyShadowBan() {
      if (!this.selectedMerchant) {
        toast.error("No merchant selected for action", { autoClose: 3000, position: "top-right" });
        return;
      }
      toast.warning(`${this.selectedMerchant.name} has been shadow banned!`, { autoClose: 5000, position: "top-right", hideProgressBar: false, closeOnClick: true, pauseOnHover: true, draggable: true });
      console.log(`Shadow banned merchant: ${this.selectedMerchant.id}`);
    },
    notifyContinueMerchant() {
      if (!this.selectedMerchant) {
        toast.error("No merchant selected for action", { autoClose: 3000, position: "top-right" });
        return;
      }
      toast.success(`${this.selectedMerchant.name} will continue operating normally!`, { autoClose: 4000, position: "top-right", hideProgressBar: false, closeOnClick: true, pauseOnHover: true, draggable: true });
      console.log(`Continued merchant operations: ${this.selectedMerchant.id}`);
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

    handleMerchantAction(actionData) {
      console.log(`${actionData.action} performed on merchant:`, actionData.merchant.id);
      // You can add additional logic here if needed, e.g., disabling buttons after action
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
  background: #f5f5f5; /* Slightly off-white background */
  color: #374151;
  min-height: 100vh;
}

/* Layout */
.merchant-dashboard {
  display: grid;
  grid-template-columns: 350px 1fr; /* Wider sidebar */
  gap: 20px;
  padding: 20px;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  min-height: 100vh;
  box-sizing: border-box;
}

/* Details Panel */
.details-panel {
  display: grid;
  grid-template-columns: 1fr 1fr; /* Two columns for details */
  gap: 20px;
  padding: 0;
}

.empty-state {
  grid-template-columns: 1fr; /* Center content when empty */
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

/* Shared Card Styles (moved from components) */
.card {
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  background: #fff;
  padding: 20px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
  transition: all 0.3s ease;
  height: fit-content; /* Adjust height to content */
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
  border-bottom: 2px solid #e6fbf8; /* Subtle underline */
  padding-bottom: 8px;
}

.card-content {
  font-size: 14px;
  color: #374151;
}

/* Responsive Design */
@media (max-width: 1024px) {
  .merchant-dashboard {
    grid-template-columns: 1fr; /* Stack sidebar and details */
    gap: 15px;
  }
  
  .sidebar {
    position: static; /* Remove sticky behavior on smaller screens */
    max-height: 400px; /* Limit sidebar height */
  }
  
  .details-panel {
    grid-template-columns: 1fr; /* Stack details cards */
  }
}

@media (max-width: 768px) {
  .merchant-dashboard {
    padding: 15px;
  }
  
  .card {
    padding: 15px;
  }
  
  .metrics-grid {
    grid-template-columns: 1fr; /* Single column for metrics */
  }
}

@media (max-width: 480px) {
  .merchant-dashboard {
    padding: 10px;
  }
  
  .action-card-button {
    width: 160px; /* Smaller buttons on very small screens */
  }
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
</style>