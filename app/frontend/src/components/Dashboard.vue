<template>
  <div class="merchant-dashboard teal-theme">
    <DashboardSideBar
      :merchants="merchants"
      :selectedMerchant="selectedMerchant"
      :searchQuery="searchQuery"
      :filters="filters"
      @select-merchant="selectMerchant"
      @route-to-merchant="routeToMerchantPage"
      @update:searchQuery="searchQuery = $event"
      @update:filters="filters = $event"
    />

    <section class="details-panel" v-if="selectedMerchant">
      <DashboardMerchantInfo :merchant="selectedMerchant" />
      <DashboardRiskMetrics :merchant="selectedMerchant" />
      <DashboardActionCenter :merchant="selectedMerchant" @action="handleMerchantAction" />
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
import { toast } from "vue3-toastify";
import "vue3-toastify/dist/index.css";
// Import merchant data directly
import merchantData from '@/assets/merchant-data.json';

// Import the new components
import DashboardSideBar from './DashboardSideBar.vue';
import DashboardMerchantInfo from './DashboardMerchantInfo.vue';
import DashboardRiskMetrics from './DashboardRiskMetrics.vue';
import DashboardActionCenter from './DashboardActionCenter.vue';
import DashboardTransactionAnalytics from './DashboardTransactionAnalytics.vue';

export default {
  name: "MerchantDashboard",
  components: {
    DashboardSideBar,
    DashboardMerchantInfo,
    DashboardRiskMetrics,
    DashboardActionCenter,
    DashboardTransactionAnalytics,
  },
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
    };
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

    handleMerchantAction(actionType) {
      if (!this.selectedMerchant) {
        toast.error("No merchant selected for action", {
          autoClose: 3000,
          position: "top-right",
        });
        return;
      }

      switch (actionType) {
        case 'permanent-ban':
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
          break;
        case 'shadow-ban':
          toast.warning(`${this.selectedMerchant.name} has been shadow banned!`, {
            autoClose: 5000,
            position: "top-right",
            hideProgressBar: false,
            closeOnClick: true,
            pauseOnHover: true,
            draggable: true,
          });
          console.log(`Shadow banned merchant: ${this.selectedMerchant.id}`);
          break;
        case 'continue':
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
          break;
        default:
          console.warn(`Unknown action type: ${actionType}`);
      }
    },

    // Removed formatCurrency as it wasn't used in the final version of the original component.
    // If it was intended for use in a future component, it can be added back.

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

<style>
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

/* Responsive Design */
@media (max-width: 1024px) {
  .merchant-dashboard {
    grid-template-columns: 1fr;
    gap: 15px;
  }

  .details-panel {
    grid-template-columns: 1fr;
  }
}

/* Remove duplicate styles if they are already in component styles */
/* If styles are truly global for the dashboard, they should remain here */
/* e.g. the main grid layout, the theme variables */
/* The component-specific styles have been moved to their respective files */
</style>