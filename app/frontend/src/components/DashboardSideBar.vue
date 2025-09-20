<template>
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
        :class="{ selected: selectedMerchantId && m.id === selectedMerchantId }"
        @click="$emit('select-merchant', m)"
        role="option"
      >
        <div class="merchant-summary">
          <div class="merchant-name">{{ m.name }}</div>
          <div class="merchant-details-div">
            <div class="merchant-risk" v-if="m.riskMetrics">
              Risk: {{ m.riskMetrics.riskScore ?? "—" }}
            </div>
            <!-- This button would ideally emit an event to the parent for routing -->
            <button 
              @click.stop="handleViewDetails(m.id)" 
              class="merchant-summary-button"
            >
              View Details
            </button>
          </div>
        </div>
      </div>
    </div>
  </aside>
</template>

<script>
export default {
  name: "DashboardSideBar",
  props: {
    merchants: {
      type: Array,
      default: () => []
    },
    selectedMerchantId: {
      type: [String, Number],
      default: null
    }
  },
  emits: ['select-merchant', 'view-details'], // Emitting view-details event
  data() {
    return {
      searchQuery: "",
      filters: {
        riskHigh: false,
        statusActive: false,
        dateRangeThisMonth: false,
      }
    };
  },
  computed: {
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
      
      if (this.filters.statusActive) {
        list = list.filter((m) => m.status === 'active');
      }
      
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
    }
  },
  methods: {
    handleViewDetails(merchantId) {
      this.$emit('view-details', merchantId); // Emit event for parent to handle routing
    }
  }
};
</script>

<style scoped>
/* Sidebar Styles moved from Dashboard.vue */
.sidebar {
  background: #ffffff;
  border-radius: 12px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
  border: 1px solid #e5e7eb;
  padding: 20px;
  display: flex;
  flex-direction: column;
  height: fit-content;
  position: sticky; /* Make sidebar sticky */
  top: 20px; /* Stick to the top with some padding */
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
  flex: 1; /* Allow list to take available space */
  max-height: 500px; /* Limit height and enable scrolling */
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
  background: #14b8a6; /* Teal thumb */
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
  background: #e6fbf8; /* Light teal background */
  border-left: 3px solid #14b8a6; /* Teal left border */
}

.merchant-summary {
  background: linear-gradient(135deg, #008080, #0f9a92); /* Teal gradient */
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