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
        :value="searchQuery"
        @input="$emit('update:searchQuery', $event.target.value)"
        placeholder="Search merchants..."
        class="search-input"
      />
      <div class="filters">
        <div class="filter-title">Filter Options:</div>
        <label class="actual-filters">
          <input
            type="checkbox"
            :checked="filters.riskHigh"
            @change="$emit('update:filters', { ...filters, riskHigh: $event.target.checked })"
          />
          <span class="checkmark"></span>
          High Risk Level
        </label>
        <label class="actual-filters">
          <input
            type="checkbox"
            :checked="filters.statusActive"
            @change="$emit('update:filters', { ...filters, statusActive: $event.target.checked })"
          />
          <span class="checkmark"></span>
          Active Status
        </label>
        <label class="actual-filters">
          <input
            type="checkbox"
            :checked="filters.dateRangeThisMonth"
            @change="$emit('update:filters', { ...filters, dateRangeThisMonth: $event.target.checked })"
          />
          <span class="checkmark"></span>
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
        @click="$emit('select-merchant', m)"
        role="option"
      >
        <div class="merchant-summary">
          <div class="merchant-header">
            <div class="merchant-name">{{ m.name }}</div>
            <div class="selected-badge" v-if="selectedMerchant && m.id === selectedMerchant.id">
              ✓
            </div>
          </div>
          <div class="merchant-details-div">
            <div class="merchant-risk" v-if="m.riskMetrics">
              Risk: {{ m.riskMetrics.riskScore ?? "—" }}
            </div>
            <button
              @click.stop="$emit('route-to-merchant', m.id)"
              class="merchant-summary-button"
            >
              View Details
            </button>
          </div>
        </div>
      </div>
    </div>

    <div class="results-footer">
      {{ filteredMerchants.length }} merchant{{ filteredMerchants.length !== 1 ? 's' : '' }}
    </div>
  </aside>
</template>

<script>
export default {
  name: "DashboardSideBar",
  props: {
    merchants: {
      type: Array,
      required: true,
    },
    selectedMerchant: {
      type: Object,
      default: null,
    },
    searchQuery: {
      type: String,
      default: "",
    },
    filters: {
      type: Object,
      required: true,
      default: () => ({
        riskHigh: false,
        statusActive: false,
        dateRangeThisMonth: false,
      }),
    },
  },
  computed: {
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
  emits: ['select-merchant', 'route-to-merchant', 'update:searchQuery', 'update:filters'],
};
</script>

<style scoped>
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
  border: 1.5px solid #14b8a6;
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
  background: #f8fafa;
  border-radius: 8px;
  padding: 15px;
  border: 1px solid #e6fbf8;
}

.filter-title {
  font-weight: 600;
  margin-bottom: 12px;
  color: #008080;
  font-size: 14px;
}

.actual-filters {
  display: flex;
  align-items: center;
  color: #374151;
  font-size: 13px;
  gap: 10px;
  margin-bottom: 10px;
  cursor: pointer;
  transition: color 0.2s ease;
  position: relative;
  padding-left: 0;
}

.actual-filters:hover {
  color: #008080;
}

.actual-filters input[type="checkbox"] {
  opacity: 0;
  position: absolute;
  width: 0;
  height: 0;
}

.checkmark {
  width: 16px;
  height: 16px;
  border: 2px solid #14b8a6;
  border-radius: 3px;
  display: inline-block;
  position: relative;
  transition: all 0.2s ease;
  background: white;
}

.actual-filters input[type="checkbox"]:checked + .checkmark {
  background: #14b8a6;
  border-color: #008080;
}

.actual-filters input[type="checkbox"]:checked + .checkmark::after {
  content: '✓';
  position: absolute;
  color: white;
  font-size: 11px;
  font-weight: bold;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
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
  background: #14b8a6;
  border-radius: 3px;
}

.merchant-item {
  margin-bottom: 10px;
  cursor: pointer;
  transition: all 0.2s ease;
  border-radius: 8px;
}

.merchant-item:hover {
  transform: translateX(2px);
}

.merchant-item.selected {
  transform: translateX(4px);
}

.merchant-item.selected .merchant-summary {
  background: linear-gradient(135deg, #008080, #14b8a6);
  box-shadow: 0 4px 12px rgba(0, 128, 128, 0.25);
  border-left: 4px solid #0d9488;
}

.merchant-summary {
  background: linear-gradient(135deg, #008080, #0f9a92);
  border-radius: 8px;
  color: white;
  padding: 14px;
  box-shadow: 0 2px 8px rgba(0, 128, 128, 0.2);
  transition: all 0.2s ease;
}

.merchant-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.merchant-name {
  font-weight: 600;
  font-size: 16px;
}

.selected-badge {
  background: rgba(255, 255, 255, 0.2);
  border-radius: 50%;
  width: 22px;
  height: 22px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: bold;
  border: 1px solid rgba(255, 255, 255, 0.3);
  animation: pulse 2s infinite;
}

@keyframes pulse {
  0% {
    box-shadow: 0 0 0 0 rgba(255, 255, 255, 0.4);
  }
  70% {
    box-shadow: 0 0 0 6px rgba(255, 255, 255, 0);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(255, 255, 255, 0);
  }
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
  background: rgba(255, 255, 255, 0.15);
  padding: 4px 8px;
  border-radius: 10px;
  font-weight: 500;
  border: 1px solid rgba(255, 255, 255, 0.2);
}

.merchant-summary-button {
  background: #ffffff;
  color: #008080;
  border: none;
  border-radius: 6px;
  padding: 7px 12px;
  cursor: pointer;
  font-weight: 600;
  font-size: 12px;
  transition: all 0.2s ease;
  border: 1px solid rgba(0, 128, 128, 0.1);
}

.merchant-summary-button:hover {
  background: #f0fdfa;
  transform: translateY(-1px);
  box-shadow: 0 3px 8px rgba(0, 128, 128, 0.2);
}

.results-footer {
  margin-top: 15px;
  padding-top: 15px;
  border-top: 1px solid #e6fbf8;
  text-align: center;
  color: #6b7280;
  font-size: 12px;
  background: #f8fafa;
  padding: 10px;
  border-radius: 6px;
  font-weight: 500;
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

@media (max-width: 1024px) {
  .sidebar {
    position: static;
    max-height: 400px;
  }
}
</style>