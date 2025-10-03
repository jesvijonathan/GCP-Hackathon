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
            <div class="merchant-name">
              <!-- Status dots: red (inactive) highest priority, then yellow (restrictions) -->
              <span
                v-if="m.activation_flag === false || m.status==='inactive'"
                class="status-dot inactive-dot"
                title="Inactive"
              ></span>
              <span
                v-else-if="m.has_restrictions || (m.restrictions && Object.keys(m.restrictions||{}).length)"
                class="status-dot restriction-dot"
                title="Restrictions Active"
              ></span>
              {{ m.name }}
            </div>
            <div class="selected-badge" v-if="selectedMerchant && m.id === selectedMerchant.id">
              ✓
            </div>
          </div>
          <div class="merchant-details-div">
            <div class="merchant-status-chip" :class="statusClass(m)">
              {{ statusLabel(m) }}
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
  methods: {
    statusLabel(m){
      if(m.activation_flag === false || m.status === 'inactive') return 'Inactive';
      if(m.has_restrictions || (m.restrictions && Object.keys(m.restrictions||{}).length)) return 'Restricted';
      return 'Active';
    },
    statusClass(m){
      if(m.activation_flag === false || m.status === 'inactive') return 'inactive';
      if(m.has_restrictions || (m.restrictions && Object.keys(m.restrictions||{}).length)) return 'restricted';
      return 'active';
    },
    
    confirmDelete(m){
      if(!m || !m.id) return;
      if(confirm(`Remove merchant '${m.name}' from dashboard? This deletes only profile & risk data.`)){
        this.$emit('delete-merchant', m.id);
      }
    }
  },
  emits: ['select-merchant', 'route-to-merchant', 'delete-merchant', 'update:searchQuery', 'update:filters'],
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

/* Merchant list & improved scrollbar */
.merchant-list {
  flex: 1;
  max-height: calc(100vh - 330px); /* dynamic height so it uses viewport space */
  overflow-y: auto;
  overflow-x: hidden;
  position: relative;
  padding-right: 4px; /* keep content from touching scrollbar */
  scrollbar-width: thin;            /* Firefox */
  scrollbar-color: #14b8a6 #f1f5f9; /* Firefox */
  -webkit-overflow-scrolling: touch;
  background: linear-gradient(#ffffff, #ffffff) padding-box;
}

/* WebKit scrollbars */
.merchant-list::-webkit-scrollbar { width: 10px; }
.merchant-list::-webkit-scrollbar-track { background: #f1f5f9; border-radius: 10px; }
.merchant-list::-webkit-scrollbar-thumb {
  background: linear-gradient(180deg,#14b8a6,#0f9a92);
  border-radius: 10px;
  border: 2px solid #f1f5f9; /* creates padding effect */
}
.merchant-list::-webkit-scrollbar-thumb:hover { background: linear-gradient(180deg,#0f9a92,#0b7f7b); }

/* Subtle top/bottom fade indicators (static, low opacity so acceptable if always visible) */
.merchant-list::before,
.merchant-list::after {
  content: '';
  position: sticky;
  left: 0; right: 0;
  height: 18px;
  pointer-events: none;
  z-index: 2;
}
.merchant-list::before { top: 0; background: linear-gradient(to bottom, rgba(255,255,255,0.95), rgba(255,255,255,0)); }
.merchant-list::after { bottom: 0; background: linear-gradient(to top, rgba(255,255,255,0.95), rgba(255,255,255,0)); }

/* Merchant card styles */
.merchant-item { margin-bottom: 10px; cursor: pointer; border-radius: 10px; position: relative; }
.merchant-item:focus-visible { outline: 2px solid #0d9488; outline-offset: 2px; }

.merchant-summary {
  background: #ffffff;
  border: 1px solid #d1e7e5;
  border-radius: 10px;
  color: #065f5b;
  padding: 12px 14px 10px;
  box-shadow: 0 2px 4px rgba(14,116,144,0.08), 0 1px 2px rgba(0,0,0,0.04);
  transition: border-color .18s ease, box-shadow .18s ease, transform .18s ease;
  backdrop-filter: blur(2px);
}

.merchant-item:hover .merchant-summary {
  border-color: #14b8a6;
  box-shadow: 0 4px 10px rgba(20,184,166,0.18);
  transform: translateY(-2px);
}

.merchant-item.selected .merchant-summary {
  border: 2px solid #0d9488;
  box-shadow: 0 6px 16px rgba(13,148,136,0.25);
  background: linear-gradient(135deg,#f0fdfa,#ecfeff);
}

/* Subtle accent bar (left) for selected */
.merchant-item.selected .merchant-summary::before {
  content: '';
  position: absolute;
  inset: 0 0 0 0;
  border-radius: 10px;
  padding-left: 4px;
  background: linear-gradient(90deg, rgba(13,148,136,0.5), rgba(13,148,136,0) 55%);
  mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  -webkit-mask-composite: xor;
  mask-composite: exclude;
  pointer-events: none;
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
  display:flex;
  align-items:center;
  gap:6px;
}
/* Inactive status indicator */
.status-dot { width:10px; height:10px; border-radius:50%; display:inline-block; flex-shrink:0; }
.inactive-dot { background:#dc2626; box-shadow:0 0 0 2px #fee2e2; }
.restriction-dot { background:#f59e0b; box-shadow:0 0 0 2px #fef3c7; }

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

.merchant-row-actions { display:flex; align-items:center; gap:6px; }
.merchant-delete-button {
  background:#fff5f5;
  color:#b91c1c;
  border:1px solid #fecaca;
  width:30px; height:30px; border-radius:6px;
  cursor:pointer; font-size:14px; font-weight:700; line-height:1;
  display:flex; align-items:center; justify-content:center;
  transition: background .18s ease, color .18s ease, box-shadow .18s ease;
}
.merchant-delete-button:hover { background:#dc2626; color:#ffffff; box-shadow:0 2px 6px rgba(220,38,38,0.35); }
.merchant-delete-button:active { transform:translateY(1px); }

.merchant-status-chip {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: .4px;
  padding: 4px 10px 3px;
  border-radius: 999px;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  border: 1px solid transparent;
  text-transform: uppercase;
  position: relative;
  line-height: 1.1;
}
.merchant-status-chip.active { background:#e6fbf8; color:#065f5b; border-color:#14b8a6; }
.merchant-status-chip.restricted { background:#fff7ed; color:#9a5b05; border-color:#f59e0b; }
.merchant-status-chip.inactive { background:#fef2f2; color:#991b1b; border-color:#dc2626; }
.merchant-status-chip.inactive::before, .merchant-status-chip.restricted::before, .merchant-status-chip.active::before {
  content:''; width:6px; height:6px; border-radius:50%;
  background: currentColor; opacity:.85; box-shadow:0 0 0 2px rgba(255,255,255,0.7);
}

.merchant-summary-button {
  background: #0d9488;
  color: #ffffff;
  border: none;
  border-radius: 6px;
  padding: 6px 11px;
  cursor: pointer;
  font-weight: 600;
  font-size: 11px;
  line-height: 1.1;
  transition: background .18s ease, box-shadow .18s ease, transform .18s ease;
  box-shadow: 0 1px 3px rgba(0,0,0,0.15);
}
.merchant-summary-button:hover { background:#0b7f7b; box-shadow:0 3px 8px rgba(0,0,0,0.25); transform:translateY(-1px); }
.merchant-summary-button:active { transform:translateY(0); box-shadow:0 2px 4px rgba(0,0,0,0.18); }

/* Status dots adjust for light card */
.inactive-dot { background:#dc2626; box-shadow:0 0 0 2px #fee2e2; }
.restriction-dot { background:#f59e0b; box-shadow:0 0 0 2px #fef3c7; }

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