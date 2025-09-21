<template>
  <div class="card transaction-analytics-card" v-if="merchant">
    <div class="card-header">
      <h3>Recent Activities & Alerts</h3>
    </div>
    <div class="card-content activities">
      <p>
        <strong>Last Activity:</strong>
        {{ formatDate(merchant.lastActivity) }}
      </p>
      <ul v-if="merchant.alerts && merchant.alerts.length > 0">
        <li v-for="(al, idx) in merchant.alerts" :key="idx">
          <strong>{{ al.type }}</strong>: {{ al.message }}
          <em>({{ al.date }})</em> - Severity: {{ al.severity }}
        </li>
      </ul>
      <p v-else class="no-alerts">No recent alerts</p>
    </div>
  </div>
</template>

<script>
export default {
  name: "DashboardTransactionAnalytics",
  props: {
    merchant: {
      type: Object,
      required: true,
    },
  },
  methods: {
    formatDate(dt) {
      if (!dt) return "N/A";
      try {
        const d = new Date(dt);
        return d.toLocaleString();
      } catch {
        return dt;
      }
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
</style>