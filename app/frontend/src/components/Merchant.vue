<template>
  <div class="merchant-detail-container">
    <div v-if="loading" class="loading">Loading merchant details...</div>
    <div class="side-panel">
      <div class="merchant-header">
        <h1>{{ merchant.name }}</h1>
        <span class="merchant-id">ID: {{ merchant.id }}</span>
      </div>
      <button @click="goBack" class="back-btn">← Back to Dashboard</button>

      <div class="notification">Notifications</div>

      <div class="action-center-dropdown">
        <button>Permanent Ban </button>
        <button>Shadow Ban</button>
        <button>Restrict Merchant</button>
      </div>
      <div class="actions-taken">
        <h1>Actions Taken On {{ merchant.name }}</h1>
        <p class="actions-taken-data">No actions taken</p>
    </div>
    </div>

    <div v-if="merchant" class="merchant-detail">
      <!-- Merchant Details -->

      <div class="merchant-info">
        <div class="info-section">
          <h3>Contact Information</h3>
          <p><strong>Email:</strong> {{ merchant.email || "N/A" }}</p>
          <p><strong>Phone:</strong> {{ merchant.phone || "N/A" }}</p>
          <p><strong>Address:</strong> {{ merchant.address || "N/A" }}</p>
        </div>

        <div class="info-section">
          <h3>Business Details</h3>
          <p><strong>Category:</strong> {{ merchant.category || "N/A" }}</p>
          <p>
            <strong>Business Type:</strong> {{ merchant.businessType || "N/A" }}
          </p>
          <p>
            <strong>Status:</strong>
            <span
              :class="['status', (merchant.status || 'unknown').toLowerCase()]"
            >
              {{ merchant.status || "Unknown" }}
            </span>
          </p>
          <p>
            <strong>Revenue:</strong>
            {{
              merchant.revenue ? `$${merchant.revenue.toLocaleString()}` : "N/A"
            }}
          </p>
        </div>

        <!-- Risk Metrics Section -->
        <div class="info-section" v-if="merchant.riskMetrics">
          <h3>Risk Metrics</h3>
          <p>
            <strong>Risk Score:</strong>
            {{ merchant.riskMetrics.riskScore || "N/A" }}
          </p>
          <p>
            <strong>Risk Level:</strong>
            {{ getRiskLevel(merchant.riskMetrics.riskScore) }}
          </p>
        </div>

        <!-- Description Section -->
        <div class="info-section" v-if="merchant.description">
          <h3>Description</h3>
          <p>{{ merchant.description }}</p>
        </div>

        <!-- Recent Alerts Section -->
        <div
          class="info-section"
          v-if="merchant.alerts && merchant.alerts.length > 0"
        >
          <h3>Recent Alerts</h3>
          <div class="alerts-list">
            <div
              v-for="(alert, index) in merchant.alerts.slice(0, 3)"
              :key="index"
              class="alert-item"
              :class="alert.severity.toLowerCase()"
            >
              <strong>{{ alert.type }}:</strong> {{ alert.message }}
              <span class="alert-date">({{ formatDate(alert.date) }})</span>
            </div>
          </div>
        </div>

        <!-- Last Activity Section -->
        <div class="info-section">
          <h3>Activity Information</h3>
          <p>
            <strong>Last Activity:</strong>
            {{ formatDate(merchant.lastActivity) }}
          </p>
          <p><strong>Created:</strong> {{ formatDate(merchant.createdAt) }}</p>
        </div>
      </div>
    </div>

    <div v-else class="error">
      <h2>Merchant not found</h2>
      <p>The merchant with ID "{{ $route.params.id }}" could not be found.</p>
      <button @click="goBack" class="back-btn">← Back to Dashboard</button>
    </div>
  </div>
</template>

<script>
import merchantData from "@/assets/merchant-data.json";
import NewsCarousel from "./NewsCarousel.vue";

export default {
  name: "MerchantDetail",
  data() {
    return {
      merchant: null,
      loading: true,
    };
  },
  computed: {
    merchantId() {
      // Get merchant ID from route parameters
      return this.$route.params.id;
    },
  },
  created() {
    this.fetchMerchantDetails();
  },
  watch: {
    // Watch for route changes
    "$route.params.id"(newId) {
      if (newId) {
        this.fetchMerchantDetails();
      }
    },
  },
  methods: {
    fetchMerchantDetails() {
      this.loading = true;

      // Simulate API call delay (remove in production)
      setTimeout(() => {
        try {
          // Handle different possible data structures
          const merchants =
            merchantData?.merchants ||
            merchantData?.merchantsList ||
            merchantData ||
            [];

          // Find merchant by ID
          this.merchant = merchants.find(
            (merchant) => merchant.id.toString() === this.merchantId
          );

          if (this.merchant) {
            console.log("Found merchant:", this.merchant);
          } else {
            console.log("Merchant not found with ID:", this.merchantId);
          }
        } catch (error) {
          console.error("Error loading merchant data:", error);
          this.merchant = null;
        }

        this.loading = false;
      }, 300);
    },

    goBack() {
      this.$router.push("/dashboard");
    },

    formatDate(dateString) {
      if (!dateString) return "N/A";
      try {
        const date = new Date(dateString);
        return date.toLocaleDateString("en-US", {
          year: "numeric",
          month: "long",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        });
      } catch (error) {
        return dateString;
      }
    },

    getRiskLevel(score) {
      if (!score) return "Unknown";
      if (score >= 80) return "High Risk";
      if (score >= 60) return "Medium Risk";
      if (score >= 40) return "Low Risk";
      return "Very Low Risk";
    },
  },
};
</script>

<style scoped>
.merchant-detail-container {
  margin: 0 auto;
  padding: 20px;
  font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
  background-color: #f5f5f5;
  min-height: 100vh;
  display: flex;
  flex-direction: row;
  gap: 20px;
}

.side-panel {
  min-width: 280px;
  max-width: 300px;
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

.merchant-header {
  background: linear-gradient(135deg, #008080, #20b2aa);
  color: white;
  padding: 20px;
  border-radius: 8px;
  text-align: center;
  box-shadow: 0 4px 12px rgba(0, 128, 128, 0.2);
  margin-bottom: 20px;
}

.merchant-header h1 {
  font-size: 18px;
  font-weight: 600;
  margin: 0 0 8px 0;
}

.merchant-id {
  background: rgba(255, 255, 255, 0.2);
  padding: 4px 12px;
  border-radius: 15px;
  font-size: 0.85em;
  font-weight: 500;
  backdrop-filter: blur(10px);
}

.back-btn {
  background: #008080;
  color: white;
  border: none;
  padding: 12px 16px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  font-weight: 500;
  transition: all 0.2s ease;
  margin-bottom: 20px;
  width: 100%;
}

.back-btn:hover {
  background: #006666;
  transform: translateY(-1px);
  box-shadow: 0 2px 8px rgba(0, 128, 128, 0.3);
}

.notification {
  background: #f0fdfa;
  color: #008080;
  border: 1px solid #14b8a6;
  padding: 12px 16px;
  border-radius: 6px;
  margin-bottom: 20px;
  font-size: 14px;
  font-weight: 600;
  text-align: center;
  box-shadow: 0 2px 4px rgba(20, 184, 166, 0.1);
}

.action-center-dropdown {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 20px;
}

.action-center-dropdown button {
  padding: 10px 16px;
  border: 1px solid #14b8a6;
  border-radius: 6px;
  background: #ffffff;
  color: #008080;
  font: 13px "Segoe UI", sans-serif;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
  width: 100%;
}

.action-center-dropdown button:hover {
  background: #008080;
  color: #ffffff;
  transform: translateY(-1px);
  box-shadow: 0 2px 6px rgba(0, 128, 128, 0.2);
}

.action-center-dropdown button:active {
  transform: translateY(0);
}

.actions-taken{
    text-align: center;
    color: #666;
    font-size: 14px;
}

.actions-taken-data{
    color: #999;
    font-size: 13px;
    font-style: italic;
}

.actions-taken h1{
    text-align: center;
    color: #008080;
    font-size: 13px;
    font-style: italic;
}

/* Actions section header */
.side-panel > div:last-child {
  color: #008080;
  font-weight: 600;
  font-size: 14px;
  text-align: center;
  margin-top: 10px;
  padding-top: 15px;
  border-top: 1px solid #e5e7eb;
}

.merchant-detail {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 20px;
  max-width: calc(100% - 320px);
}

.loading,
.error {
  text-align: center;
  padding: 40px;
  color: #666;
  background: white;
  border-radius: 12px;
  box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
}

.merchant-info {
  display: grid;
  gap: 20px;
  grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
}

.info-section {
  background: white;
  padding: 25px;
  border-radius: 10px;
  box-shadow: 0 3px 12px rgba(0, 0, 0, 0.08);
  border-left: 4px solid #008080;
  transition: all 0.3s ease;
}

.info-section:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 20px rgba(0, 0, 0, 0.12);
}

.info-section h3 {
  color: #008080;
  margin-top: 0;
  margin-bottom: 15px;
  font-size: 1.2em;
  font-weight: 600;
  border-bottom: 2px solid #e6fbf8;
  padding-bottom: 8px;
}

.info-section p {
  margin: 10px 0;
  line-height: 1.6;
  color: #374151;
  font-size: 14px;
}

.status {
  padding: 4px 12px;
  border-radius: 15px;
  font-size: 0.8em;
  font-weight: bold;
  display: inline-block;
}

.status.active {
  background: #d4edda;
  color: #155724;
}

.status.inactive {
  background: #f8d7da;
  color: #721c24;
}

.status.pending {
  background: #fff3cd;
  color: #856404;
}

.status.unknown {
  background: #e2e3e5;
  color: #383d41;
}

.alerts-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.alert-item {
  padding: 10px;
  border-radius: 6px;
  border-left: 3px solid #ccc;
  background: #f8f9fa;
  font-size: 13px;
}

.alert-item.high {
  border-left-color: #dc3545;
  background: #f8d7da;
}

.alert-item.medium {
  border-left-color: #ffc107;
  background: #fff3cd;
}

.alert-item.low {
  border-left-color: #28a745;
  background: #d4edda;
}

.alert-date {
  font-style: italic;
  color: #6c757d;
  font-size: 0.9em;
}

.error {
  background: #f8d7da;
  color: #721c24;
  padding: 40px;
  border-radius: 12px;
  border: 1px solid #f5c6cb;
  text-align: center;
}

.error h2 {
  margin-bottom: 15px;
  color: #721c24;
}

/* Responsive Design */
@media (max-width: 1024px) {
  .merchant-detail-container {
    flex-direction: column;
    gap: 15px;
  }
  
  .side-panel {
    min-width: 100%;
    max-width: 100%;
    position: static;
  }
  
  .merchant-detail {
    max-width: 100%;
  }
}

@media (max-width: 768px) {
  .merchant-detail-container {
    padding: 15px;
  }
  
  .side-panel {
    padding: 15px;
  }
  
  .merchant-header {
    padding: 15px;
  }
  
  .merchant-info {
    grid-template-columns: 1fr;
    gap: 15px;
  }
  
  .info-section {
    padding: 20px;
  }
}

@media (max-width: 480px) {
  .merchant-detail-container {
    padding: 10px;
  }
  
  .side-panel {
    padding: 12px;
  }
  
  .merchant-header {
    padding: 12px;
  }
  
  .merchant-header h1 {
    font-size: 16px;
  }
  
  .info-section {
    padding: 15px;
  }
}
</style>