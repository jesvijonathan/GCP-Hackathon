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
        <button @click="openPermanentBanModal">Permanent Ban</button>
        <button @click="openShadowBanModal">Shadow Ban</button>
        <button @click="openRestrictModal">Restrict Merchant</button>
      </div>
      <div class="actions-taken">
        <h1>Actions Taken On {{ merchant.name }}</h1>
        <p class="actions-taken-data">
          {{
            actionsTaken.length > 0
              ? actionsTaken.join(", ")
              : "No actions taken"
          }}
        </p>
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

    <!-- Permanent Ban Confirmation Modal -->
    <div
      v-if="showPermanentBanModal"
      class="modal-overlay"
      @click="closePermanentBanModal"
    >
      <div class="modal-content" @click.stop>
        <div class="modal-header">
          <h3>Confirm Permanent Ban</h3>
          <button @click="closePermanentBanModal" class="close-btn">
            &times;
          </button>
        </div>
        <div class="modal-body">
          <div class="warning-icon">⚠️</div>
          <p class="warning-text">
            Are you sure you want to permanently ban
            <strong>{{ merchant.name }}</strong
            >?
          </p>
          <p class="warning-subtext">
            This action cannot be undone. The merchant will be completely
            blocked from all services.
          </p>
        </div>
        <div class="modal-footer">
          <button @click="closePermanentBanModal" class="btn-cancel">
            Cancel
          </button>
          <button @click="confirmPermanentBan" class="btn-danger">
            Permanent Ban
          </button>
        </div>
      </div>
    </div>

    <!-- Shadow Ban Confirmation Modal -->
    <div
      v-if="showShadowBanModal"
      class="modal-overlay"
      @click="closeShadowBanModal"
    >
      <div class="modal-content" @click.stop>
        <div class="modal-header">
          <h3>Confirm Shadow Ban</h3>
          <button @click="closeShadowBanModal" class="close-btn">
            &times;
          </button>
        </div>
        <div class="modal-body">
          <div class="warning-icon">🔒</div>
          <p class="warning-text">
            Are you sure you want to shadow ban
            <strong>{{ merchant.name }}</strong
            >?
          </p>
          <p class="warning-subtext">
            The merchant will have limited visibility and reduced functionality.
          </p>
        </div>
        <div class="modal-footer">
          <button @click="closeShadowBanModal" class="btn-cancel">
            Cancel
          </button>
          <button @click="confirmShadowBan" class="btn-warning">
            Shadow Ban
          </button>
        </div>
      </div>
    </div>

    <!-- Restrict Merchant Modal -->
    <div
      v-if="showRestrictModal"
      class="modal-overlay"
      @click="closeRestrictModal"
    >
      <div class="modal-content large" @click.stop>
        <div class="modal-header">
          <h3>Restrict Merchant: {{ merchant.name }}</h3>
          <button @click="closeRestrictModal" class="close-btn">&times;</button>
        </div>
        <div class="modal-body">
          <form @submit.prevent="confirmRestriction" class="restriction-form">
            <!-- Transaction Limits -->
            <div class="form-section">
              <h4>Transaction Limits</h4>
              <div class="form-row">
                <div class="form-group">
                  <label for="dailyLimit">Daily Transaction Limit ($)</label>
                  <input
                    id="dailyLimit"
                    v-model.number="restrictionData.dailyTransactionLimit"
                    type="number"
                    class="form-input"
                    placeholder="e.g., 1000"
                    min="0"
                  />
                </div>
                <div class="form-group">
                  <label for="monthlyLimit"
                    >Monthly Transaction Limit ($)</label
                  >
                  <input
                    id="monthlyLimit"
                    v-model.number="restrictionData.monthlyTransactionLimit"
                    type="number"
                    class="form-input"
                    placeholder="e.g., 25000"
                    min="0"
                  />
                </div>
              </div>
            </div>

            <!-- Category Restrictions -->
            <div class="form-section">
              <h4>Restricted Categories</h4>
              <div class="form-group">
                <label for="categoryCodes">Merchant Category Codes (MCC)</label>
                <div class="category-selector">
                  <div class="selected-categories">
                    <span
                      v-for="code in restrictionData.restrictedCategoryCodes"
                      :key="code"
                      class="category-tag"
                    >
                      {{ code }}
                      <button
                        type="button"
                        @click="removeCategoryCode(code)"
                        class="remove-tag"
                      >
                        &times;
                      </button>
                    </span>
                  </div>
                  <select
                    v-model="selectedCategoryCode"
                    @change="addCategoryCode"
                    class="form-select"
                  >
                    <option value="">Select category to restrict</option>
                    <option value="5541">Gas Stations</option>
                    <option value="5542">Automated Fuel Dispensers</option>
                    <option value="5812">Eating Places, Restaurants</option>
                    <option value="5814">Fast Food Restaurants</option>
                    <option value="5921">
                      Package Stores-Beer, Wine, Liquor
                    </option>
                    <option value="5993">Cigar Stores and Stands</option>
                    <option value="6010">Financial Institutions</option>
                    <option value="6011">ATMs</option>
                    <option value="7995">Betting/Casino Gambling</option>
                  </select>
                </div>
              </div>
            </div>

            <!-- Additional Restrictions -->
            <div class="form-section">
              <h4>Additional Restrictions</h4>
              <div class="form-group">
                <label for="maxTransactionAmount"
                  >Maximum Single Transaction ($)</label
                >
                <input
                  id="maxTransactionAmount"
                  v-model.number="restrictionData.maxTransactionAmount"
                  type="number"
                  class="form-input"
                  placeholder="e.g., 500"
                  min="0"
                />
              </div>
              <div class="form-group">
                <label for="allowedCountries">Allowed Countries</label>
                <input
                  id="allowedCountries"
                  v-model="restrictionData.allowedCountries"
                  type="text"
                  class="form-input"
                  placeholder="e.g., US, CA, GB (comma-separated)"
                />
              </div>
              <div class="form-group">
                <label class="checkbox-label">
                  <input
                    v-model="restrictionData.requireAdditionalVerification"
                    type="checkbox"
                    class="form-checkbox"
                  />
                  Require additional verification for transactions
                </label>
              </div>
              <div class="form-group">
                <label class="checkbox-label">
                  <input
                    v-model="restrictionData.blockInternationalTransactions"
                    type="checkbox"
                    class="form-checkbox"
                  />
                  Block international transactions
                </label>
              </div>
            </div>

            <!-- Reason -->
            <div class="form-section">
              <h4>Restriction Reason</h4>
              <div class="form-group">
                <label for="reason">Reason for Restriction</label>
                <textarea
                  id="reason"
                  v-model="restrictionData.reason"
                  class="form-textarea"
                  placeholder="Explain the reason for these restrictions..."
                  rows="3"
                  required
                ></textarea>
              </div>
            </div>

            <!-- Duration -->
            <div class="form-section">
              <h4>Restriction Duration</h4>
              <div class="form-row">
                <div class="form-group">
                  <label for="startDate">Start Date</label>
                  <input
                    id="startDate"
                    v-model="restrictionData.startDate"
                    type="date"
                    class="form-input"
                    required
                  />
                </div>
                <div class="form-group">
                  <label for="endDate">End Date (Optional)</label>
                  <input
                    id="endDate"
                    v-model="restrictionData.endDate"
                    type="date"
                    class="form-input"
                  />
                </div>
              </div>
            </div>
          </form>
        </div>
        <div class="modal-footer">
          <button @click="closeRestrictModal" class="btn-cancel">Cancel</button>
          <button @click="confirmRestriction" class="btn-primary">
            Apply Restrictions
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import merchantData from "@/assets/merchant-data.json";
import { toast } from "vue3-toastify";
import "vue3-toastify/dist/index.css";

export default {
  name: "MerchantDetail",
  data() {
    return {
      merchant: null,
      loading: true,
      showPermanentBanModal: false,
      showShadowBanModal: false,
      showRestrictModal: false,
      selectedCategoryCode: "",
      actionsTaken: [],
      restrictionData: {
        dailyTransactionLimit: null,
        monthlyTransactionLimit: null,
        maxTransactionAmount: null,
        restrictedCategoryCodes: [],
        allowedCountries: "",
        requireAdditionalVerification: false,
        blockInternationalTransactions: false,
        reason: "",
        startDate: new Date().toISOString().split("T")[0],
        endDate: "",
      },
    };
  },
  computed: {
    merchantId() {
      return this.$route.params.id;
    },
  },
  created() {
    this.fetchMerchantDetails();
  },
  watch: {
    "$route.params.id"(newId) {
      if (newId) {
        this.fetchMerchantDetails();
      }
    },
  },
  methods: {
    fetchMerchantDetails() {
      this.loading = true;

      setTimeout(() => {
        try {
          const merchants =
            merchantData?.merchants ||
            merchantData?.merchantsList ||
            merchantData ||
            [];

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

    // Modal methods
    openPermanentBanModal() {
      this.showPermanentBanModal = true;
    },

    closePermanentBanModal() {
      this.showPermanentBanModal = false;
    },

    openShadowBanModal() {
      this.showShadowBanModal = true;
    },

    closeShadowBanModal() {
      this.showShadowBanModal = false;
    },

    openRestrictModal() {
      this.showRestrictModal = true;
      // Reset form data
      this.restrictionData = {
        dailyTransactionLimit: null,
        monthlyTransactionLimit: null,
        maxTransactionAmount: null,
        restrictedCategoryCodes: [],
        allowedCountries: "",
        requireAdditionalVerification: false,
        blockInternationalTransactions: false,
        reason: "",
        startDate: new Date().toISOString().split("T")[0],
        endDate: "",
      };
    },

    closeRestrictModal() {
      this.showRestrictModal = false;
      this.selectedCategoryCode = "";
    },

    // Action confirmation methods
    confirmPermanentBan() {
      console.log("Permanent ban confirmed for:", this.merchant.name);
      this.actionsTaken.push("Permanent Ban");
      this.closePermanentBanModal();
      // Add your ban logic here
      // alert(`${this.merchant.name} has been permanently banned!`);
      toast.error(`${this.merchant.name} has been permanently banned!`, {
        autoClose: 5000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
    },

    confirmShadowBan() {
      console.log("Shadow ban confirmed for:", this.merchant.name);
      this.actionsTaken.push("Shadow Ban");
      this.closeShadowBanModal();
      // Add your shadow ban logic here
      // alert(`${this.merchant.name} has been shadow banned!`);
      toast.warning(`${this.merchant.name} has been shadow banned!`, {
        autoClose: 5000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
    },

    confirmRestriction() {
      if (!this.restrictionData.reason.trim()) {
        alert("Please provide a reason for the restriction.");
        return;
      }

      console.log("Restriction data:", this.restrictionData);
      this.actionsTaken.push("Restricted");
      this.closeRestrictModal();

      // Store the restriction data (you can send this to your API)
      const restrictionSummary = {
        merchantId: this.merchant.id,
        merchantName: this.merchant.name,
        restrictions: { ...this.restrictionData },
        appliedAt: new Date().toISOString(),
      };

      console.log("Applied restrictions:", restrictionSummary);
      // alert(`Restrictions applied to ${this.merchant.name}!`);
      toast.success(
        `Restrictions applied on ${this.merchant.name} !`,
        {
          autoClose: 4000,
          position: "top-right",
          hideProgressBar: false,
          closeOnClick: true,
          pauseOnHover: true,
          draggable: true,
        }
      );
    },

    // Category code methods
    addCategoryCode() {
      if (
        this.selectedCategoryCode &&
        !this.restrictionData.restrictedCategoryCodes.includes(
          this.selectedCategoryCode
        )
      ) {
        this.restrictionData.restrictedCategoryCodes.push(
          this.selectedCategoryCode
        );
        this.selectedCategoryCode = "";
      }
    },

    removeCategoryCode(code) {
      const index = this.restrictionData.restrictedCategoryCodes.indexOf(code);
      if (index > -1) {
        this.restrictionData.restrictedCategoryCodes.splice(index, 1);
      }
    },
  },
};
</script>

<style scoped>
/* Existing styles remain the same... */
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

.actions-taken {
  text-align: center;
  color: #666;
  font-size: 14px;
}

.actions-taken-data {
  color: #999;
  font-size: 13px;
  font-style: italic;
}

.actions-taken h1 {
  text-align: center;
  color: #008080;
  font-size: 13px;
  font-style: italic;
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

/* Modal Styles */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.6);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
  backdrop-filter: blur(4px);
}

.modal-content {
  background: white;
  border-radius: 12px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
  max-width: 500px;
  width: 90%;
  max-height: 90vh;
  overflow-y: auto;
  animation: modalSlideIn 0.3s ease;
}

.modal-content.large {
  max-width: 800px;
}

@keyframes modalSlideIn {
  from {
    opacity: 0;
    transform: translateY(-50px) scale(0.9);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}

.modal-header {
  padding: 20px 24px;
  border-bottom: 1px solid #e5e7eb;
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: linear-gradient(135deg, #f8fafc, #f1f5f9);
}

.modal-header h3 {
  margin: 0;
  color: #008080;
  font-size: 18px;
  font-weight: 600;
}

.close-btn {
  background: none;
  border: none;
  font-size: 24px;
  color: #9ca3af;
  cursor: pointer;
  padding: 0;
  width: 30px;
  height: 30px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  transition: all 0.2s ease;
}

.close-btn:hover {
  background: #f3f4f6;
  color: #374151;
}

.modal-body {
  padding: 24px;
}

.warning-icon {
  font-size: 48px;
  text-align: center;
  margin-bottom: 16px;
}

.warning-text {
  font-size: 16px;
  text-align: center;
  margin-bottom: 12px;
  color: #374151;
}

.warning-subtext {
  font-size: 14px;
  text-align: center;
  color: #6b7280;
  margin-bottom: 0;
}

.modal-footer {
  padding: 16px 24px;
  border-top: 1px solid #e5e7eb;
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  background: #fafbfc;
}

/* Button Styles */
.btn-cancel {
  padding: 10px 20px;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  background: white;
  color: #374151;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-cancel:hover {
  background: #f9fafb;
  border-color: #9ca3af;
}

.btn-danger {
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  background: #dc2626;
  color: white;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-danger:hover {
  background: #b91c1c;
  transform: translateY(-1px);
}

.btn-warning {
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  background: #f59e0b;
  color: white;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-warning:hover {
  background: #d97706;
  transform: translateY(-1px);
}

.btn-primary {
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  background: #008080;
  color: white;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.btn-primary:hover {
  background: #006666;
  transform: translateY(-1px);
}

/* Form Styles */
.restriction-form {
  max-height: 60vh;
  overflow-y: auto;
}

.form-section {
  margin-bottom: 24px;
  padding-bottom: 20px;
  border-bottom: 1px solid #f3f4f6;
}

.form-section:last-child {
  border-bottom: none;
  margin-bottom: 0;
}

.form-section h4 {
  margin: 0 0 16px 0;
  color: #008080;
  font-size: 16px;
  font-weight: 600;
}

.form-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}

.form-group {
  margin-bottom: 16px;
}

.form-group label {
  display: block;
  margin-bottom: 6px;
  color: #374151;
  font-weight: 500;
  font-size: 14px;
}

.form-input,
.form-select,
.form-textarea {
  width: 100%;
  padding: 10px 12px;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  font-size: 14px;
  transition: all 0.2s ease;
  box-sizing: border-box;
}

.form-input:focus,
.form-select:focus,
.form-textarea:focus {
  outline: none;
  border-color: #008080;
  box-shadow: 0 0 0 3px rgba(0, 128, 128, 0.1);
}

.form-textarea {
  resize: vertical;
  min-height: 80px;
}

.checkbox-label {
  display: flex !important;
  align-items: center;
  gap: 8px;
  cursor: pointer;
}

.form-checkbox {
  width: auto !important;
  margin: 0 !important;
  accent-color: #008080;
}

/* Category Selector */
.category-selector {
  space-y: 12px;
}

.selected-categories {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 12px;
  min-height: 20px;
}

.category-tag {
  background: #e6fbf8;
  color: #008080;
  padding: 4px 8px;
  border-radius: 12px;
  font-size: 12px;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 4px;
}

.remove-tag {
  background: none;
  border: none;
  color: #008080;
  cursor: pointer;
  font-size: 14px;
  padding: 0;
  margin-left: 4px;
}

.remove-tag:hover {
  color: #006666;
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

  .modal-content {
    width: 95%;
    margin: 10px;
  }

  .form-row {
    grid-template-columns: 1fr;
  }

  .modal-footer {
    flex-direction: column;
  }

  .modal-footer button {
    width: 100%;
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

  .modal-body {
    padding: 16px;
  }

  .modal-header {
    padding: 16px;
  }
}
</style>
