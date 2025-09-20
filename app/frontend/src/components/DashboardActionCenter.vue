<template>
  <div class="card action-center-card">
    <h1 class="action-card-heading">Action Center for {{ merchant.name }}</h1>
    <button @click="handlePermanentBan" class="action-card-button">
      Permanent Ban
    </button>
    <button @click="handleShadowBan" class="action-card-button">
      Shadow Ban
    </button>
    <button @click="handleContinueMerchant" class="action-card-button">
      Continue Merchant
    </button>
  </div>
</template>

<script>
import { toast } from 'vue3-toastify';

export default {
  name: "DashboardActionCenter",
  props: {
    merchant: {
      type: Object,
      required: true
    }
  },
  emits: ['merchant-action'],
  methods: {
    handlePermanentBan() {
      toast.error(`${this.merchant.name} has been permanently banned!`, {
        autoClose: 5000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
      
      this.$emit('merchant-action', {
        action: 'permanent-ban',
        merchant: this.merchant
      });
    },
    
    handleShadowBan() {
      toast.warning(`${this.merchant.name} has been shadow banned!`, {
        autoClose: 5000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
      
      this.$emit('merchant-action', {
        action: 'shadow-ban',
        merchant: this.merchant
      });
    },
    
    handleContinueMerchant() {
      toast.success(`${this.merchant.name} will continue operating normally!`, {
        autoClose: 4000,
        position: "top-right",
        hideProgressBar: false,
        closeOnClick: true,
        pauseOnHover: true,
        draggable: true,
      });
      
      this.$emit('merchant-action', {
        action: 'continue',
        merchant: this.merchant
      });
    }
  }
};
</script>

<style scoped>
/* Shared Card Styles moved from Dashboard.vue */
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

/* Specific Styles for Action Center Card */
.action-center-card {
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
  padding: 25px; /* More padding */
}

.action-card-button {
  background: #008080; /* Dark teal */
  color: white;
  border: none;
  border-radius: 8px;
  padding: 12px 20px;
  margin: 6px;
  width: 200px; /* Consistent button width */
  cursor: pointer;
  font-weight: 500;
  font-size: 14px;
  transition: all 0.2s ease;
}

.action-card-button:hover {
  background: #006666; /* Darker teal on hover */
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
</style>