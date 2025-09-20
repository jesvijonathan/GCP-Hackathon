<template>
  <div class="card merchant-info-card">
    <div class="card-header">
      <h3>Merchant Info</h3>
    </div>
    <div class="card-content">
      <p><strong>Name:</strong> {{ merchant.name }}</p>
      <p><strong>ID:</strong> {{ merchant.id }}</p>
      <p><strong>Business Type:</strong> {{ merchant.businessType }}</p>
    </div>
  </div>
</template>

<script>
export default {
  name: "DashboardMerchantInfo",
  props: {
    merchant: {
      type: Object,
      required: true
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

.card-content {
  font-size: 14px;
  color: #374151;
}

/* Specific styles for this card */
.merchant-info-card {
  /* No specific overrides needed here, general card styles apply */
}
</style>