<template>
  <div class="card action-center-card" v-if="merchant">
    <h1 class="action-card-heading">
      Action Center for {{ merchant.name }}
    </h1>
    <button @click="$emit('action', 'permanent-ban')" class="action-card-button">
      Permanent Ban
    </button>
    <button @click="$emit('action', 'shadow-ban')" class="action-card-button">
      Shadow Ban
    </button>
    <button @click="$emit('action', 'continue')" class="action-card-button">
      Continue Merchant
    </button>
  </div>
</template>

<script>
export default {
  name: "DashboardActionCenter",
  props: {
    merchant: {
      type: Object,
      required: true,
    },
  },
  emits: ['action'],
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

.action-center-card {
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
  padding: 25px;
}

.action-card-button {
  background: #008080;
  color: white;
  border: none;
  border-radius: 8px;
  padding: 12px 20px;
  margin: 6px;
  width: 200px;
  cursor: pointer;
  font-weight: 500;
  font-size: 14px;
  transition: all 0.2s ease;
}

.action-card-button:hover {
  background: #006666;
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

/* Responsive styles */
@media (max-width: 480px) {
  .action-card-button {
    width: 160px;
  }
}
</style>