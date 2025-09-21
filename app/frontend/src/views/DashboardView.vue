<template>
  <div class="dashboard-view">
    <div class="page-header">
      <h1 class="page-title">Merchant Management Dashboard</h1>
      <nav class="breadcrumb">
        <router-link to="/">Home</router-link> > 
        <span>Merchants</span>
      </nav>
    </div>
    
    <Dashboard />
  </div>
</template>

<script>
import Dashboard from '@/components/Dashboard.vue'; // Correct import path

export default {
  name: 'DashboardView',
  components: {
    Dashboard
  }
};
</script>

<style scoped>
.dashboard-view {
  min-height: 100vh;
}

.page-header {
  padding: 20px;
  background: #f8f9fa;
  border-bottom: 1px solid #e9ecef;
}

.page-title {
  margin: 0 0 10px 0;
  color: #2c3e50;
  font-size: 24px;
}

.breadcrumb {
  color: #6c757d;
  font-size: 14px;
}

.breadcrumb a {
  color: #007bff;
  text-decoration: none;
}

.breadcrumb a:hover {
  text-decoration: underline;
}
</style>