<template>
  <div class="dashboard-view">
  <div class="page-header">
    <nav class="breadcrumb">
      <span class="breadcrumb-item current">Dashboard</span
      >
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item">-</span>
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

.page-header {
  padding: 16px 20px;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
}

.breadcrumb {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 14px;
  color: #6b7280;
}

.breadcrumb-item {
  color: #6b7280;
  text-decoration: none;
  transition: color 0.2s ease;
}

.breadcrumb-item:hover {
  color: #008080;
}

.breadcrumb-item.current {
  color: #008080;
  font-weight: 500;
}

.breadcrumb-separator {
  color: #d1d5db;
  user-select: none;
}
</style>