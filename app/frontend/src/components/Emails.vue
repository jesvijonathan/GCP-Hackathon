<template>
  <div class="page-header">
    <nav class="breadcrumb">
      <router-link to="/dashboard" class="breadcrumb-item"
        >Home</router-link
      >
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item">Mailbox</span>
    </nav>
  </div>
  <div class="email-view-container">
    <!-- Email List Panel -->
    <div class="email-list-panel">
      <div class="email-list-header">
        <div class="header-content">
          <button @click="goBack" class="back-btn">
            <span class="back-icon">←</span>
            Back
          </button>
          <!-- <h2>Emails</h2> -->
        </div>
      </div>

      <div class="email-list">
        <div
          v-for="email in emails"
          :key="email.id"
          @click="selectEmail(email)"
          :class="[
            'email-item',
            {
              selected: selectedEmail && selectedEmail.id === email.id,
            },
          ]"
        >
          <div class="email-avatar">
            <div class="avatar-placeholder">
              {{ getInitials(email.sender.name) }}
            </div>
          </div>

          <div class="email-content">
            <div class="sender-name">{{ email.sender.name }}</div>
            <div class="email-subject">{{ email.subject }}</div>
            <div class="email-preview">
              {{ truncateText(email.preview, 80) }}
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Email Detail Panel -->
    <div class="email-detail-panel">
      <div v-if="!selectedEmail" class="no-selection">
        <h3>Select an email to view</h3>
        <p>Choose an email from the list to see its details.</p>
      </div>

      <div v-else class="email-detail">
        <div class="email-detail-header">
          <h2>{{ selectedEmail.subject }}</h2>
        </div>

        <div class="email-detail-content">
          <div class="sender-info">
            <div class="sender-avatar">
              <div class="avatar-placeholder large">
                {{ getInitials(selectedEmail.sender.name) }}
              </div>
            </div>
            <div class="sender-details">
              <div class="sender-name">{{ selectedEmail.sender.name }}</div>
              <div class="sender-email">{{ selectedEmail.sender.email }}</div>
              <div class="email-date">{{ formatDate(selectedEmail.date) }}</div>
            </div>
          </div>

          <div class="email-body">
            <div v-html="selectedEmail.body"></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import mailData from "./../assets/mail-data.json";

export default {
  name: "EmailView",
  data() {
    return {
      emails: mailData.emails || [],
      selectedEmail: null,
    };
  },
  methods: {
    selectEmail(email) {
      this.selectedEmail = email;
    },

    goBack() {
      // Option 1: Use Vue Router to go back
      if (this.$router) {
        this.$router.go(-1);
      }
      // Option 2: Emit event to parent component
      else {
        this.$emit("go-back");
      }

      // Option 3: If you want to go to a specific route
      // this.$router.push('/dashboard');
    },

    getInitials(name) {
      return name
        .split(" ")
        .map((word) => word[0])
        .join("")
        .toUpperCase()
        .slice(0, 2);
    },

    truncateText(text, maxLength) {
      if (!text) return "";
      return text.length > maxLength
        ? text.substring(0, maxLength) + "..."
        : text;
    },

    formatDate(dateString) {
      const date = new Date(dateString);
      return date.toLocaleDateString("en-US", {
        weekday: "long",
        year: "numeric",
        month: "long",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      });
    },
  },
};
</script>

<style scoped>
.email-view-container {
  display: grid;
  grid-template-columns: 350px 1fr;
  height: 100vh;
  font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
  background-color: #f5f5f5;
  overflow: hidden;
}

/* Email List Panel */
.email-list-panel {
  background: white;
  border-right: 1px solid #e5e7eb;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.email-list-header {
  padding: 15px 20px;
  border-bottom: 1px solid #e5e7eb;
  background: #008080;
  color: white;
  flex-shrink: 0;
}

.email-list-header .header-content {
  display: flex;
  /* flex-direction: column; */
  gap: 12px;
  align-items: center;
  justify-content: space-between;
}

.back-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  background: rgba(255, 255, 255, 0.2);
  color: white;
  border: 1px solid rgba(255, 255, 255, 0.3);
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
  align-self: flex-start;
}

.back-btn:hover {
  background: rgba(255, 255, 255, 0.3);
  border-color: rgba(255, 255, 255, 0.5);
  transform: translateX(-2px);
}

.back-icon {
  font-size: 16px;
  font-weight: bold;
}

.email-list-header h2 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
}

.email-list {
  flex: 1;
  overflow-y: auto;
  padding: 10px;
}

.email-list::-webkit-scrollbar {
  width: 6px;
}

.email-list::-webkit-scrollbar-track {
  background: #f1f1f1;
}

.email-list::-webkit-scrollbar-thumb {
  background: #14b8a6;
  border-radius: 3px;
}

.email-item {
  display: flex;
  align-items: center;
  padding: 15px;
  margin-bottom: 8px;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
  background: white;
  border: 1px solid #e5e7eb;
}

.email-item:hover {
  background: #f0fdfa;
  border-color: #14b8a6;
}

.email-item.selected {
  background: #008080;
  color: white;
  border-color: #008080;
}

.email-avatar {
  margin-right: 12px;
  flex-shrink: 0;
}

.avatar-placeholder {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  background: linear-gradient(135deg, #008080, #14b8a6);
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 600;
  font-size: 14px;
}

.email-item.selected .avatar-placeholder {
  background: white;
  color: #008080;
}

.email-content {
  flex: 1;
  min-width: 0;
}

.sender-name {
  font-weight: 600;
  color: #374151;
  font-size: 14px;
  margin-bottom: 4px;
}

.email-item.selected .sender-name {
  color: white;
}

.email-subject {
  font-weight: 500;
  color: #4b5563;
  font-size: 13px;
  margin-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.email-item.selected .email-subject {
  color: #f0fdfa;
}

.email-preview {
  color: #6b7280;
  font-size: 12px;
  line-height: 1.4;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.email-item.selected .email-preview {
  color: #e6fbf8;
}

/* Email Detail Panel */
.email-detail-panel {
  background: white;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.no-selection {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  text-align: center;
  color: #6b7280;
}

.no-selection h3 {
  color: #008080;
  margin-bottom: 8px;
  font-size: 20px;
}

.email-detail {
  height: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.email-detail-header {
  padding: 20px;
  border-bottom: 1px solid #e5e7eb;
  background: linear-gradient(135deg, #f8fafc, #f1f5f9);
  flex-shrink: 0;
}

.email-detail-header h2 {
  margin: 0;
  color: #008080;
  font-size: 20px;
  font-weight: 600;
  line-height: 1.3;
}

.email-detail-content {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
  height: 0;
}

.email-detail-content::-webkit-scrollbar {
  width: 8px;
}

.email-detail-content::-webkit-scrollbar-track {
  background: #f1f1f1;
  border-radius: 4px;
}

.email-detail-content::-webkit-scrollbar-thumb {
  background: #14b8a6;
  border-radius: 4px;
}

.email-detail-content::-webkit-scrollbar-thumb:hover {
  background: #008080;
}

.sender-info {
  display: flex;
  align-items: center;
  gap: 15px;
  margin-bottom: 20px;
  padding-bottom: 20px;
  border-bottom: 1px solid #e5e7eb;
  flex-shrink: 0;
}

.sender-avatar .avatar-placeholder.large {
  width: 50px;
  height: 50px;
  font-size: 18px;
}

.sender-details .sender-name {
  font-size: 16px;
  font-weight: 600;
  color: #374151;
  margin-bottom: 4px;
}

.sender-details .sender-email {
  color: #6b7280;
  font-size: 14px;
  margin-bottom: 4px;
}

.sender-details .email-date {
  color: #9ca3af;
  font-size: 13px;
}

.email-body {
  line-height: 1.6;
  color: #374151;
  word-wrap: break-word;
  overflow-wrap: break-word;
}

.email-body :deep(p) {
  margin-bottom: 16px;
}

.email-body :deep(h3) {
  color: #008080;
  margin-top: 20px;
  margin-bottom: 10px;
}

.email-body :deep(ul),
.email-body :deep(ol) {
  margin-bottom: 16px;
  padding-left: 20px;
}

.email-body :deep(li) {
  margin-bottom: 4px;
}

.email-body :deep(strong) {
  color: #374151;
}

.email-body :deep(a) {
  color: #008080;
  text-decoration: none;
}

.email-body :deep(a:hover) {
  text-decoration: underline;
}

.email-body :deep(div[style*="background"]) {
  padding: 15px;
  border-radius: 8px;
  margin-bottom: 15px;
}

.email-body :deep(table) {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 15px;
  max-width: 100%;
  table-layout: auto;
}

.email-body :deep(th),
.email-body :deep(td) {
  padding: 8px;
  border: 1px solid #e5e7eb;
  text-align: left;
  word-wrap: break-word;
}

.email-body :deep(th) {
  background: #f8f9fa;
  font-weight: 600;
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

/* Responsive Design */
@media (max-width: 768px) {
  .email-view-container {
    grid-template-columns: 1fr;
    grid-template-rows: 300px 1fr;
  }

  .email-list-panel {
    border-right: none;
    border-bottom: 1px solid #e5e7eb;
  }

  .email-list-header .header-content {
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    gap: 15px;
  }

  .back-btn {
    align-self: auto;
  }
}

@media (max-width: 480px) {
  .email-view-container {
    grid-template-rows: 250px 1fr;
  }

  .email-list-header {
    padding: 12px 15px;
  }

  .email-detail-header {
    padding: 15px;
  }

  .email-detail-content {
    padding: 15px;
  }

  .email-item {
    padding: 12px;
  }

  .email-list-header h2 {
    font-size: 16px;
  }

  .back-btn {
    padding: 6px 12px;
    font-size: 13px;
  }
}
</style>
