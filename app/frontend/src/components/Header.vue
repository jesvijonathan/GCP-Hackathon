<template>
  <header class="header">
    <div class="container">
      <img class="logo" :src="logo" alt="NullByte" />
      <nav class="nav">
        <button class="action-center">Action Center</button>
        <button class="auth-button" @click="toggleAuth">
          {{ isAuthenticated ? 'Logout' : 'Login' }}
        </button>
      </nav>
    </div>
  </header>
</template>

<script setup>
import { onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import NullByteLogo from '@/assets/img/Nullbyte.svg'
import { useAuth } from '@/composables/useAuth'

const router = useRouter()
const { isAuthenticated, logout, checkAuthStatus } = useAuth()

const logo = NullByteLogo

const toggleAuth = () => {
  if (isAuthenticated.value) {
    // User is logged in, so logout
    logout()
    router.push('/login')
  } else {
    // User is not logged in, redirect to login
    router.push('/login')
  }
}

// Check auth status when component mounts
onMounted(() => {
  checkAuthStatus()
})

// Debug: Watch authentication changes
watch(isAuthenticated, (newValue) => {
  console.log('Header: Auth state changed to:', newValue)
})
</script>

<style scoped>
.header {
  background-color: teal;
  color: white;
  padding: 1rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.container {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.logo {
  height: 40px;
  width: auto;
  display: block;
}

.nav {
  display: flex;
  gap: 1rem;
}

.action-center,
.auth-button {
  background-color: white;
  color: teal;
  border: none;
  padding: 0.5rem 1rem;
  border-radius: 5px;
  cursor: pointer;
  font-size: 1rem;
  transition: background-color 0.3s ease;
}

.action-center:hover,
.auth-button:hover {
  background-color: #e0f7fa;
}
</style>