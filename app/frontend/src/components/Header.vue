<template>
  <header class="header">
    <div class="container">
      <img class="logo" :src="logo" alt="NullByte" />
      <nav class="nav">
        <button class="action-center">Action Center</button>
        <button class="auth-button" @click="toggleAuth">
          {{ isLoggedIn ? 'Logout' : 'Login' }}
        </button>
      </nav>
    </div>
  </header>
</template>

<script>
import NullByteLogo from '@/assets/img/Nullbyte.svg'

export default {
  name: 'Header',
  data() {
    return {
      isLoggedIn: false,
      logo: NullByteLogo,
    }
  },
  mounted() {
    // Check auth status when component mounts
    this.checkAuthStatus()
    
    // Listen for storage changes (in case user logs out in another tab)
    window.addEventListener('storage', this.handleStorageChange)
  },
  beforeUnmount() {
    window.removeEventListener('storage', this.handleStorageChange)
  },
  methods: {
    checkAuthStatus() {
      const authStatus = localStorage.getItem('isAuthenticated')
      this.isLoggedIn = authStatus === 'true'
    },
    
    handleStorageChange(event) {
      if (event.key === 'isAuthenticated') {
        this.checkAuthStatus()
      }
    },
    
    toggleAuth() {
      if (this.isLoggedIn) {
        // User is logged in, so logout
        this.logout()
      } else {
        // User is not logged in, redirect to login
        this.$router.push('/login')
      }
    },
    
    logout() {
      localStorage.setItem('isAuthenticated', 'false')
      // Or completely remove: localStorage.removeItem('isAuthenticated')
      this.isLoggedIn = false
      this.$router.push('/login')
    },
    
    login() {
      localStorage.setItem('isAuthenticated', 'true')
      this.isLoggedIn = true
    }
  }
}
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