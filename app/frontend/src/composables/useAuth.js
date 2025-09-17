// src/composables/useAuth.js
import { ref } from 'vue'

// Global reactive state - shared across all components
const isAuthenticated = ref(false)

// Initialize auth status immediately when module loads
const initializeAuth = () => {
  const authStatus = localStorage.getItem('isAuthenticated')
  isAuthenticated.value = authStatus === 'true'
  console.log('useAuth: Initialized auth status:', isAuthenticated.value)
}

// Call initialization
initializeAuth()

export const useAuth = () => {
  const login = (credentials) => {
    console.log('useAuth: Login called with:', credentials)
    
    // Here you can add validation logic if needed
    // For now, we'll just set the user as authenticated
    isAuthenticated.value = true
    localStorage.setItem('isAuthenticated', 'true')
    
    console.log('useAuth: Login successful, auth state:', isAuthenticated.value)
    return { success: true, message: 'Login successful' }
  }

  const logout = () => {
    console.log('useAuth: Logout called')
    isAuthenticated.value = false
    localStorage.setItem('isAuthenticated', 'false')
    
    // Clean up other user data if needed
    localStorage.removeItem('username')
    localStorage.removeItem('password')
    
    console.log('useAuth: Logout complete, auth state:', isAuthenticated.value)
  }

  const checkAuthStatus = () => {
    const authStatus = localStorage.getItem('isAuthenticated')
    isAuthenticated.value = authStatus === 'true'
    console.log('useAuth: Checked auth status:', isAuthenticated.value)
    return isAuthenticated.value
  }

  return {
    isAuthenticated,
    login,
    logout,
    checkAuthStatus
  }
}