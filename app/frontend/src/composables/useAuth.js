// src/composables/useAuth.js
import { ref } from 'vue'

const isAuthenticated = ref(false)

export const useAuth = () => {
  const login = (credentials) => {
    isAuthenticated.value = true
    localStorage.setItem('isAuthenticated', 'true')
    return { success: true }
  }

  const logout = () => {
    isAuthenticated.value = false
    localStorage.setItem('isAuthenticated', 'false')
  }

  const checkAuthStatus = () => {
    const authStatus = localStorage.getItem('isAuthenticated')
    isAuthenticated.value = authStatus === 'true'
  }

  checkAuthStatus()

  return {
    isAuthenticated,
    login,
    logout,
    checkAuthStatus
  }
}