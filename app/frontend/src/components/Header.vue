<template>
  <header class="header">
    <div class="container">
      <div class="header-left">
        <img 
          @click="redirectToAboutUs" 
          class="logo" 
          :src="logo" 
          alt="NullByte" 
        />
      </div>
      
      <nav v-if="isAuthenticated" class="nav" aria-label="Main navigation" >
        <button @click="redirectToDashboard"  class="action-center" aria-label="Action Center">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" class="button-icon">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
          </svg>
          Action Center
        </button>
        
        <button 
          @click="redirectToMerchantOnboarding" 
          class="action-center merchant-btn"
          aria-label="Onboard Merchant"
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" class="button-icon">
            <path d="M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z"/>
          </svg>
          Onboard Merchant
        </button>
        
        <button 
          class="auth-button" 
          @click="toggleAuth"
          :aria-label="isAuthenticated ? 'Logout' : 'Login'"
        >
          <svg v-if="!isAuthenticated" width="16" height="16" viewBox="0 0 24 24" fill="currentColor" class="button-icon">
            <path d="M11 7L9.6 8.4l2.6 2.6H2v2h10.2l-2.6 2.6L11 17l5-5-5-5zm9 12h-8v2h8c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2h-8v2h8v12z"/>
          </svg>
          <svg v-else width="16" height="16" viewBox="0 0 24 24" fill="currentColor" class="button-icon">
            <path d="M17 7l-1.41 1.41L18.17 11H8v2h10.17l-2.58 2.59L17 17l5-5-5-5zM4 5h8V3H4c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h8v-2H4V5z"/>
          </svg>
          {{ isAuthenticated ? 'Logout' : 'Login' }}
        </button>
      </nav>
      <nav v-else class="nav" aria-label="Main navigation">
            <img
            @click="redirectToAboutUs"
            class="wl-logo"
            src="https://nullbyte-ticketing-system.vercel.app/assets/logo-fFaqgoXu.png"
            alt="NullByte"
            />
            |
            <img
            @click="redirectToAboutUs"
            class="wl-logo_g"
            src="https://nullbyte-ticketing-system.vercel.app/assets/Google_2015_logo.svg-CnFd-Bbr.webp"
            alt="NullByte"
            />
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

const redirectToMerchantOnboarding = () => {
  if (isAuthenticated.value) {
    router.push('/merchant-onboarding')
  } else {
    router.push('/login')
  }
}

const redirectToDashboard = () => {
  if (isAuthenticated.value) {
    router.push('/dashboard')
  } else {
    router.push('/login')
  }
}
const redirectToAboutUs = () => {
  if (isAuthenticated.value) {
    router.push('/about')
  } else {
    router.push('/login')
  }
}

// Debug: Watch authentication changes
watch(isAuthenticated, (newValue) => {
  console.log('Header: Auth state changed to:', newValue)
})
</script>

<style scoped>
.header {
  background: linear-gradient(135deg, #008080 0%, #006666 100%);
  color: white;
  padding: 0;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
  backdrop-filter: blur(10px);
  position: sticky;
  top: 0;
  z-index: 1000;
}

.wl-logo{
    height: 22px;
    width: auto;
    display: block;
    cursor: pointer;
    transition: all 0.3s ease;
    filter: brightness(0) invert(1);
    margin-right: 2rem;
    margin: 1rem 1rem;
}
.wl-logo:hover{
  transform: scale(1.02);
  filter: brightness(0) invert(1) drop-shadow(0 0 8px rgba(255, 255, 255, 0.3));
}

.wl-logo_g{
    height: 30px;
    width: auto;
    display: block;
    cursor: pointer;
    transition: all 0.3s ease;
    filter: brightness(0) invert(1);
    margin-right: 2rem;
    margin-left: 1rem;
    margin-top: 4px;
}
.wl-logo_g:hover{
  transform: scale(1.02);
  filter: brightness(0) invert(1) drop-shadow(0 0 8px rgba(255, 255, 255, 0.3));
}

.container {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
  max-width: none;
  margin: 0;
  padding: 1rem 0.5rem;
}

.header-left {
  display: flex;
  align-items: center;
  margin-left: 3rem;
}

.logo {
  height: 32px;
  width: auto;
  display: block;
  cursor: pointer;
  transition: all 0.3s ease;
  filter: brightness(0) invert(1);
}

.logo:hover {
  transform: scale(1.15, 1.05) rotate(-1deg);
  filter: brightness(0) invert(1) drop-shadow(0 0 8px rgba(255, 255, 255, 0.3));
}

.nav {
  display: flex;
  gap: 1rem;
  align-items: center;
  margin-right: 0.5rem;
}

.action-center,
.auth-button {
  background: rgba(255, 255, 255, 0.95);
  color: #008080;
  border: none;
  padding: 0.75rem 1.25rem;
  border-radius: 7px;
  cursor: pointer;
  font-size: 0.95rem;
  font-weight: 600;
  transition: all 0.3s ease;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  backdrop-filter: blur(10px);
  /* box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1); */
  position: relative;
  overflow: hidden;
      background: #008080;
  color: white;
}

.action-center::before,
.auth-button::before {
  content: '';
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
    background: white;
  transition: left 0.5s ease;
}

.action-center:hover::before,
.auth-button:hover::before {
  left: 100%;
}

.action-center:hover,
.auth-button:hover {
  color: #008080;
     background: white;
          text-decoration: underline;
}

.action-center:active,
.auth-button:active {
  transform: translateY(0);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
}

.merchant-btn {
    background: #008080;
  color: white;
}

.merchant-btn:hover {
  color: #008080;
     background: white;
          text-decoration: underline;
}

.auth-button {
    background: #008080;
  color: white;

}

.auth-button:hover {
  color: #008080;
     background: white;
     text-decoration: underline;
}

.button-icon {
  transition: transform 0.1s ease;
}



/* Responsive Design */
@media (max-width: 768px) {
  .container {
    padding: 0.75rem 0.25rem;
    flex-wrap: wrap;
    gap: 1rem;
  }
  
  .header-left {
    flex: 1;
    margin-left: 0.25rem;
  }
  
  .nav {
    gap: 0.5rem;
    flex-wrap: wrap;
    margin-right: 0.25rem;
  }
  
  .action-center,
  .auth-button {
    padding: 0.5rem 0.75rem;
    font-size: 0.85rem;
  }
  
  .button-icon {
    width: 14px;
    height: 14px;
  }
}

@media (max-width: 480px) {
  .container {
    padding: 0.5rem 0.125rem;
  }
  
  .header-left {
    margin-left: 0.125rem;
  }
  
  .nav {
    width: 100%;
    justify-content: space-between;
    margin-right: 0.125rem;
  }
  
  .action-center,
  .auth-button {
    flex: 1;
    justify-content: center;
    min-width: 0;
    padding: 0.5rem;
  }
  
  .action-center span,
  .auth-button span {
    display: none;
  }
  
  .container {
    flex-direction: column;
    align-items: stretch;
  }
  
  .header-left {
    justify-content: center;
    margin-bottom: 0.5rem;
    margin-left: 0;
  }
  
  .nav {
    margin-right: 0;
  }
}

/* Loading state animation */
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.7; }
}

.loading {
  animation: pulse 1.5s ease-in-out infinite;
}

/* Focus states for accessibility */
.action-center:focus,
.auth-button:focus,
.logo:focus {
  outline: 2px solid rgba(255, 255, 255, 0.8);
  outline-offset: 2px;
}
</style>