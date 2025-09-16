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
  import NullByteLogo from '@/assets/img/Nullbyte.svg';
  
  export default {
    data() {
      return {
        isLoggedIn: false,
        logo: NullByteLogo,
      };
    },
    methods: {
      toggleAuth() {
        this.isLoggedIn = !this.isLoggedIn;
      },
    },
  };
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
  }
  
  .action-center:hover,
  .auth-button:hover {
    background-color: #e0f7fa;
  }
  </style>