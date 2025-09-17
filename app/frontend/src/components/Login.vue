<script setup>
import { RouterLink, useRouter } from "vue-router";
import { ref, onMounted } from 'vue';
import { useAuth } from '@/composables/useAuth';

const router = useRouter();
const { login } = useAuth(); // Get the login function from useAuth

const username = ref("");
const password = ref("");

const handleSubmit = (event) => {
  event.preventDefault();
  
  // Use the login function from useAuth instead of directly setting localStorage
  const credentials = {
    username: username.value,
    password: password.value
  };
  
  const result = login(credentials);
  
  if (result.success) {
    // Also store username and password if needed
    localStorage.setItem("username", username.value);
    localStorage.setItem("password", password.value);
    
    router.push('/dashboard');
    
    username.value = "";
    password.value = "";
  }
};
</script>

<template>
  <div class="login-container">
    <form class="login-form">
      <h2 class="heading">Welcome!</h2>
      <!-- <p class="subtitle">Sign in to continue</p> -->

      <div class="form-group">
        <div class="input-wrapper">
          <span class="input-icon">👤</span>
          <input
            type="text"
            id="username"
            placeholder="Enter your username"
            v-model="username"
          />
        </div>
      </div>

      <div class="form-group">
        <div class="input-wrapper">
          <span class="input-icon">🔒</span>
          <input
            type="password"
            id="password"
            placeholder="Enter your password"
            v-model="password"
          />
        </div>
      </div>

      <button @click="handleSubmit" type="submit" class="login-button">
        <span>Login</span>
        <span class="button-arrow">→</span>
      </button>
    </form>
  </div>
</template>

<style scoped>
/* Your existing styles remain the same */
* {
  box-sizing: border-box;
}

.login-container {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 80vh;
  margin: 0;
  font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background-color: #f8f9fa;
  padding: 20px;
}

.login-form {
  width: 100%;
  max-width: 400px;
  padding: 40px 30px;
  background: white;
  border-radius: 8px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
  border: 1px solid #e9ecef;
}

.heading {
  text-align: center;
  margin-bottom: 20px;
  font-size: 24px;
  color: #188882;
  font-weight: 600;
}

.subtitle {
  text-align: center;
  margin-bottom: 30px;
  color: #20b2aa;
  font-size: 15px;
}

.form-group {
  margin-bottom: 20px;
}

.input-wrapper {
  position: relative;
}

.input-icon {
  position: absolute;
  left: 12px;
  top: 50%;
  transform: translateY(-50%);
  font-size: 16px;
  color: #6c757d;
  z-index: 2;
}

.form-group input {
  width: 100%;
  padding: 12px 12px 12px 40px;
  background: #f8f9fa;
  border: 1px solid #dee2e6;
  border-radius: 6px;
  font-size: 14px;
  color: #2c3e50;
  transition: all 0.2s ease;
}

.form-group input::placeholder {
  color: #adb5bd;
}

.form-group input:focus {
  outline: none;
  background: white;
  border-color: #20b2aa;
  box-shadow: 0 0 0 3px rgba(32, 178, 170, 0.1);
}

.login-button {
  width: 100%;
  padding: 12px 20px;
  background-color: #20b2aa;
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  margin-top: 10px;
}

.login-button:hover {
  background-color: #1a9b96;
  transform: translateY(-1px);
  box-shadow: 0 2px 8px rgba(32, 178, 170, 0.3);
}

.login-button:active {
  transform: translateY(0);
}

.button-arrow {
  transition: transform 0.2s ease;
}

.login-button:hover .button-arrow {
  transform: translateX(2px);
}

/* Responsive Design */
@media (max-width: 480px) {
  .login-container {
    padding: 15px;
  }

  .login-form {
    padding: 30px 20px;
    max-width: 100%;
  }

  .heading {
    font-size: 22px;
  }
}
</style>