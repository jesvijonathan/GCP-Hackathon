Here’s a matching footer added to your Vue component. It uses the same teal styling and includes logo, navigation links, and a simple social area for a cohesive look with the header.

```vue
<template>
  <footer class="footer">
    <div class="container footer-content">
      <div class="footer-left">
        <img class="footer-logo" :src="logo" alt="NullByte" />
      </div>

      <nav class="footer-links" aria-label="Footer links">
        <a href="#">About</a>
        <a href="#">Docs</a>
        <a href="#">Support</a>
      </nav>

      <div class="footer-right" aria-label="Social">
        <a href="#" class="footer-icon" aria-label="GitHub">GitHub</a>
        <a href="#" class="footer-icon" aria-label="Twitter">X</a>
      </div>
    </div>
    <div class="footer-bottom">© 2025 NullByte. All rights reserved.</div>
  </footer>
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

/* Matching Footer Styles */
.footer {
  background-color: teal;
  color: white;
  padding: 1rem 0;
  margin-top: 1rem;
}

.footer-content {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 2rem;
  padding: 0 1rem;
}

.footer-left .footer-logo {
  height: 28px;
  width: auto;
  display: block;
}

.footer-links {
  display: flex;
  gap: 1.25rem;
  justify-content: center;
  flex: 1;
}

.footer-links a {
  color: white;
  text-decoration: none;
  font-size: 0.95rem;
}

.footer-right {
  display: flex;
  gap: 0.75rem;
  justify-content: flex-end;
}

.footer-icon {
  color: white;
  text-decoration: none;
  font-size: 0.95rem;
}

.footer-bottom {
  text-align: center;
  font-size: 0.8rem;
  padding-top: 0.5rem;
  opacity: 0.9;
}

/* Responsive tweaks */
@media (max-width: 768px) {
  .footer-content {
    flex-direction: column;
    align-items: center;
    text-align: center;
    gap: 0.75rem;
  }
  .footer-left,
  .footer-right {
    justify-content: center;
    width: 100%;
  }
  .footer-links {
    justify-content: center;
  }
}
</style>
```