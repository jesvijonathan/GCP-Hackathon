<template>
  <footer class="footer">
    <div class="container footer-content">
      <div class="footer-left">
        <router-link to="/about" class="footer-logo-link">
          <img class="footer-logo" :src="logo" alt="NullByte" />
        </router-link>
      </div>

      <nav class="footer-links" aria-label="Footer links">
        <router-link to="/about" class="footer-link">About</router-link>
        <a href="#" class="footer-link">Docs</a>
        <a href="#" class="footer-link">Support</a>
      </nav>

      <div class="footer-right" aria-label="Social">
        <a href="#" class="footer-icon" aria-label="GitHub">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
            <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
          </svg>
        </a>
        <a href="#" class="footer-icon" aria-label="Twitter">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
            <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>
          </svg>
        </a>
      </div>
    </div>
    
    <div class="footer-divider"></div>
    
    <div class="footer-bottom">
      <div class="container">
        <span class="copyright">© 2025 NullByte. All rights reserved.</span>
        <div class="footer-bottom-links">
          <a href="#" class="footer-bottom-link">Privacy Policy</a>
          <a href="#" class="footer-bottom-link">Terms of Service</a>
        </div>
      </div>
    </div>
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
.footer {
  background: linear-gradient(135deg, #008080 0%, #006666 100%);
  color: white;
  margin-top: auto;
  box-shadow: 0 -4px 20px rgba(0, 0, 0, 0.1);
}

.container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 1rem;
}

.footer-content {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 2rem;
  padding: 2rem 0;
}

.footer-left .footer-logo-link {
  display: inline-block;
  transition: transform 0.3s ease, opacity 0.3s ease;
}

.footer-left .footer-logo-link:hover {
  transform: scale(1.05);
  opacity: 0.9;
}

.footer-logo {
  height: 32px;
  width: auto;
  display: block;
  filter: brightness(0) invert(1);
}

.footer-links {
  display: flex;
  gap: 2rem;
  justify-content: center;
  flex: 1;
}

.footer-link {
  color: white;
  text-decoration: none;
  font-size: 1rem;
  font-weight: 500;
  padding: 0.5rem 0;
  position: relative;
  transition: color 0.3s ease;
}

.footer-link::after {
  content: '';
  position: absolute;
  bottom: 0;
  left: 0;
  width: 0;
  height: 2px;
  background: white;
  transition: width 0.3s ease;
}

.footer-link:hover {
  color: #e0f7fa;
}

.footer-link:hover::after {
  width: 100%;
}

.footer-right {
  display: flex;
  gap: 1rem;
  justify-content: flex-end;
}

.footer-icon {
  color: white;
  text-decoration: none;
  padding: 0.75rem;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.1);
  transition: all 0.3s ease;
  display: flex;
  align-items: center;
  justify-content: center;
  backdrop-filter: blur(10px);
}

.footer-icon:hover {
  background: rgba(255, 255, 255, 0.2);
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
}

.footer-divider {
  height: 1px;
  background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.3), transparent);
  margin: 0 2rem;
}

.footer-bottom {
  padding: 1.5rem 0;
  background: rgba(0, 0, 0, 0.1);
}

.footer-bottom .container {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.copyright {
  font-size: 0.9rem;
  opacity: 0.8;
  font-weight: 300;
}

.footer-bottom-links {
  display: flex;
  gap: 1.5rem;
}

.footer-bottom-link {
  color: white;
  text-decoration: none;
  font-size: 0.85rem;
  opacity: 0.7;
  transition: opacity 0.3s ease;
}

.footer-bottom-link:hover {
  opacity: 1;
}

/* Responsive Design */
@media (max-width: 768px) {
  .footer-content {
    flex-direction: column;
    align-items: center;
    text-align: center;
    gap: 1.5rem;
    padding: 1.5rem 0;
  }
  
  .footer-left,
  .footer-right {
    justify-content: center;
    width: 100%;
  }
  
  .footer-links {
    justify-content: center;
    gap: 1.5rem;
    width: 100%;
  }

  .footer-bottom .container {
    flex-direction: column;
    gap: 1rem;
    text-align: center;
  }

  .footer-bottom-links {
    justify-content: center;
  }
}

@media (max-width: 480px) {
  .footer-links {
    flex-direction: column;
    gap: 1rem;
  }
  
  .footer-right {
    gap: 0.75rem;
  }
  
  .footer-bottom-links {
    flex-direction: column;
    gap: 0.75rem;
  }
}

/* Router link active state */
.router-link-active.footer-link {
  color: #e0f7fa;
}

.router-link-active.footer-link::after {
  width: 100%;
}
</style>