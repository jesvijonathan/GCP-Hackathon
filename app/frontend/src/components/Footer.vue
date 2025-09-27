<template>
  <footer class="footer">
    <div class="container footer-content">
      <div class="footer-left">
        <router-link to="/about" class="footer-logo-link">
          <img class="footer-logo" :src="logo" alt="NullByte" />
        </router-link>
      </div>

      <!-- <nav class="footer-links" aria-label="Footer links">
        <router-link to="/about" class="footer-link">About</router-link>
        <a href="#" class="footer-link">Docs</a>
        <a href="#" class="footer-link">Support</a>
      </nav> -->

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

        <a href="#" class="footer-icon" aria-label="LinkedIn">
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-linkedin" viewBox="0 0 16 16">
          <path d="M0 1.146C0 .513.526 0 1.175 0h13.65C15.474 0 16 .513 16 1.146v13.708c0 .633-.526 1.146-1.175 1.146H1.175C.526 16 0 15.487 0 14.854zm4.943 12.248V6.169H2.542v7.225zm-1.2-8.212c.837 0 1.358-.554 1.358-1.248-.015-.709-.52-1.248-1.342-1.248S2.4 3.226 2.4 3.934c0 .694.521 1.248 1.327 1.248zm4.908 8.212V9.359c0-.216.016-.432.08-.586.173-.431.568-.878 1.232-.878.869 0 1.216.662 1.216 1.634v3.865h2.401V9.25c0-2.22-1.184-3.252-2.764-3.252-1.274 0-1.845.7-2.165 1.193v.025h-.016l.016-.025V6.169h-2.4c.03.678 0 7.225 0 7.225z"/>
        </svg>
        </a>

        <a href="#" class="footer-icon" aria-label="Email">
          <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-envelope" viewBox="0 0 16 16">
  <path d="M0 4a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2zm2-1a1 1 0 0 0-1 1v.217l7 4.2 7-4.2V4a1 1 0 0 0-1-1zm13 2.383-4.708 2.825L15 11.105zm-.034 6.876-5.64-3.471L8 9.583l-1.326-.795-5.64 3.47A1 1 0 0 0 2 13h12a1 1 0 0 0 .966-.741M1 11.105l4.708-2.897L1 5.383z"/>
</svg>
        </a>
      </div>
    </div>
    
    <div class="footer-divider"></div>
    
    <div class="footer-bottom">
      <div class="container">
        <span class="copyright"><div>© 2025 NullByte </div>| <div>All rights reserved </div> | <div>Worldline </div>| <div>Google Cloud</div></span>
        <div class="footer-bottom-links">
          <a href="#" class="footer-bottom-link">Privacy Policy</a>
          <a href="#" class="footer-bottom-link">Terms of Service</a>
          <a href="#" class="footer-bottom-link">Source</a>
          <a href="#" class="footer-bottom-link">Contact</a>
          <a href="#" class="footer-bottom-link">Docs</a>
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
  margin: 0 auto;
  padding: 0rem 3rem;
}

.footer-content {
  display: flex;
  align-items: center;
  justify-content: space-between;
    padding: 2.6rem 6rem 2.1rem 5rem;
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
  margin-top: 7px;
  height: 45px;
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
  padding: 0.7rem 0;
  background: rgb(0 110 110);
}

.footer-bottom .container {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.copyright {
    font-size: 0.8rem;
    opacity: 0.8;
    font-weight: 300;
    display: flex;
    gap: 0.5rem;
}

.footer-bottom-links {
  display: flex;
  gap: 1.3rem;
  font-size: 0.8rem;
}

.footer-bottom-link {
  color: white;
  text-decoration: none;
  font-size: 0.8rem;
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