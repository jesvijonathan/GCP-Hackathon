<script setup>
import { ref } from 'vue';

// Import team photos
import pp0 from '@/assets/p0.jpeg';
import pp1 from '@/assets/pp1.jpg';
import pp2 from '@/assets/pp2.jpg';
import pp3 from '@/assets/pp3.jpg';

const teamMembers = [
  {
    name: 'Jesvi Jonathan',
    role: 'Software Engineer',
    department: 'Worldline > MS > WLPFO',
    title: 'THE HARDCODER ⚡',
    image: pp0,
    linkedin: 'https://www.linkedin.com/in/jesvijonathan/'
  },
  {
    name: 'Akileswar PrathapKumar',
    role: 'Software Engineer',
    department: 'Worldline > MS > WLPFO',
    title: 'THE DEPLOYER 👨‍💻',
    image: pp1,
    linkedin: 'https://www.linkedin.com/in/akileswar/'
  },
  {
    name: 'Shrutika Donde',
    role: 'Associate Engineer',
    department: 'Worldline > MS > WLPFO',
    title: 'THE SLIDESHOW MAVEN 🎬',
    image: pp3,
    linkedin: 'https://www.linkedin.com/in/shrutika-donde/'
  },
  {
    name: 'Atharva Kutwal',
    role: 'Associate Engineer',
    department: 'Worldline > MS > WLPFO',
    title: 'THE NARRATOR 🎤',
    image: pp2,
    linkedin: 'https://www.linkedin.com/in/atharva-kutwal-588a8a219/'
  }
];

const techStack = [
  'Vue 3', 'Python', 'MySQL', 'Linux',
  'Multimodal Models', 'Compute Engine', 'Cloud GPU',
  'Cloud SQL', 'Cloud Run', 'Active Directory',
  'Ollama', 'Vertex API', 'GCS Bucket'
];
</script>

<template>
  <div class="about-container">
    <!-- Hero Section -->
    <section class="hero-section">
      <div class="hero-content">
        <h1 class="main-title">About Team Nullbyte</h1>
        <p class="hero-subtitle">Innovating with AI-powered solutions at Worldline</p>
        <div class="hero-decoration"></div>
      </div>
    </section>

    <!-- About Us Section -->
    <section class="section">
      <div class="container">
        <h2 class="section-title">About Us</h2>
        <div class="text-content">
          <p>
            We're a group of four developers working together at Worldline Global Services on innovative projects. 
            Our team includes Jesvi Jonathan, Akileswar Prathapkumar, Atharva Kutwal, and Shrutika Donde. 
            We bring different strengths to the table, handling everything from coding to problem-solving.
          </p>
          <p>
            Whether it's tackling complex bugs, optimizing systems, or developing new features, we work collaboratively to deliver results. 
            Each member plays a unique role, and our teamwork drives innovation and excellence in every project we undertake.
          </p>
        </div>
      </div>
    </section>

    <!-- Team Section -->
    <section class="section team-section">
      <div class="container">
        <h2 class="section-title">Meet The Team</h2>
        <p class="section-subtitle">
          Four passionate developers who joined forces for the Worldline Google Cloud 2025 Hackathon
        </p>
        <div class="team-grid">
          <a 
            v-for="member in teamMembers" 
            :key="member.name"
            :href="member.linkedin"
            target="_blank"
            class="team-card"
          >
            <div class="member-image">
              <img :src="member.image" :alt="member.name" />
              <div class="image-overlay"></div>
            </div>
            <div class="member-info">
              <h3 class="member-name">{{ member.name }}</h3>
              <p class="member-department">{{ member.department }}</p>
              <p class="member-role">{{ member.role }}</p>
              <p class="member-title">{{ member.title }}</p>
            </div>
          </a>
        </div>
      </div>
    </section>

    <!-- Project Section -->
    <section class="section project-section">
      <div class="container">
        <h2 class="section-title">The Making</h2>
        <div class="project-content">
          <div class="feature-grid">
            <div class="feature-card">
              <div class="feature-icon">🤖</div>
              <h3>AI-Powered Triaging</h3>
              <p>Generative AI automates ticket management and improves service efficiency through intelligent routing.</p>
            </div>
            <div class="feature-card">
              <div class="feature-icon">🔗</div>
              <h3>Seamless Integration</h3>
              <p>Works with JIRA, PagerDuty, and Serviceline for a unified ticketing experience.</p>
            </div>
            <div class="feature-card">
              <div class="feature-icon">⚡</div>
              <h3>Smart Automation</h3>
              <p>Skill-based routing system matches tickets to employees based on expertise and performance.</p>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- Tech Stack Section -->
    <section class="section tech-section">
      <div class="container">
        <h2 class="section-title">Tech Stack</h2>
        <p class="tech-description">
          Built with cutting-edge technologies for optimal performance. 
          <a href="https://github.com/jesvijonathan/Nullbyte-Ticketing-System" target="_blank" class="github-link">
            View on GitHub →
          </a>
        </p>
        <div class="tech-grid">
          <div v-for="tech in techStack" :key="tech" class="tech-item">
            <span>{{ tech }}</span>
          </div>
        </div>
      </div>
    </section>

    <!-- Footer -->
    <footer class="footer">
      <div class="container">
        <div class="footer-content">
          <p class="thanks">
            Special thanks to all organizers and participants who made this event possible
          </p>
          <div class="logos">
            <img src="@/assets/Google_2015_logo.svg.webp" alt="Google" class="logo" />
            <img src="@/assets/logo.png" alt="Worldline" class="logo" />
            <img src="@/assets/Nullbyte.svg" alt="Nullbyte" class="logo" />
          </div>
          <div class="footer-credits">
            <span>Made with ❤️ by Team </span>
            <span class="team-name">Nullbyte</span>
          </div>
        </div>
      </div>
    </footer>
  </div>
</template>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

* {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

.about-container {
  background: #ffffff;
  color: #2d3748;
  min-height: 100vh;
  line-height: 1.6;
}

.container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 1.5rem;
}

/* Hero Section */
.hero-section {
  background: linear-gradient(135deg, #0d9488 0%, #14b8a6 50%, #5eead4 100%);
  color: white;
  padding: 5rem 0;
  text-align: center;
  position: relative;
  overflow: hidden;
}

.hero-section::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.05'%3E%3Ccircle cx='30' cy='30' r='2'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E");
}

.hero-content {
  position: relative;
  z-index: 1;
  max-width: 700px;
  margin: 0 auto;
  padding: 0 1.5rem;
}

.main-title {
  font-size: 2.75rem;
  font-weight: 800;
  margin-bottom: 1rem;
  letter-spacing: -0.025em;
}

.hero-subtitle {
  font-size: 1rem;
  opacity: 0.9;
  font-weight: 400;
  margin-bottom: 2rem;
}

.hero-decoration {
  width: 60px;
  height: 4px;
  background: white;
  margin: 0 auto;
  border-radius: 2px;
  opacity: 0.8;
}

/* Sections */
.section {
  padding: 4rem 0;
}

.section-title {
  font-size: 2rem;
  font-weight: 700;
  color: #0d9488;
  text-align: center;
  margin-bottom: 0.75rem;
  letter-spacing: -0.025em;
}

.section-subtitle {
  text-align: center;
  color: #64748b;
  font-size: 0.95rem;
  margin-bottom: 3rem;
  max-width: 600px;
  margin-left: auto;
  margin-right: auto;
  font-weight: 400;
}

/* About Section */
.text-content {
  max-width: 800px;
  margin: 0 auto;
}

.text-content p {
  font-size: 0.95rem;
  line-height: 1.7;
  color: #475569;
  margin-bottom: 1.25rem;
  text-align: justify;
  font-weight: 400;
}

/* Team Section */
.team-section {
  background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
}

.team-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1.5rem;
  margin-top: 3rem;
}

.team-card {
  background: white;
  border-radius: 1rem;
  padding: 1.75rem;
  text-align: center;
  text-decoration: none;
  color: inherit;
  border: 1px solid #e2e8f0;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  position: relative;
  overflow: hidden;
}

.team-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 3px;
  background: linear-gradient(90deg, #0d9488, #14b8a6);
  transform: scaleX(0);
  transition: transform 0.3s ease;
}

.team-card:hover::before {
  transform: scaleX(1);
}

.team-card:hover {
  transform: translateY(-5px);
  box-shadow: 0 10px 25px rgba(13, 148, 136, 0.15);
  border-color: #0d9488;
}

.member-image {
  position: relative;
  margin-bottom: 1.25rem;
  display: inline-block;
}

.member-image img {
  width: 80px;
  height: 80px;
  border-radius: 50%;
  object-fit: cover;
  border: 3px solid #e2e8f0;
  transition: all 0.3s ease;
}

.team-card:hover .member-image img {
  border-color: #0d9488;
}

.member-name {
  font-size: 1rem;
  font-weight: 600;
  color: #1e293b;
  margin-bottom: 0.25rem;
  letter-spacing: -0.01em;
}

.member-department {
  color: #64748b;
  font-size: 0.75rem;
  margin-bottom: 0.25rem;
  font-weight: 400;
}

.member-role {
  color: #475569;
  font-size: 0.8rem;
  margin-bottom: 0.75rem;
  font-weight: 500;
}

.member-title {
  color: #0d9488;
  font-weight: 600;
  font-size: 0.8rem;
  background: #f0fdfa;
  padding: 0.25rem 0.5rem;
  border-radius: 0.375rem;
  display: inline-block;
}

/* Project Section */
.feature-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 2rem;
  margin-top: 2rem;
}

.feature-card {
  background: white;
  padding: 2rem;
  border-radius: 1rem;
  border: 1px solid #e2e8f0;
  text-align: center;
  transition: all 0.3s ease;
}

.feature-card:hover {
  box-shadow: 0 8px 25px rgba(13, 148, 136, 0.1);
  transform: translateY(-2px);
}

.feature-icon {
  font-size: 2rem;
  margin-bottom: 1rem;
}

.feature-card h3 {
  font-size: 1.1rem;
  font-weight: 600;
  color: #1e293b;
  margin-bottom: 0.75rem;
}

.feature-card p {
  font-size: 0.85rem;
  color: #64748b;
  line-height: 1.6;
}

/* Tech Section */
.tech-section {
  background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
}

.tech-description {
  text-align: center;
  color: #64748b;
  font-size: 0.9rem;
  margin-bottom: 2.5rem;
  font-weight: 400;
}

.github-link {
  color: #0d9488;
  text-decoration: none;
  font-weight: 500;
  transition: color 0.3s ease;
}

.github-link:hover {
  color: #0f766e;
}

.tech-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 0.75rem;
  margin-top: 2rem;
}

.tech-item {
  background: white;
  padding: 0.75rem 1rem;
  border-radius: 0.5rem;
  text-align: center;
  font-weight: 500;
  font-size: 0.8rem;
  border: 1px solid #e2e8f0;
  transition: all 0.3s ease;
  color: #475569;
}

.tech-item:hover {
  border-color: #0d9488;
  background: #0d9488;
  color: white;
  transform: translateY(-1px);
}

/* Footer */
.footer {
  background: #1e293b;
  color: white;
  padding: 2.5rem 0;
}

.footer-content {
  text-align: center;
}

.thanks {
  font-size: 0.9rem;
  margin-bottom: 1.5rem;
  color: #cbd5e1;
  font-weight: 400;
}

.logos {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 2rem;
  margin-bottom: 1.5rem;
  flex-wrap: wrap;
}

.logo {
  height: 28px;
  filter: brightness(0) invert(1);
  opacity: 0.7;
  transition: opacity 0.3s ease;
}

.logo:hover {
  opacity: 1;
}

.footer-credits {
  font-size: 0.9rem;
  color: #cbd5e1;
  font-weight: 400;
}

.team-name {
  color: #14b8a6;
  font-weight: 600;
  font-size: 1rem;
}

/* Responsive Design */
@media (max-width: 1024px) {
  .team-grid {
    grid-template-columns: repeat(2, 1fr);
    gap: 1.25rem;
  }
}

@media (max-width: 768px) {
  .main-title {
    font-size: 2.25rem;
  }
  
  .section-title {
    font-size: 1.75rem;
  }
  
  .team-grid {
    grid-template-columns: repeat(2, 1fr);
    gap: 1rem;
  }
  
  .team-card {
    padding: 1.5rem;
  }
  
  .member-image img {
    width: 70px;
    height: 70px;
  }
  
  .tech-grid {
    grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  }
  
  .feature-grid {
    grid-template-columns: 1fr;
    gap: 1.5rem;
  }
  
  .container {
    padding: 0 1rem;
  }
}

@media (max-width: 480px) {
  .main-title {
    font-size: 1.875rem;
  }
  
  .hero-section {
    padding: 3rem 0;
  }
  
  .section {
    padding: 2.5rem 0;
  }
  
  .team-grid {
    grid-template-columns: 1fr;
  }
  
  .logos {
    gap: 1rem;
  }
}
</style>