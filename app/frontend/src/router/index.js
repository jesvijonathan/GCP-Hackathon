import { createRouter, createWebHistory } from 'vue-router';
import HomeView from '@/views/HomeView.vue';
import NotFoundView from '@/views/NotFoundView.vue';
import JobView from '@/views/JobView.vue';
import AddJobView from '@/views/AddJobView.vue';
import EditJobView from '@/views/EditJobView.vue';
import DashboardView from '@/views/DashboardView.vue'; // Updated import
import MerchantOnboarding from '@/components/MerchantOnboarding.vue';
import About from '@/views/AboutView.vue';
import MerchantView from '@/views/MerchantView.vue';

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'home',
      component: HomeView,
    },
    {
      path: '/login',
      name: 'login',
      component: HomeView,
    },
    {
      path: '/dashboard',
      name: 'dashboard',
      component: DashboardView, // Updated to use DashboardView instead of Dashboard
      meta: {
        title: 'Merchant Dashboard',
        requiresAuth: true // Optional: add if you have authentication
      }
    },
    {
      path: '/merchant-onboarding',
      name: 'merchant-onboarding',
      component: MerchantOnboarding,
    },
    {
      path: '/merchant/:id',
      name: 'MerchantDetails',
      component: MerchantView,
    },
    // Optional: Add alternative routes for merchants
    {
      path: '/merchants',
      redirect: '/dashboard' // Redirect /merchants to /dashboard
    },
    // Optional: Catch all 404 route (if you want to use NotFoundView)
    {
      path: '/:pathMatch(.*)*',
      name: 'NotFound',
      component: NotFoundView
    }
  ],
});

// Optional: Add navigation guards if needed
router.beforeEach((to, from, next) => {
  // Example: Set page title
  if (to.meta && to.meta.title) {
    document.title = to.meta.title;
  }
  
  // Example: Check authentication for protected routes
  if (to.meta && to.meta.requiresAuth) {
    // Add your authentication check logic here
    // const isAuthenticated = checkUserAuth();
    // if (!isAuthenticated) {
    //   next({ name: 'login' });
    //   return;
    // }
  }
  
  next();
});

export default router;