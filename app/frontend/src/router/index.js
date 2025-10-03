import { createRouter, createWebHistory } from "vue-router";
import { toast } from 'vue3-toastify';
import 'vue3-toastify/dist/index.css';
import HomeView from "@/views/HomeView.vue";
import NotFoundView from "@/views/NotFoundView.vue";
import DashboardView from "@/views/DashboardView.vue"; // Corrected import path
import MerchantOnboarding from "@/components/MerchantOnboarding.vue";
import MerchantView from "@/views/MerchantView.vue";
import SocialUpdates from "@/components/SocialUpdates.vue";
import AboutView from "@/views/AboutView.vue";
import MailboxView from "@/views/MailBoxView.vue";
import NewAboutView from "@/views/NewAboutView.vue";

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: "/",
      name: "home",
      component: HomeView,
    },
    {
      path: "/login",
      name: "login",
      component: HomeView,
    },
    {
      path: "/dashboard",
      name: "dashboard",
      component: DashboardView, // Use the View component here
      meta: {
        title: "Merchant Dashboard",
        // requiresAuth: true // Uncomment if you have authentication guards
      },
    },
    {
      path: "/merchant-onboarding",
      name: "merchant-onboarding",
      component: MerchantOnboarding,
    },
    {
      path: "/merchant/:id",
      name: "MerchantDetails",
      component: MerchantView,
      // Validate merchant existence before entering the route
      beforeEnter: async (to, from, next) => {
        const id = to.params.id;
        if(!id || typeof id !== 'string'){
          toast.error('Invalid merchant identifier');
          return next('/dashboard');
        }
        // Simple in-memory cache to avoid repeated hits during session
        if(!window.__KNOWN_MERCHANTS){ window.__KNOWN_MERCHANTS = new Set(); }
        if(window.__KNOWN_MERCHANTS.has(id)) return next();
        try {
          const resp = await fetch(`http://localhost:8000/v1/merchants/${encodeURIComponent(id)}`);
          if(resp.ok){
            window.__KNOWN_MERCHANTS.add(id);
            return next();
          }
          if(resp.status === 404){
            toast.warning(`Merchant '${id}' not found`);
            return next('/dashboard');
          }
          toast.error(`Merchant lookup failed (${resp.status})`);
          return next('/dashboard');
        } catch(e){
          toast.error('Network error validating merchant');
          return next('/dashboard');
        }
      }
    },
    // Optional: Redirect from /merchants to /dashboard if you want both paths
    {
      path: "/merchants",
      redirect: "/dashboard",
    },
    {
      path: "/explore/:name",
      name: "social-updates",
      component: SocialUpdates,
    },
    {
      path: "/mailbox",
      name: "mailbox",
      component: MailboxView,
    },
    {
      path: "/about",
      name: "about",
      component: NewAboutView,
    },
    // Catch-all route for 404 pages
    {
      path: "/:pathMatch(.*)*",
      name: "NotFound",
      component: NotFoundView,
    },
  ],
});

// Optional: Global navigation guard for setting page titles etc.
router.beforeEach((to, from, next) => {
  if (to.meta && to.meta.title) {
    document.title = to.meta.title;
  }
  // Add authentication checks here if needed
  next();
});

export default router;
