import { createRouter, createWebHistory } from "vue-router";
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
    },
    // Optional: Redirect from /merchants to /dashboard if you want both paths
    {
      path: "/merchants",
      redirect: "/dashboard",
    },
    {
      path: "/merchant/:id/social-updates",
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
