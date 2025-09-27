<script setup>
import Header from '@/components/Header.vue';
import Footer from './components/Footer.vue';
import { RouterView } from 'vue-router';
</script>

<template>
  <Header />
  <RouterView :key="$route.fullPath" />
  <Footer />
</template>
