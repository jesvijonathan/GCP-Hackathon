<script setup>
// import Navbar from '@/components/Navbar.vue';
import Header from '@/components/Header.vue';
import Footer from './components/Footer.vue';
import { RouterView } from 'vue-router';
</script>

<template>
  <!-- <Navbar /> -->
   <Header />
  <RouterView />
  <Footer />
</template>
