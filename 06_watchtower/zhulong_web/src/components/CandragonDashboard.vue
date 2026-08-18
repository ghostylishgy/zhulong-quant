<script setup>
import { ref } from 'vue'
import CandragonFunnel from './CandragonFunnel.vue'
import GoldPillCard from './GoldPillCard.vue'

const currentTime = ref(new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' }))

setInterval(() => {
  currentTime.value = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })
}, 1000)
</script>

<template>
  <div class="min-h-screen bg-black text-gray-100">
    <header class="bg-gradient-to-r from-gray-950 via-gray-900 to-gray-950 border-b-2 border-amber-900/50 shadow-2xl">
      <div class="container mx-auto px-6 py-6">
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-4">
            <div class="w-12 h-12 bg-gradient-to-br from-amber-600 to-amber-800 rounded-lg flex items-center justify-center shadow-lg shadow-amber-500/50">
              <span class="text-2xl">🐉</span>
            </div>
            <div>
              <h1 class="text-3xl font-black text-transparent bg-clip-text bg-gradient-to-r from-amber-400 to-amber-600 tracking-wider">
                烛龙 AI 量化决策中枢
              </h1>
            </div>
          </div>

          <div class="text-right">
            <div class="text-sm text-gray-500 uppercase tracking-wider">北京时间</div>
            <div class="text-xl font-mono text-amber-500 font-bold">{{ currentTime }}</div>
          </div>
        </div>

        <div class="mt-6 flex items-center gap-6 text-xs">
          <div class="flex items-center gap-2">
            <div class="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
            <span class="text-gray-400">系统核心在线</span>
          </div>
          <div class="flex items-center gap-2">
            <div class="w-2 h-2 bg-amber-500 rounded-full animate-pulse"></div>
            <span class="text-gray-400">推理引擎待命</span>
          </div>
          <div class="flex items-center gap-2">
            <div class="w-2 h-2 bg-blue-500 rounded-full"></div>
            <span class="text-gray-400">数据底座就绪</span>
          </div>
        </div>
      </div>
    </header>

    <main class="container mx-auto px-6 py-8">
      <div class="flex flex-col lg:flex-row gap-8">
        <div class="flex-1">
          <CandragonFunnel />
        </div>

        <div class="flex-1">
          <GoldPillCard />
        </div>
      </div>

      <div class="mt-8 text-center text-xs text-gray-600">
        <p>Powered by DeepSeek & Zhipu</p>
      </div>
    </main>
  </div>
</template>

<style>

body {
  margin: 0;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}
</style>
