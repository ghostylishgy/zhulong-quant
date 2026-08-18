<script setup>
import { ref, onMounted } from 'vue'

const funnel = ref({ l1: 0, l2: 0, l3: 0, l4: 0 })
const loading = ref(true)
const error = ref(null)

const fetchFunnel = async () => {
  try {
    loading.value = true
    error.value = null
    const response = await fetch('http://192.0.2.10:8000/api/stats/funnel')
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    funnel.value = await response.json()
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

const getWidth = (value, max) => {
  if (max === 0) return 0
  const percent = (value / max) * 100
  return Math.max(percent, value > 0 ? 15 : 0)
}

const getColor = (level) => {
  const colors = {
    l1: 'bg-gray-700',
    l2: 'bg-gray-600',
    l3: 'bg-amber-700',
    l4: 'bg-amber-500'
  }
  return colors[level] || 'bg-gray-700'
}

const getRate = (current, total) => {
  return total > 0 ? ((current / total) * 100).toFixed(2) : '0.00'
}

onMounted(() => {
  fetchFunnel()
  setInterval(fetchFunnel, 30000)
})
</script>

<template>
  <div class="bg-gray-950 border border-gray-800 rounded-lg p-4 md:p-6 shadow-2xl">
    <div class="flex items-center justify-between mb-4 md:mb-6">
      <h2 class="text-lg md:text-xl font-bold text-amber-500 tracking-wider">
        多维智能审计漏斗
      </h2>
      <div v-if="loading" class="text-gray-600 text-xs md:text-sm animate-pulse">加载中...</div>
    </div>

    <div v-if="error" class="text-rose-500 text-xs md:text-sm border border-rose-900 bg-rose-950 p-3 rounded">
      错误: {{ error }}
    </div>

    <div v-else class="space-y-3 md:space-y-4">
      <div class="relative">
        <div class="flex items-center justify-between mb-1">
          <span class="text-gray-400 text-xs md:text-sm font-mono">L1 数据初筛</span>
          <span class="text-gray-300 font-bold text-sm md:text-base">{{ funnel.l1.toLocaleString() }}</span>
        </div>
        <div class="h-10 md:h-12 bg-gray-900 rounded overflow-hidden relative">
          <div
            :class="getColor('l1')"
            :style="{ width: '100%' }"
            class="h-full transition-all duration-500 flex items-center justify-end pr-3 md:pr-4"
          >
            <span class="text-white text-xs font-bold">100%</span>
          </div>
        </div>
      </div>

      <div class="relative">
        <div class="flex items-center justify-between mb-1">
          <span class="text-gray-400 text-xs md:text-sm font-mono">L2 深度推理</span>
          <span class="text-gray-300 font-bold text-sm md:text-base">{{ funnel.l2.toLocaleString() }}</span>
        </div>
        <div class="h-10 md:h-12 bg-gray-900 rounded overflow-hidden relative">
          <div
            :class="getColor('l2')"
            :style="{ width: getWidth(funnel.l2, funnel.l1) + '%', minWidth: funnel.l2 > 0 ? '60px' : '0' }"
            class="h-full transition-all duration-500 flex items-center justify-end pr-3 md:pr-4"
          >
            <span class="text-white text-xs font-bold">{{ getRate(funnel.l2, funnel.l1) }}%</span>
          </div>
        </div>
      </div>

      <div class="relative">
        <div class="flex items-center justify-between mb-1">
          <span class="text-amber-600 text-xs md:text-sm font-mono">L3 战略审计</span>
          <span class="text-amber-400 font-bold text-sm md:text-base">{{ funnel.l3.toLocaleString() }}</span>
        </div>
        <div class="h-10 md:h-12 bg-gray-900 rounded overflow-hidden relative">
          <div
            :class="getColor('l3')"
            :style="{ width: getWidth(funnel.l3, funnel.l1) + '%', minWidth: funnel.l3 > 0 ? '60px' : '0' }"
            class="h-full transition-all duration-500 flex items-center justify-end pr-3 md:pr-4"
          >
            <span v-if="funnel.l3 > 0" class="text-white text-xs font-bold">{{ getRate(funnel.l3, funnel.l1) }}%</span>
          </div>
        </div>
      </div>

      <div class="relative">
        <div class="flex items-center justify-between mb-1">
          <span class="text-amber-500 text-xs md:text-sm font-mono">L4 终极裁决</span>
          <span class="text-amber-300 font-bold text-base md:text-lg">{{ funnel.l4.toLocaleString() }}</span>
        </div>
        <div class="h-12 md:h-14 bg-gray-900 rounded overflow-hidden relative border-2 border-amber-900">
          <div
            :class="getColor('l4')"
            :style="{ width: getWidth(funnel.l4, funnel.l1) + '%', minWidth: funnel.l4 > 0 ? '60px' : '0' }"
            class="h-full transition-all duration-500 flex items-center justify-end pr-3 md:pr-4 shadow-lg shadow-amber-500/50"
          >
            <span v-if="funnel.l4 > 0" class="text-black text-xs md:text-sm font-black">{{ getRate(funnel.l4, funnel.l1) }}%</span>
          </div>
        </div>
      </div>

      <div class="mt-4 md:mt-6 pt-3 md:pt-4 border-t border-gray-800 flex justify-between text-xs text-gray-500">
        <span>淘汰率: <span class="text-rose-500 font-bold">{{ (100 - parseFloat(getRate(funnel.l4, funnel.l1))).toFixed(2) }}%</span></span>
        <span>存活: <span class="text-amber-500 font-bold">{{ funnel.l4 }}/{{ funnel.l1 }}</span></span>
      </div>
    </div>
  </div>
</template>
