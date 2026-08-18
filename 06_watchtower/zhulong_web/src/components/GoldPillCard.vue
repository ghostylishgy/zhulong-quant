<script setup>
import { ref, onMounted } from 'vue'

const approved = ref([])
const loading = ref(true)
const error = ref(null)

const fetchApproved = async () => {
  try {
    loading.value = true
    error.value = null
    const response = await fetch('http://192.0.2.10:8000/api/results/approved')
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    approved.value = await response.json()
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchApproved()
  setInterval(fetchApproved, 30000)
})
</script>

<template>
  <div class="bg-gray-950 border border-gray-800 rounded-lg p-4 md:p-6 shadow-2xl">
    <div class="flex items-center justify-between mb-4 md:mb-6">
      <h2 class="text-lg md:text-xl font-bold text-amber-500 tracking-wider">
        Alpha 核心决策池
      </h2>
      <div v-if="loading" class="text-gray-600 text-xs md:text-sm animate-pulse">加载中...</div>
    </div>

    <div v-if="error" class="text-rose-500 text-xs md:text-sm border border-rose-900 bg-rose-950 p-3 rounded">
      错误: {{ error }}
    </div>

    <div v-else-if="approved.length === 0" class="flex flex-col items-center justify-center py-12 md:py-16 px-4">
      <div class="relative w-24 h-24 md:w-32 md:h-32 mb-6">
        <div class="absolute inset-0 border-4 border-amber-900/30 rounded-full"></div>
        <div class="absolute inset-0 border-4 border-transparent border-t-amber-500 rounded-full animate-spin" style="animation-duration: 3s;"></div>
        <div class="absolute inset-4 border-4 border-transparent border-t-amber-600 rounded-full animate-spin" style="animation-duration: 2s; animation-direction: reverse;"></div>
        <div class="absolute inset-0 flex items-center justify-center">
          <div class="w-3 h-3 bg-amber-500 rounded-full animate-pulse"></div>
        </div>
      </div>

      <div class="text-center space-y-3">
        <div class="text-amber-500 text-base md:text-lg font-black tracking-widest">
          等待最终裁决
        </div>
        <div class="text-gray-500 text-xs md:text-sm">
          AI 深度演算中... (等待最终策略评估)
        </div>
        <div class="text-gray-700 text-xs mt-4">
          <span class="inline-block w-2 h-2 bg-amber-600 rounded-full animate-pulse mr-2"></span>
          深度战略审计进行中
        </div>
      </div>
    </div>

    <div v-else class="space-y-3 md:space-y-4 max-h-[500px] md:max-h-[600px] overflow-y-auto">
      <div
        v-for="(item, index) in approved"
        :key="index"
        class="relative bg-gradient-to-br from-gray-900 to-black border border-amber-900/30 rounded-lg p-4 md:p-5 hover:border-amber-700 transition-all"
      >
        <div class="absolute top-2 right-2 md:top-3 md:right-3 opacity-20">
          <div class="text-amber-500 font-black text-3xl md:text-5xl rotate-12">
            通过
          </div>
        </div>

        <div class="relative z-10">
          <div class="flex items-start justify-between mb-3">
            <div>
              <h3 class="text-base md:text-lg font-bold text-amber-400">{{ item.symbol }}</h3>
              <p class="text-xs md:text-sm text-gray-400 mt-1">{{ item.name }}</p>
            </div>
            <div class="bg-amber-500 text-black px-2 md:px-3 py-1 rounded text-xs font-black">
              L4
            </div>
          </div>

          <div class="mt-3 md:mt-4 space-y-2">
            <div class="text-xs text-gray-500 uppercase">裁决理由</div>
            <p class="text-xs md:text-sm text-gray-300 leading-relaxed">
              {{ item.reasoning }}
            </p>
          </div>

          <div class="mt-3 md:mt-4 pt-3 border-t border-gray-800 flex justify-between text-xs text-gray-600">
            <span>{{ item.trade_date }}</span>
            <span class="text-amber-600">已通过</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
