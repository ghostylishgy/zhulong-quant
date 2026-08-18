import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useSessionStore = defineStore('session', () => {
  const user_session = ref('guest')
  const permission_level = ref('observer')

  const setSession = (sessionId, level = permission_level.value) => {
    user_session.value = sessionId || 'guest'
    permission_level.value = level || 'observer'
  }

  return {
    user_session,
    permission_level,
    setSession,
  }
})
