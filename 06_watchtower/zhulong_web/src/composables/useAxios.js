import axios from 'axios'

const resolveBaseURL = () => {
  const configured = import.meta.env.VITE_API_BASE_URL
  if (configured) return configured

  if (typeof window !== 'undefined') {
    const host = window.location.hostname
    if (host === 'localhost' || host === '127.0.0.1') return 'http://127.0.0.1:8000'
    if (/^192\.168\./.test(host)) return `http://${host}:8000`
    if (host === 'example.com' || host === 'www.example.com') return 'https://api.example.com'
  }

  return 'https://api.example.com'
}

const baseURL = resolveBaseURL()

const http = axios.create({
  baseURL,
  timeout: 120000,
  headers: {
    'Content-Type': 'application/json',
  },
})

const friendlyMessage = (error) => {
  const detail = error?.response?.data?.detail
  if (detail) return detail
  const status = error?.response?.status
  if (status === 401) return '访问口令不正确。'
  if (status === 429) return '访问过于频繁，请稍后再试。'
  if (status >= 500) return '云端服务暂时不可用，请稍后刷新。'
  if (error?.code === 'ECONNABORTED') return '云端通道响应超时，请稍后再试。'
  if (String(error?.message || '').includes('Network Error')) return '云端通道连接异常，请检查网络或稍后刷新。'
  return error?.message || '请求失败，请稍后再试。'
}

http.interceptors.response.use(
  (response) => response,
  (error) => Promise.reject(new Error(friendlyMessage(error))),
)

export function useAxios() {
  const get = async (url, config = {}) => {
    const response = await http.get(url, config)
    return response.data
  }

  const post = async (url, data = {}, config = {}) => {
    const response = await http.post(url, data, config)
    return response.data
  }

  return {
    client: http,
    baseURL,
    get,
    post,
  }
}
