import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { DomainStatus } from '@/api/client'
import { fetchDagStatus, fetchDagAvailable } from '@/api/client'

export const STATUS_COLOR: Record<string, string> = {
  pending:     '#9ca3af',   // gray
  available:   '#3b82f6',   // blue
  in_progress: '#f59e0b',   // amber
  complete:    '#22c55e',   // green
  blocked:     '#ef4444',   // red
}

export const useDagStore = defineStore('dag', () => {
  const domains = ref<DomainStatus[]>([])
  const available = ref<string[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)

  const byDomain = computed(() =>
    Object.fromEntries(domains.value.map(d => [d.domain, d]))
  )

  const completedCount = computed(() =>
    domains.value.filter(d => d.status === 'complete').length
  )

  async function loadStatus(sessionId: string) {
    loading.value = true
    error.value = null
    try {
      const [statusRes, availRes] = await Promise.all([
        fetchDagStatus(sessionId),
        fetchDagAvailable(sessionId),
      ])
      if (statusRes.success) domains.value = statusRes.data
      if (availRes.success) available.value = availRes.data
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Unknown error'
    } finally {
      loading.value = false
    }
  }

  // Called when an SSE domain_update event arrives
  function applyUpdate(event: { domain: string; status: string }) {
    const d = domains.value.find(x => x.domain === event.domain)
    if (d) d.status = event.status as DomainStatus['status']
  }

  return { domains, available, loading, error, byDomain, completedCount, loadStatus, applyUpdate }
})
