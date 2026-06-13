import { ref, onUnmounted } from 'vue'

export interface SSEEvent {
  type: string
  [key: string]: unknown
}

/**
 * Composable that maintains an EventSource connection to
 * GET /api/v2/events/{sessionId} and delivers parsed events.
 *
 * Usage:
 *   const { connected, lastEvent } = useSSE('20260601120000')
 */
export function useSSE(sessionId: string) {
  const connected = ref(false)
  const lastEvent = ref<SSEEvent | null>(null)
  const error = ref<string | null>(null)

  const source = new EventSource(`/api/v2/events/${sessionId}`)

  source.addEventListener('connected', () => {
    connected.value = true
    error.value = null
  })

  source.addEventListener('domain_update', (e: MessageEvent) => {
    try {
      lastEvent.value = JSON.parse(e.data) as SSEEvent
    } catch {
      // ignore malformed events
    }
  })

  source.onerror = () => {
    connected.value = false
    error.value = 'SSE connection lost — reconnecting…'
    // EventSource auto-reconnects after onerror
  }

  onUnmounted(() => source.close())

  return { connected, lastEvent, error }
}
