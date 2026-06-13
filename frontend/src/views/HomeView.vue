<template>
  <main style="font-family: system-ui, sans-serif; max-width: 900px; margin: 0 auto; padding: 2rem;">
    <h1>SDTM 全域自动化平台 <small style="font-size:0.5em; color:#6b7280">v2.0</small></h1>

    <!-- Health card -->
    <section style="border:1px solid #e5e7eb; border-radius:8px; padding:1rem; margin-bottom:1.5rem;">
      <h2 style="margin:0 0 .75rem">服务状态</h2>

      <div v-if="healthLoading" style="color:#6b7280">检查中…</div>
      <div v-else-if="healthError" style="color:#ef4444">{{ healthError }}</div>
      <div v-else-if="health">
        <p>网关：<span style="color:#22c55e">● {{ health.gateway }}</span></p>
        <p>R MCP Server：<span :style="{color: health.r_mcp_server.status==='ok'?'#22c55e':'#ef4444'}">
          ● {{ health.r_mcp_server.status }}
        </span> &nbsp; {{ health.r_mcp_server.r_version }}</p>
        <p style="font-size:.85em; color:#6b7280">sdtm.oak {{ health.r_mcp_server.sdtm_oak }}</p>
      </div>
    </section>

    <!-- Session input -->
    <section style="margin-bottom:1.5rem; display:flex; gap:.5rem; align-items:center;">
      <label>Session ID：</label>
      <input v-model="sessionId" placeholder="YYYYMMDDHHmmss"
             style="border:1px solid #d1d5db; border-radius:4px; padding:.3rem .6rem; width:14rem;" />
      <button @click="loadDag" :disabled="dagStore.loading"
              style="padding:.3rem 1rem; background:#3b82f6; color:#fff; border:none; border-radius:4px; cursor:pointer;">
        载入 DAG
      </button>
      <span v-if="sseConnected" style="color:#22c55e; font-size:.85em">● 实时推送已连接</span>
      <span v-else style="color:#9ca3af; font-size:.85em">○ 未连接</span>
    </section>

    <!-- DAG table -->
    <section v-if="dagStore.domains.length">
      <h2>域进度（{{ dagStore.completedCount }} / {{ dagStore.domains.length }} 完成）</h2>
      <table style="width:100%; border-collapse:collapse; font-size:.9em;">
        <thead>
          <tr style="border-bottom:2px solid #e5e7eb; text-align:left;">
            <th style="padding:.4rem .5rem;">层</th>
            <th style="padding:.4rem .5rem;">域</th>
            <th style="padding:.4rem .5rem;">全名</th>
            <th style="padding:.4rem .5rem;">依赖</th>
            <th style="padding:.4rem .5rem;">状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="d in dagStore.domains" :key="d.domain"
              style="border-bottom:1px solid #f3f4f6;">
            <td style="padding:.4rem .5rem; color:#6b7280;">L{{ d.layer }}</td>
            <td style="padding:.4rem .5rem; font-weight:600;">{{ d.domain }}</td>
            <td style="padding:.4rem .5rem;">{{ d.full_name }}</td>
            <td style="padding:.4rem .5rem; font-size:.85em; color:#6b7280;">
              {{ d.depends_on.join(', ') || '—' }}
            </td>
            <td style="padding:.4rem .5rem;">
              <span :style="{
                background: STATUS_COLOR[d.status],
                color:'#fff',
                padding:'.15rem .5rem',
                borderRadius:'12px',
                fontSize:'.8em',
              }">{{ d.status }}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </section>
  </main>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { fetchHealth } from '@/api/client'
import type { HealthData } from '@/api/client'
import { useDagStore, STATUS_COLOR } from '@/stores/dag'
import { useSSE } from '@/composables/useSSE'

const dagStore = useDagStore()

// ── Health ─────────────────────────────────────────────────────────────────
const health = ref<HealthData | null>(null)
const healthLoading = ref(true)
const healthError = ref<string | null>(null)

fetchHealth()
  .then(r => { if (r.success) health.value = r.data })
  .catch(e => { healthError.value = e.message })
  .finally(() => { healthLoading.value = false })

// ── DAG + SSE ─────────────────────────────────────────────────────────────
const sessionId = ref('')
let sseRef: ReturnType<typeof useSSE> | null = null
const sseConnected = ref(false)

async function loadDag() {
  if (!sessionId.value) return
  await dagStore.loadStatus(sessionId.value)

  // (re-)connect SSE for this session
  if (sseRef) {
    sseConnected.value = false
    sseRef = null
  }
  sseRef = useSSE(sessionId.value)
  watch(sseRef.connected, v => { sseConnected.value = v })
  watch(sseRef.lastEvent, evt => {
    if (evt?.type === 'domain_update') dagStore.applyUpdate(evt as any)
  })
}
</script>
