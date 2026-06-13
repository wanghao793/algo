import axios from 'axios'

export const api = axios.create({
  baseURL: '/api/v2',
  headers: { 'X-API-Version': '2.0' },
  timeout: 30_000,
})

// ── Types ─────────────────────────────────────────────────────────────────

export interface ApiMeta {
  timestamp_utc: string
  api_version: string
  session_id?: string
  domain?: string
}

export interface ApiError {
  code: string
  message: string
  detail?: unknown
  http_status: number
}

export interface ApiResponse<T = unknown> {
  success: boolean
  data: T
  meta: ApiMeta
  error?: ApiError
}

export interface DomainStatus {
  domain: string
  full_name: string
  class: string
  layer: number
  depends_on: string[]
  status: 'pending' | 'available' | 'in_progress' | 'complete' | 'blocked'
}

export interface HealthData {
  gateway: string
  r_mcp_server: {
    status: string
    r_version: string
    sdtm_oak: string
    timestamp_utc: string
  }
}

// ── API helpers ────────────────────────────────────────────────────────────

export async function fetchHealth(): Promise<ApiResponse<HealthData>> {
  const { data } = await api.get<ApiResponse<HealthData>>('/health')
  return data
}

export async function fetchDagStatus(sessionId: string): Promise<ApiResponse<DomainStatus[]>> {
  const { data } = await api.get<ApiResponse<DomainStatus[]>>(`/dag/status/${sessionId}`)
  return data
}

export async function fetchDagAvailable(sessionId: string): Promise<ApiResponse<string[]>> {
  const { data } = await api.get<ApiResponse<string[]>>(`/dag/available/${sessionId}`)
  return data
}
