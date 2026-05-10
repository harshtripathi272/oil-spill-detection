export const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000/api/v1";
export const API_ROOT = process.env.NEXT_PUBLIC_API_ROOT_URL || API_BASE.replace(/\/api\/v1\/?$/, '') || "http://localhost:8000";

async function fetchJson(path: string, options: RequestInit = {}) {
  const res = await fetch(`${API_BASE}${path}`, options);
  if (!res.ok) {
    throw new Error(`Failed to fetch ${path} (${res.status})`);
  }
  return res.json();
}

export async function fetchDashboardOverview() {
  return fetchJson('/dashboard/overview');
}

export async function fetchAlerts() {
  return fetchJson('/alerts');
}

export type FetchIncidentsParams = {
  limit?: number;
  skip?: number;
  status?: string;
  min_confidence?: number;
};

export async function fetchIncidents(params?: FetchIncidentsParams) {
  const q = new URLSearchParams();
  q.set('limit', String(params?.limit ?? 10));
  if (params?.skip != null) q.set('skip', String(params.skip));
  if (params?.status) q.set('status', params.status);
  if (params?.min_confidence != null) q.set('min_confidence', String(params.min_confidence));
  return fetchJson(`/incidents?${q.toString()}`);
}

export async function fetchSystemHealth() {
  return fetchJson('/system/health');
}

export async function fetchSystemResources() {
  return fetchJson('/system/resources');
}

export async function fetchLogFiles() {
  return fetchJson('/logs/files');
}

export async function fetchLogFileContent(filename: string, lines: number = 100, search?: string) {
  const params = new URLSearchParams({ lines: lines.toString() });
  if (search) params.append('search', search);
  return fetchJson(`/logs/files/${filename}/content?${params}`);
}

export async function fetchRecentLogs(service?: string, level?: string, limit: number = 50) {
  const params = new URLSearchParams({ limit: limit.toString() });
  if (service) params.append('service', service);
  if (level) params.append('level', level);
  return fetchJson(`/logs/recent?${params}`);
}

export function getWebSocketUrl() {
  if (process.env.NEXT_PUBLIC_API_WS_URL) {
    return process.env.NEXT_PUBLIC_API_WS_URL;
  }

  if (API_ROOT) {
    return `${API_ROOT.replace(/^http/, 'ws')}/ws/updates`;
  }

  return "ws://localhost:8000/ws/updates";
}

/* ── New Pipeline APIs ── */

export async function fetchDagFlow() {
  return fetchJson('/pipeline/dag-flow');
}

export async function fetchSarImages() {
  return fetchJson('/pipeline/sar-images');
}

export async function fetchPredictionFiles() {
  return fetchJson('/pipeline/prediction-files');
}

export async function fetchModelResults() {
  return fetchJson('/pipeline/model-results');
}

export async function fetchAnomalyStats() {
  return fetchJson('/pipeline/anomaly-stats');
}

export async function fetchServicesLive() {
  return fetchJson('/system/services-live');
}

export async function fetchStatusDistribution() {
  return fetchJson('/dashboard/charts/status-distribution');
}

export async function acknowledgeAlert(alertId: number) {
  return fetchJson(`/alerts/${alertId}/acknowledge`, { method: 'POST' });
}

export async function updateIncidentStatus(incidentId: string, status: string) {
  return fetchJson(`/incidents/${incidentId}/status?status=${encodeURIComponent(status)}`, {
    method: 'PUT',
  });
}

export function getLogDownloadUrl(filename: string) {
  return `${API_BASE}/logs/download/${filename}`;
}

export function getSarImageUrl(filename: string) {
  return `${API_ROOT}/sar-images/${filename}`;
}

export function getPredictionImageUrl(filename: string) {
  return `${API_ROOT}/prediction-images/${filename}`;
}

/** Resolve stored path or filename to a browser URL (same rules as the incidents table). */
export function resolveIncidentAssetUrl(
  path: string | null | undefined,
  kind: 'sar' | 'prediction'
): string | null {
  if (!path || typeof path !== 'string') return null;
  const trimmed = path.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) return trimmed;
  const name = trimmed.includes('/') ? trimmed.split('/').pop()! : trimmed;
  if (!name) return null;
  return kind === 'sar' ? getSarImageUrl(name) : getPredictionImageUrl(name);
}

export async function fetchConfidenceHistogram() {
  return fetchJson('/pipeline/confidence-histogram');
}

export async function fetchActiveRuns() {
  return fetchJson('/pipeline/active-runs');
}

/* ── Analytics Intelligence APIs ── */

export async function fetchAnalyticsTrends(period: string = 'weekly', weeks: number = 12) {
  return fetchJson(`/analytics/trends?period=${period}&weeks=${weeks}`);
}

export async function fetchAnalyticsPeakHours() {
  return fetchJson('/analytics/peak-hours');
}

export async function fetchAnalyticsRegionalDensity() {
  return fetchJson('/analytics/regional-density');
}

export async function fetchAnalyticsConfidenceDistribution() {
  return fetchJson('/analytics/confidence-distribution');
}

export async function fetchAnalyticsDetectionLatency() {
  return fetchJson('/analytics/detection-latency');
}

export async function fetchAnalyticsOperationalKPIs() {
  return fetchJson('/analytics/operational-kpis');
}

export async function fetchAnalyticsIncidentLifecycle() {
  return fetchJson('/analytics/incident-lifecycle');
}

/* ── Vessel Intelligence APIs ── */

export type FetchVesselsParams = {
  sort_by?: 'risk_score' | 'incident_count' | 'last_seen';
  limit?: number;
  offset?: number;
};

export async function fetchVessels(params?: FetchVesselsParams) {
  const q = new URLSearchParams();
  if (params?.sort_by) q.set('sort_by', params.sort_by);
  if (params?.limit != null) q.set('limit', String(params.limit));
  if (params?.offset != null) q.set('offset', String(params.offset));
  return fetchJson(`/vessels?${q.toString()}`);
}

export async function fetchVesselWatchlist(limit: number = 10) {
  return fetchJson(`/vessels/watchlist?limit=${limit}`);
}

export async function fetchVesselDetail(vesselId: string) {
  return fetchJson(`/vessels/${encodeURIComponent(vesselId)}`);
}

export async function fetchVesselTimeline(vesselId: string) {
  return fetchJson(`/vessels/${encodeURIComponent(vesselId)}/timeline`);
}

export async function fetchVesselBehavior(vesselId: string) {
  return fetchJson(`/vessels/${encodeURIComponent(vesselId)}/behavior`);
}

export function getLogStreamUrl(services?: string[], tail = 20) {
  const svc = services?.join(",") || "anomaly_detector,ingestion,stream_processor,trigger_bridge";
  return `${API_BASE}/logs/stream?services=${encodeURIComponent(svc)}&tail=${tail}`;
}
