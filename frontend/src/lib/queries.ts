/**
 * Shared React Query hooks — fetch once, reuse everywhere.
 * staleTime controls how long cached data is considered fresh
 * (no re-fetch on page switch during this window).
 */
import { useQuery } from "@tanstack/react-query";
import {
  fetchDashboardOverview,
  fetchAlerts,
  fetchIncidents,
  fetchSystemHealth,
  fetchSystemResources,
  fetchServicesLive,
  fetchDagFlow,
  fetchSarImages,
  fetchPredictionFiles,
  fetchAnomalyStats,
  fetchModelResults,
  fetchConfidenceHistogram,
  fetchActiveRuns,
  // Analytics
  fetchAnalyticsTrends,
  fetchAnalyticsPeakHours,
  fetchAnalyticsRegionalDensity,
  fetchAnalyticsConfidenceDistribution,
  fetchAnalyticsDetectionLatency,
  fetchAnalyticsOperationalKPIs,
  fetchAnalyticsIncidentLifecycle,
  // Vessels
  fetchVessels,
  fetchVesselWatchlist,
  fetchVesselDetail,
  fetchVesselTimeline,
  fetchVesselBehavior,
} from "./api";

const MINUTE = 60_000;

// Dashboard overview — refresh every 60 s, cache for 60 s
export const useDashboardOverview = () =>
  useQuery({
    queryKey: ["dashboard", "overview"],
    queryFn: fetchDashboardOverview,
    staleTime: MINUTE,
    refetchInterval: MINUTE,
  });

// Active alerts — refresh every 30 s
export const useAlerts = () =>
  useQuery({
    queryKey: ["alerts"],
    queryFn: fetchAlerts,
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

// Incidents list — refresh every 30 s
export const useIncidents = (limit = 100) =>
  useQuery({
    queryKey: ["incidents", limit],
    queryFn: () => fetchIncidents({ limit }),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

// System health — refresh every 30 s
export const useSystemHealth = () =>
  useQuery({
    queryKey: ["system", "health"],
    queryFn: fetchSystemHealth,
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

// Resource metrics — refresh every 10 s
export const useSystemResources = () =>
  useQuery({
    queryKey: ["system", "resources"],
    queryFn: fetchSystemResources,
    staleTime: 10_000,
    refetchInterval: 10_000,
  });

// Services live — refresh every 60 s
export const useServicesLive = () =>
  useQuery({
    queryKey: ["system", "services-live"],
    queryFn: fetchServicesLive,
    staleTime: MINUTE,
    refetchInterval: MINUTE,
  });

// Pipeline data — these change rarely, cache for 5 min
export const useDagFlow = () =>
  useQuery({
    queryKey: ["pipeline", "dag-flow"],
    queryFn: fetchDagFlow,
    staleTime: 5 * MINUTE,
  });

export const useSarImages = () =>
  useQuery({
    queryKey: ["pipeline", "sar-images"],
    queryFn: fetchSarImages,
    staleTime: 5 * MINUTE,
  });

export const usePredictionFiles = () =>
  useQuery({
    queryKey: ["pipeline", "prediction-files"],
    queryFn: fetchPredictionFiles,
    staleTime: 5 * MINUTE,
  });

export const useAnomalyStats = () =>
  useQuery({
    queryKey: ["pipeline", "anomaly-stats"],
    queryFn: fetchAnomalyStats,
    staleTime: 2 * MINUTE,
    refetchInterval: 2 * MINUTE,
  });

export const useModelResults = () =>
  useQuery({
    queryKey: ["pipeline", "model-results"],
    queryFn: fetchModelResults,
    staleTime: 10 * MINUTE,
  });

export const useConfidenceHistogram = () =>
  useQuery({
    queryKey: ["pipeline", "confidence-histogram"],
    queryFn: fetchConfidenceHistogram,
    staleTime: 2 * MINUTE,
    refetchInterval: 2 * MINUTE,
  });

export const useActiveRuns = () =>
  useQuery({
    queryKey: ["pipeline", "active-runs"],
    queryFn: fetchActiveRuns,
    staleTime: 10_000,
    refetchInterval: 10_000,
  });

// ── Analytics Intelligence Hooks ──

export const useAnalyticsTrends = (period: string = 'weekly', weeks: number = 12) =>
  useQuery({
    queryKey: ["analytics", "trends", period, weeks],
    queryFn: () => fetchAnalyticsTrends(period, weeks),
    staleTime: 2 * MINUTE,
  });

export const useAnalyticsPeakHours = () =>
  useQuery({
    queryKey: ["analytics", "peak-hours"],
    queryFn: fetchAnalyticsPeakHours,
    staleTime: 5 * MINUTE,
  });

export const useAnalyticsRegionalDensity = () =>
  useQuery({
    queryKey: ["analytics", "regional-density"],
    queryFn: fetchAnalyticsRegionalDensity,
    staleTime: 5 * MINUTE,
  });

export const useAnalyticsConfidenceDistribution = () =>
  useQuery({
    queryKey: ["analytics", "confidence-distribution"],
    queryFn: fetchAnalyticsConfidenceDistribution,
    staleTime: 2 * MINUTE,
  });

export const useAnalyticsDetectionLatency = () =>
  useQuery({
    queryKey: ["analytics", "detection-latency"],
    queryFn: fetchAnalyticsDetectionLatency,
    staleTime: 2 * MINUTE,
  });

export const useAnalyticsOperationalKPIs = () =>
  useQuery({
    queryKey: ["analytics", "operational-kpis"],
    queryFn: fetchAnalyticsOperationalKPIs,
    staleTime: MINUTE,
    refetchInterval: MINUTE,
  });

export const useAnalyticsIncidentLifecycle = () =>
  useQuery({
    queryKey: ["analytics", "incident-lifecycle"],
    queryFn: fetchAnalyticsIncidentLifecycle,
    staleTime: 2 * MINUTE,
  });

// ── Vessel Intelligence Hooks ──

export const useVessels = (params?: { sort_by?: 'risk_score' | 'incident_count' | 'last_seen', limit?: number, offset?: number }) =>
  useQuery({
    queryKey: ["vessels", "list", params],
    queryFn: () => fetchVessels(params),
    staleTime: MINUTE,
  });

export const useVesselWatchlist = (limit: number = 10) =>
  useQuery({
    queryKey: ["vessels", "watchlist", limit],
    queryFn: () => fetchVesselWatchlist(limit),
    staleTime: MINUTE,
    refetchInterval: MINUTE,
  });

export const useVesselDetail = (vesselId: string) =>
  useQuery({
    queryKey: ["vessels", "detail", vesselId],
    queryFn: () => fetchVesselDetail(vesselId),
    staleTime: MINUTE,
    enabled: Boolean(vesselId?.trim()),
  });

export const useVesselTimeline = (vesselId: string) =>
  useQuery({
    queryKey: ["vessels", "timeline", vesselId],
    queryFn: () => fetchVesselTimeline(vesselId),
    staleTime: MINUTE,
    enabled: Boolean(vesselId?.trim()),
  });

export const useVesselBehavior = (vesselId: string) =>
  useQuery({
    queryKey: ["vessels", "behavior", vesselId],
    queryFn: () => fetchVesselBehavior(vesselId),
    staleTime: MINUTE,
    enabled: Boolean(vesselId?.trim()),
  });
