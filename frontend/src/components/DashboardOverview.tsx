'use client';

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { TrendingUp, TrendingDown, ChevronRight, AlertTriangle, GitBranch, Satellite, Ship, Activity } from 'lucide-react';
import styles from '@/app/page.module.css';
import { acknowledgeAlert, getLogStreamUrl, getSarImageUrl } from '@/lib/api';
import {
  useDashboardOverview, useAlerts, useConfidenceHistogram,
  useDagFlow, useSarImages, useAnomalyStats,
} from '@/lib/queries';
import Link from 'next/link';

/* ── Types ─────────────────────────────────────────────────────────────── */

interface LogLine {
  service: string;
  raw: string;
  level: string;
  timestamp: string;
  message: string;
  isAnomaly: boolean;
}

const SVC_COLORS: Record<string, string> = {
  anomaly_detector: '#f59e0b',
  stream_processor: '#3b82f6',
  ingestion:        '#22c55e',
  trigger_bridge:   '#a78bfa',
};

const SVC_LABELS: Record<string, string> = {
  anomaly_detector: 'ANOMALY',
  stream_processor: 'STREAM',
  ingestion:        'INGEST',
  trigger_bridge:   'BRIDGE',
};

function parseSSELine(raw: string, service: string): LogLine {
  const isAnomaly = raw.includes('[ANOMALY DETECTED]') || raw.includes('🚨') || raw.includes('SAR trigger');
  const m = raw.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/);
  const timestamp = m ? m[1].split(' ')[1] : '';
  let level = 'INFO';
  if (raw.includes(' ERROR ') || raw.includes('- ERROR -')) level = 'ERROR';
  else if (raw.includes(' WARNING ') || raw.includes('- WARNING -')) level = 'WARN';
  const msgM = raw.match(/- (INFO|WARNING|ERROR) - (.+)$/);
  const message = msgM ? msgM[2].trim() : raw.trim();
  return { service, raw, level, timestamp, message, isAnomaly };
}

/* ── Component ─────────────────────────────────────────────────────────── */

export default function DashboardOverview() {
  /* React Query — client-side cached data */
  const { data: overview } = useDashboardOverview();
  const { data: alertsData, refetch: refetchAlerts } = useAlerts();
  const { data: histogram } = useConfidenceHistogram();
  const { data: dagFlow } = useDagFlow();
  const { data: sarData } = useSarImages();
  const { data: anomalyStats } = useAnomalyStats();

  /* SSE multi-service live log state */
  const [logLines, setLogLines] = useState<LogLine[]>([]);
  const [activeServices, setActiveServices] = useState<Set<string>>(
    new Set(['anomaly_detector', 'stream_processor', 'ingestion', 'trigger_bridge'])
  );
  const logRef = useRef<HTMLDivElement>(null);
  const esRef = useRef<EventSource | null>(null);

  /* Connect / reconnect SSE */
  useEffect(() => {
    const connect = () => {
      esRef.current?.close();
      const url = getLogStreamUrl([...activeServices], 15);
      const es = new EventSource(url);
      es.onmessage = (evt) => {
        try {
          const payload = JSON.parse(evt.data);
          const line = parseSSELine(payload.line, payload.service);
          setLogLines((prev) => {
            const next = [...prev, line];
            return next.slice(-200); // keep last 200 lines
          });
        } catch { /* skip bad frames */ }
      };
      es.onerror = () => {
        es.close();
        setTimeout(connect, 5000);
      };
      esRef.current = es;
    };
    connect();
    return () => esRef.current?.close();
  }, [activeServices]);

  /* Auto-scroll log feed */
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [logLines]);

  /* Derived data */
  const stats = overview?.stats ?? {};
  const alerts = useMemo(() => (alertsData ?? []).slice(0, 3), [alertsData]);
  const sarImages = useMemo(() => sarData?.images ?? [], [sarData]);

  const handleAcknowledge = useCallback(async (alertId: number) => {
    try {
      await acknowledgeAlert(alertId);
      await refetchAlerts();
    } catch { /* ignore */ }
  }, [refetchAlerts]);

  const kpis = [
    { title: 'Total Incidents', value: stats.total_incidents ?? '--', color: 'var(--accent-blue)', trend: 'All time' },
    { title: 'Active Now',      value: stats.active_incidents ?? '--', color: 'var(--warning)', trend: 'Detected + Confirmed' },
    { title: 'Avg Confidence',  value: stats.avg_confidence_score != null ? `${(stats.avg_confidence_score * 100).toFixed(1)}%` : '--', color: 'var(--success)', trend: 'Detection quality' },
    { title: 'Resolved',        value: stats.resolved_incidents ?? '--', color: 'var(--text-secondary)', trend: 'Closed cases' },
  ];

  /* Status distribution (donut) */
  const statusDist = useMemo(() => {
    const sd = overview?.status_distribution?.data;
    if (!sd?.statuses?.length) return [];
    const total = sd.counts.reduce((a: number, b: number) => a + b, 0) || 1;
    const colors: Record<string, string> = {
      detected: '#f59e0b', confirmed: '#ef4444', resolved: '#22c55e', false_positive: '#6b7280',
    };
    return sd.statuses.map((s: string, i: number) => ({
      label: s.replace(/_/g, ' '), count: sd.counts[i],
      pct: Math.round((sd.counts[i] / total) * 100),
      color: colors[s.toLowerCase()] || '#6b7280',
    }));
  }, [overview]);

  /* Confidence histogram bars */
  const histLabels = histogram?.labels ?? [];
  const histCounts = histogram?.counts ?? [];
  const histMax = Math.max(1, ...histCounts);

  /* Toggle service filter */
  const toggleService = (svc: string) => {
    setActiveServices((prev) => {
      const next = new Set(prev);
      next.has(svc) ? next.delete(svc) : next.add(svc);
      return next.size > 0 ? next : prev; // always keep at least 1
    });
  };

  return (
    <div className={`${styles.page} animate-enter`}>
      {/* KPI Row */}
      <div className={styles.kpiRow}>
        {kpis.map((kpi, i) => (
          <div key={i} className={`${styles.kpiCard} card`}>
            <div className={styles.kpiTop}>
              <span className={styles.kpiTitle}>{kpi.title}</span>
              <span style={{ color: kpi.color }}>●</span>
            </div>
            <div className={styles.kpiValue} style={{ color: kpi.color }}>{kpi.value}</div>
            <div className={styles.kpiBottom}>
              <span className={styles.kpiTrend}>
                <TrendingUp size={12} /> {kpi.trend}
              </span>
            </div>
          </div>
        ))}
      </div>

      {/* Charts + Alerts */}
      <div className={styles.midRow}>
        {/* Confidence Score Distribution (replaces flat Incident Trends chart) */}
        <div className={`${styles.chartCard} card`}>
          <h3 className={styles.cardTitle}>
            Anomaly Confidence Scores
            <span style={{ fontSize: 10, color: 'var(--text-muted)', marginLeft: 8, fontWeight: 400 }}>
              {histogram?.total ?? 0} detections
            </span>
          </h3>
          <div className={styles.barChart} style={{ alignItems: 'flex-end', gap: '0.5rem', padding: '0.5rem 0' }}>
            {histCounts.length === 0 ? (
              <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data yet</span>
            ) : (
              histLabels.map((label: string, i: number) => (
                <div key={i} className={styles.barWrap} style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
                  <span style={{ fontSize: 10, color: 'var(--text-secondary)', fontWeight: 600 }}>{histCounts[i]}</span>
                  <div
                    className={styles.bar}
                    style={{
                      height: `${(histCounts[i] / histMax) * 90}px`,
                      background: i === 3 ? '#ef4444' : i === 2 ? '#f59e0b' : i === 1 ? '#3b82f6' : '#22c55e',
                      borderRadius: '4px 4px 0 0',
                      width: '100%',
                    }}
                  />
                  <span style={{ fontSize: 9, color: 'var(--text-muted)', textAlign: 'center' }}>{label}</span>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Status Distribution Donut */}
        <div className={`${styles.chartCard} card`}>
          <h3 className={styles.cardTitle}>Status Distribution</h3>
          <div className={styles.distGrid}>
            <div className={styles.donut}>
              <svg viewBox="0 0 36 36" className={styles.donutSvg}>
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="var(--border-primary)" strokeWidth="3" />
                {statusDist.reduce((acc: any[], item: any, idx: number) => {
                  const offset = idx === 0 ? 25 : acc[idx - 1].nextOffset;
                  acc.push({
                    el: <circle key={idx} cx="18" cy="18" r="15.9" fill="none" stroke={item.color} strokeWidth="3"
                          strokeDasharray={`${item.pct} ${100 - item.pct}`} strokeDashoffset={offset} strokeLinecap="round" />,
                    nextOffset: offset - item.pct,
                  });
                  return acc;
                }, []).map((a: any) => a.el)}
              </svg>
            </div>
            <div className={styles.distLegend}>
              {statusDist.length === 0
                ? <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data</span>
                : statusDist.map((item: any, i: number) => (
                  <div key={i} className={styles.legendRow}>
                    <span className={styles.legendDot} style={{ background: item.color }} />
                    <span>{item.label}</span>
                    <span className={styles.legendVal}>{item.pct}%</span>
                  </div>
                ))}
            </div>
          </div>
        </div>

        {/* Active Alerts — linked to incidents in DB */}
        <div className={`${styles.alertsCard} card`}>
          <div className={styles.alertsHeader}>
            <h3 className={styles.cardTitle}>Active Alerts</h3>
            <span className={styles.alertCount}>{alerts.length}</span>
          </div>
          <div className={styles.alertsList}>
            {alerts.length === 0 && (
              <div className={styles.empty}>No active alerts</div>
            )}
            {alerts.map((alert: any, i: number) => (
              <div key={i} className={`${styles.alertItem} ${
                alert.level === 'critical' ? styles.alertCritical :
                alert.level === 'warning'  ? styles.alertWarning : styles.alertInfo
              }`}>
                <div className={styles.alertTop}>
                  <span className={styles.alertLevel}>
                    {alert.level?.toUpperCase()} · {alert.incident_id ?? `#${i + 1}`}
                  </span>
                  <span className={styles.alertTime}>
                    {alert.created_at ? new Date(alert.created_at).toLocaleTimeString() : 'live'}
                  </span>
                </div>
                <p className={styles.alertMsg}>{alert.message}</p>
                <div className={styles.alertActions}>
                  <Link href="/incidents" className={styles.alertBtn}>View Incident</Link>
                  <button className={styles.alertBtnPrimary} onClick={() => handleAcknowledge(alert.id)}>
                    Acknowledge
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Multi-Service Live Log Feed */}
      <div className={`${styles.tableSection} card`} style={{ marginBottom: '1rem' }}>
        <div className={styles.tableHeader}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flex: 1 }}>
            <Activity size={14} style={{ color: 'var(--accent-blue)' }} />
            <h3 className={styles.cardTitle} style={{ margin: 0 }}>Live Service Logs</h3>
            <span style={{ fontSize: 10, fontWeight: 600, color: 'var(--success)', background: 'rgba(16,185,129,0.1)', padding: '2px 6px', borderRadius: 4 }}>SSE LIVE</span>
            {/* Service toggle chips */}
            <div style={{ display: 'flex', gap: '0.25rem', marginLeft: '0.5rem' }}>
              {Object.keys(SVC_COLORS).map((svc) => (
                <button
                  key={svc}
                  onClick={() => toggleService(svc)}
                  style={{
                    fontSize: 9, padding: '2px 6px', borderRadius: 4, cursor: 'pointer',
                    border: `1px solid ${SVC_COLORS[svc]}`,
                    background: activeServices.has(svc) ? SVC_COLORS[svc] + '33' : 'transparent',
                    color: SVC_COLORS[svc], fontWeight: 600, letterSpacing: '0.04em',
                  }}
                >
                  {SVC_LABELS[svc]}
                </button>
              ))}
            </div>
          </div>
          <Link href="/system-health" className={styles.viewAllLink}>
            Full Explorer <ChevronRight size={14} />
          </Link>
        </div>
        <div
          ref={logRef}
          style={{
            fontFamily: "'IBM Plex Mono', monospace",
            fontSize: 11, lineHeight: 1.7,
            background: '#060d18', borderRadius: 6,
            padding: '10px 12px', maxHeight: 240, overflowY: 'auto',
            border: '1px solid var(--border-primary)',
          }}
        >
          {logLines.length === 0 ? (
            <span style={{ color: 'var(--text-muted)' }}>Connecting to log stream…</span>
          ) : (
            logLines.map((line, i) => (
              <div key={i} style={{ display: 'flex', gap: '0.5rem', alignItems: 'baseline' }}>
                <span style={{
                  fontSize: 8, fontWeight: 700, letterSpacing: '0.06em', flexShrink: 0,
                  padding: '1px 4px', borderRadius: 3, width: 44, textAlign: 'center',
                  background: SVC_COLORS[line.service] + '22', color: SVC_COLORS[line.service],
                }}>
                  {SVC_LABELS[line.service] ?? line.service.slice(0, 6).toUpperCase()}
                </span>
                {line.isAnomaly && <AlertTriangle size={10} style={{ color: '#f59e0b', flexShrink: 0 }} />}
                <span style={{ color: '#4a5568', flexShrink: 0, minWidth: 65 }}>{line.timestamp}</span>
                <span style={{
                  color: line.isAnomaly ? '#f59e0b' : line.level === 'ERROR' ? '#ef4444' : '#94a3b8',
                  flexShrink: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}>
                  {line.message}
                </span>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Pipeline Overview */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
        {/* DAG Flow */}
        <div className="card" style={{ padding: '1.25rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1rem' }}>
            <GitBranch size={16} style={{ color: 'var(--accent-blue)' }} />
            <h3 className={styles.cardTitle} style={{ margin: 0 }}>Detection Pipeline</h3>
          </div>
          {dagFlow ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
              {dagFlow.tasks.map((task: any, idx: number) => (
                <div key={task.id} style={{ display: 'flex', alignItems: 'flex-start', gap: '0.5rem' }}>
                  <div style={{
                    width: 22, height: 22, borderRadius: '50%', background: 'var(--accent-blue)',
                    color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: 9, fontWeight: 700, flexShrink: 0, marginTop: 2,
                  }}>{idx + 1}</div>
                  <div style={{
                    flex: 1, padding: '0.35rem 0.6rem',
                    background: 'var(--bg-tertiary)', borderRadius: 5,
                    border: '1px solid var(--border-primary)',
                  }}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-primary)' }}>{task.label}</div>
                    <div style={{ fontSize: 9, color: 'var(--text-muted)' }}>{task.description}</div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading…</span>
          )}
        </div>

        {/* Right column: Anomaly Stats + SAR Images */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          {/* Anomaly Stats */}
          <div className="card" style={{ padding: '1.25rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.75rem' }}>
              <Ship size={16} style={{ color: 'var(--warning)' }} />
              <h3 className={styles.cardTitle} style={{ margin: 0 }}>Anomaly Summary</h3>
            </div>
            {anomalyStats ? (
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
                {[
                  { label: 'Total Anomalies', value: anomalyStats.total_anomalies, color: '#f59e0b' },
                  { label: 'Unique Vessels',  value: anomalyStats.unique_vessels,  color: '#3b82f6' },
                  { label: 'Avg Score',       value: anomalyStats.avg_anomaly_score, color: '#22c55e' },
                  { label: 'Log Lines',       value: anomalyStats.total_log_lines, color: '#94a3b8' },
                ].map((item) => (
                  <div key={item.label} style={{ background: 'var(--bg-tertiary)', padding: '0.6rem', borderRadius: 6, textAlign: 'center' }}>
                    <div style={{ fontSize: 20, fontWeight: 700, color: item.color }}>{item.value}</div>
                    <div style={{ fontSize: 9, color: 'var(--text-muted)', marginTop: 2 }}>{item.label}</div>
                  </div>
                ))}
              </div>
            ) : <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading…</span>}
          </div>

          {/* SAR Images */}
          <div className="card" style={{ padding: '1.25rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.75rem' }}>
              <Satellite size={16} style={{ color: 'var(--success)' }} />
              <h3 className={styles.cardTitle} style={{ margin: 0 }}>SAR Imagery ({sarImages.length})</h3>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.4rem' }}>
              {sarImages.filter((img: any) => img.type === 'preprocessed').slice(0, 3).map((img: any, i: number) => (
                <div key={i} style={{ borderRadius: 5, overflow: 'hidden', border: '1px solid var(--border-primary)' }}>
                  <img src={getSarImageUrl(img.filename)} alt={img.granule_id}
                    style={{ width: '100%', height: 72, objectFit: 'cover' }} />
                  <div style={{ padding: '0.25rem 0.4rem', fontSize: 8, color: 'var(--text-muted)' }}>
                    {img.granule_id.split('_').slice(-3, -1).join(' ')}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Recent Incidents Table */}
      <div className={`${styles.tableSection} card`}>
        <div className={styles.tableHeader}>
          <h3 className={styles.cardTitle}>Recent Incidents</h3>
          <Link href="/incidents" className={styles.viewAllLink}>View All <ChevronRight size={14} /></Link>
        </div>
        <table className={styles.table}>
          <thead>
            <tr><th>ID</th><th>Confidence</th><th>Location</th><th>Detected</th><th>Status</th></tr>
          </thead>
          <tbody>
            {!overview && <tr><td colSpan={5} className={styles.empty}>Loading…</td></tr>}
            {overview?.recent_incidents?.length === 0 && (
              <tr><td colSpan={5} className={styles.empty}>No incidents</td></tr>
            )}
            {(overview?.recent_incidents ?? []).slice(0, 5).map((inc: any) => (
              <tr key={inc.id}>
                <td className={styles.incId}>{inc.id}</td>
                <td>
                  <div className={styles.confCell}>
                    <div className={styles.confBar}>
                      <div className={styles.confFill} style={{
                        width: `${(inc.confidence_score || 0) * 100}%`,
                        background: inc.confidence_score > 0.8 ? 'var(--success)' : inc.confidence_score > 0.6 ? 'var(--warning)' : 'var(--text-dim)',
                      }} />
                    </div>
                    <span className={styles.confVal}>{inc.confidence_score?.toFixed(2) ?? '--'}</span>
                  </div>
                </td>
                <td style={{ color: 'var(--text-secondary)', fontSize: 12, fontFamily: 'monospace' }}>
                  {inc.latitude?.toFixed(1)}°N, {inc.longitude?.toFixed(1)}°E
                </td>
                <td style={{ color: 'var(--text-muted)', fontSize: 13 }}>
                  {inc.detection_time ? new Date(inc.detection_time).toLocaleString() : '--'}
                </td>
                <td><span className={`status-badge ${inc.status?.toLowerCase().replace(' ', '_')}`}>{inc.status}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
