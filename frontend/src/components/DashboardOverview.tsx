'use client';

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { TrendingUp, TrendingDown, ChevronRight, Activity, AlertTriangle, GitBranch, Satellite, BarChart3, Ship } from 'lucide-react';
import styles from '@/app/page.module.css';
import {
  fetchDashboardOverview, fetchAlerts, fetchLogFileContent,
  getWebSocketUrl, acknowledgeAlert, fetchDagFlow,
  fetchSarImages, fetchAnomalyStats, getSarImageUrl,
} from '@/lib/api';
import Link from 'next/link';

interface DashboardData {
  stats?: any;
  alerts?: any[];
  incidents?: any[];
  status_distribution?: any;
}

interface LogLine {
  raw: string;
  level: string;
  timestamp: string;
  message: string;
  isAnomaly: boolean;
}

function parseLine(raw: string): LogLine {
  const m = raw.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ - \S+ - (\w+) - (.+)$/);
  const timestamp = m ? m[1] : '';
  const level = m ? m[2] : 'INFO';
  const message = m ? m[3].trim() : raw.trim();
  const isAnomaly = raw.includes('[ANOMALY DETECTED]') || raw.includes('🚨');
  return { raw, level, timestamp, message, isAnomaly };
}

export default function DashboardOverview() {
  const [data, setData] = useState<DashboardData>({});
  const [loading, setLoading] = useState(true);
  const [logLines, setLogLines] = useState<LogLine[]>([]);
  const logRef = useRef<HTMLDivElement>(null);
  const isFetchingLogs = useRef(false);

  /* Pipeline state */
  const [dagFlow, setDagFlow] = useState<any>(null);
  const [sarImages, setSarImages] = useState<any[]>([]);
  const [anomalyStats, setAnomalyStats] = useState<any>(null);

  const fetchLogs = useCallback(async () => {
    if (isFetchingLogs.current) return;
    isFetchingLogs.current = true;
    try {
      const result = await fetchLogFileContent('anomaly_detector.log', 60);
      const lines: LogLine[] = (result.content as string)
        .split('\n')
        .filter((l: string) => l.trim())
        .map(parseLine);
      setLogLines(lines);
    } catch { /* ignore */ } finally {
      isFetchingLogs.current = false;
    }
  }, []);

  useEffect(() => {
    async function init() {
      try {
        const [overview, alertsData, dagData, sarData, anomalyData] = await Promise.all([
          fetchDashboardOverview(),
          fetchAlerts(),
          fetchDagFlow().catch(() => null),
          fetchSarImages().catch(() => ({ images: [] })),
          fetchAnomalyStats().catch(() => null),
        ]);
        setData({
          stats: overview.stats,
          alerts: alertsData.slice(0, 3),
          incidents: overview.recent_incidents,
          status_distribution: overview.status_distribution,
        });
        setDagFlow(dagData);
        setSarImages(sarData.images || []);
        setAnomalyStats(anomalyData);
      } catch (err) {
        console.error('Dashboard fetch error:', err);
      } finally {
        setLoading(false);
      }
    }
    init();

    fetchLogs();
    const logTimer = setInterval(fetchLogs, 8000);

    let socket: WebSocket | null = null;
    const connect = () => {
      try {
        socket = new WebSocket(getWebSocketUrl());
        socket.addEventListener('message', (event) => {
          try {
            const msg = JSON.parse(event.data);
            if (msg.type === 'dashboard_update') {
              setData((cur) => ({
                ...cur,
                stats: msg.stats ?? cur.stats,
                alerts: msg.alerts?.slice(0, 3) ?? cur.alerts,
              }));
            }
          } catch { /* ignore bad JSON */ }
        });
        socket.addEventListener('close', () => setTimeout(connect, 5000));
      } catch { /* swallow */ }
    };
    connect();
    return () => {
      socket?.close();
      clearInterval(logTimer);
    };
  }, [fetchLogs]);

  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [logLines]);

  const stats = data.stats || {};
  const alerts = useMemo(() => data.alerts || [], [data.alerts]);
  const incidents = useMemo(() => data.incidents || [], [data.incidents]);

  /* Compute real status distribution */
  const statusDist = useMemo(() => {
    const sd = data.status_distribution?.data;
    if (!sd?.statuses || !sd?.counts) return [];
    const total = sd.counts.reduce((a: number, b: number) => a + b, 0) || 1;
    const colors: Record<string, string> = {
      detected: 'var(--status-detected)',
      confirmed: 'var(--status-confirmed)',
      resolved: 'var(--status-resolved)',
      false_positive: 'var(--status-false-pos)',
    };
    return sd.statuses.map((s: string, i: number) => ({
      label: s.replace(/_/g, ' '),
      count: sd.counts[i],
      pct: Math.round((sd.counts[i] / total) * 100),
      color: colors[s.toLowerCase()] || 'var(--text-dim)',
    }));
  }, [data.status_distribution]);

  const handleAcknowledge = async (alertId: number) => {
    try {
      await acknowledgeAlert(alertId);
      setData((cur) => ({
        ...cur,
        alerts: (cur.alerts || []).filter((a) => a.id !== alertId),
      }));
    } catch { /* ignore */ }
  };

  const kpis = [
    {
      title: 'Total Incidents',
      value: stats.total_incidents ?? '--',
      trend: 'All time',
      trendDir: 'up' as const,
      color: 'var(--accent-blue)',
    },
    {
      title: 'Active Now',
      value: stats.active_incidents ?? '--',
      trend: 'Detected + Confirmed',
      trendDir: 'up' as const,
      color: 'var(--warning)',
    },
    {
      title: 'Avg Confidence',
      value: stats.avg_confidence_score != null
        ? `${(stats.avg_confidence_score * 100).toFixed(1)}%`
        : '--',
      trend: 'Detection quality',
      trendDir: 'up' as const,
      color: 'var(--success)',
    },
    {
      title: 'Resolved',
      value: stats.resolved_incidents ?? '--',
      trend: 'Closed cases',
      trendDir: 'down' as const,
      color: 'var(--text-secondary)',
    },
  ];

  /* Compute real bar chart from incidents_over_time if available, else from incidents */
  const barData = useMemo(() => {
    const iot = (data as any)?.incidents_over_time?.data;
    if (iot?.dates?.length > 0 && iot?.counts?.length > 0) {
      return iot.counts as number[];
    }
    // Fallback: group incidents by day
    if (incidents.length === 0) return [];
    const byCounts: Record<string, number> = {};
    incidents.forEach((inc: any) => {
      if (!inc.detection_time) return;
      const d = inc.detection_time.split('T')[0];
      byCounts[d] = (byCounts[d] || 0) + 1;
    });
    return Object.values(byCounts);
  }, [data, incidents]);

  const maxBar = Math.max(1, ...barData);

  return (
    <div className={`${styles.page} animate-enter`}>
      {/* KPI Row */}
      <div className={styles.kpiRow}>
        {kpis.map((kpi, i) => (
          <div key={i} className={`${styles.kpiCard} card`}>
            <div className={styles.kpiTop}>
              <span className={styles.kpiTitle}>{kpi.title}</span>
              <span className={styles.kpiIcon} style={{ color: kpi.color }}>●</span>
            </div>
            <div className={styles.kpiValue} style={{ color: kpi.color }}>{kpi.value}</div>
            <div className={styles.kpiBottom}>
              <span className={`${styles.kpiTrend} ${kpi.trendDir === 'up' ? styles.trendUp : styles.trendDown}`}>
                {kpi.trendDir === 'up' ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                {kpi.trend}
              </span>
            </div>
          </div>
        ))}
      </div>

      {/* Middle Row: Charts + Alerts */}
      <div className={styles.midRow}>
        {/* Incident Trends — real data */}
        <div className={`${styles.chartCard} card`}>
          <h3 className={styles.cardTitle}>Incident Trends</h3>
          <div className={styles.barChart}>
            {barData.length === 0 ? (
              <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>No incident data yet</span>
            ) : (
              barData.map((h, i) => (
                <div key={i} className={styles.barWrap}>
                  <div className={styles.bar} style={{ height: `${(h / maxBar) * 100}px` }} />
                </div>
              ))
            )}
          </div>
        </div>

        {/* Status Distribution — real data */}
        <div className={`${styles.chartCard} card`}>
          <h3 className={styles.cardTitle}>Status Distribution</h3>
          <div className={styles.distGrid}>
            <div className={styles.donut}>
              <svg viewBox="0 0 36 36" className={styles.donutSvg}>
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="var(--border-primary)" strokeWidth="3" />
                {statusDist.reduce((acc: any[], item: any, idx: number) => {
                  const offset = idx === 0 ? 25 : acc[idx - 1].nextOffset;
                  acc.push({
                    el: (
                      <circle key={idx} cx="18" cy="18" r="15.9" fill="none" stroke={item.color} strokeWidth="3"
                        strokeDasharray={`${item.pct} ${100 - item.pct}`} strokeDashoffset={offset} strokeLinecap="round" />
                    ),
                    nextOffset: offset - item.pct,
                  });
                  return acc;
                }, []).map((a: any) => a.el)}
              </svg>
            </div>
            <div className={styles.distLegend}>
              {statusDist.length === 0 ? (
                <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>No data</span>
              ) : (
                statusDist.map((item: any, i: number) => (
                  <div key={i} className={styles.legendRow}>
                    <span className={styles.legendDot} style={{ background: item.color }} />
                    <span>{item.label}</span><span className={styles.legendVal}>{item.pct}%</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Active Alerts */}
        <div className={`${styles.alertsCard} card`}>
          <div className={styles.alertsHeader}>
            <h3 className={styles.cardTitle}>Active Alerts</h3>
            <span className={styles.alertCount}>{alerts.length}</span>
          </div>
          <div className={styles.alertsList}>
            {alerts.length === 0 && !loading && (
              <div className={styles.empty}>No active alerts</div>
            )}
            {alerts.map((alert, i) => (
              <div
                key={i}
                className={`${styles.alertItem} ${
                  alert.level === 'critical' ? styles.alertCritical :
                  alert.level === 'warning' ? styles.alertWarning : styles.alertInfo
                }`}
              >
                <div className={styles.alertTop}>
                  <span className={styles.alertLevel}>
                    {alert.level?.toUpperCase()} - ZN-{String(alert.id || i).padStart(2, '0')}
                  </span>
                  <span className={styles.alertTime}>
                    {alert.created_at ? new Date(alert.created_at).toLocaleTimeString() : 'now'}
                  </span>
                </div>
                <p className={styles.alertMsg}>{alert.message}</p>
                <div className={styles.alertActions}>
                  <Link href="/incidents" className={styles.alertBtn}>View Details</Link>
                  <button className={styles.alertBtnPrimary} onClick={() => handleAcknowledge(alert.id)}>
                    Acknowledge
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Live Anomaly Log Feed */}
      <div className={`${styles.tableSection} card`} style={{ marginBottom: '1rem' }}>
        <div className={styles.tableHeader}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <Activity size={14} style={{ color: 'var(--accent-blue)' }} />
            <h3 className={styles.cardTitle} style={{ margin: 0 }}>Anomaly Detector — Live Feed</h3>
            <span style={{
              fontSize: 10, fontWeight: 600, letterSpacing: '0.08em',
              color: 'var(--success)', background: 'rgba(16,185,129,0.1)',
              padding: '2px 6px', borderRadius: 4, marginLeft: 4
            }}>LIVE</span>
          </div>
          <Link href="/system-health" className={styles.viewAllLink}>
            Full Log Explorer <ChevronRight size={14} />
          </Link>
        </div>
        <div
          ref={logRef}
          style={{
            fontFamily: "'IBM Plex Mono', monospace",
            fontSize: 11,
            lineHeight: 1.7,
            background: '#060d18',
            borderRadius: 6,
            padding: '10px 12px',
            maxHeight: 220,
            overflowY: 'auto',
            border: '1px solid var(--border-primary)',
          }}
        >
          {logLines.length === 0 ? (
            <span style={{ color: 'var(--text-muted)' }}>Loading anomaly detector logs…</span>
          ) : (
            logLines.map((line, i) => (
              <div key={i} style={{
                color: line.isAnomaly
                  ? 'var(--warning)'
                  : line.level === 'ERROR'
                  ? 'var(--danger, #ef4444)'
                  : 'var(--text-secondary)',
                display: 'flex', gap: '0.75rem', alignItems: 'baseline',
              }}>
                {line.isAnomaly && <AlertTriangle size={10} style={{ flexShrink: 0, marginTop: 3, color: 'var(--warning)' }} />}
                <span style={{ color: 'var(--text-muted)', flexShrink: 0, minWidth: 130 }}>
                  {line.timestamp.split(' ')[1] || line.timestamp}
                </span>
                <span>{line.message}</span>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Pipeline Overview Section */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
        {/* DAG Flow Visualization */}
        <div className="card" style={{ padding: '1.25rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1rem' }}>
            <GitBranch size={16} style={{ color: 'var(--accent-blue)' }} />
            <h3 className={styles.cardTitle} style={{ margin: 0 }}>Detection Pipeline (DAG Flow)</h3>
          </div>
          {dagFlow ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              {dagFlow.tasks.map((task: any, idx: number) => (
                <div key={task.id} style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <div style={{
                    width: 24, height: 24, borderRadius: '50%',
                    background: 'var(--accent-blue)',
                    color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: 10, fontWeight: 700, flexShrink: 0,
                  }}>{idx + 1}</div>
                  <div style={{
                    flex: 1, padding: '0.4rem 0.75rem',
                    background: 'var(--bg-tertiary)',
                    borderRadius: 6, border: '1px solid var(--border-primary)',
                  }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>{task.label}</div>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>{task.description}</div>
                  </div>
                  {idx < dagFlow.tasks.length - 1 && (
                    <div style={{ position: 'absolute', left: 11, marginTop: 28, width: 2, height: 8, background: 'var(--border-primary)' }} />
                  )}
                </div>
              ))}
            </div>
          ) : (
            <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading pipeline…</span>
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
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
                <div style={{ background: 'var(--bg-tertiary)', padding: '0.75rem', borderRadius: 8, textAlign: 'center' }}>
                  <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--warning)' }}>{anomalyStats.total_anomalies}</div>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>Total Anomalies</div>
                </div>
                <div style={{ background: 'var(--bg-tertiary)', padding: '0.75rem', borderRadius: 8, textAlign: 'center' }}>
                  <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--accent-blue)' }}>{anomalyStats.unique_vessels}</div>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>Unique Vessels</div>
                </div>
                <div style={{ background: 'var(--bg-tertiary)', padding: '0.75rem', borderRadius: 8, textAlign: 'center' }}>
                  <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--success)' }}>{anomalyStats.avg_anomaly_score}</div>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>Avg Score</div>
                </div>
                <div style={{ background: 'var(--bg-tertiary)', padding: '0.75rem', borderRadius: 8, textAlign: 'center' }}>
                  <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--text-primary)' }}>{anomalyStats.total_log_lines}</div>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>Log Lines</div>
                </div>
              </div>
            ) : (
              <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading stats…</span>
            )}
          </div>

          {/* SAR Images */}
          <div className="card" style={{ padding: '1.25rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.75rem' }}>
              <Satellite size={16} style={{ color: 'var(--success)' }} />
              <h3 className={styles.cardTitle} style={{ margin: 0 }}>SAR Imagery ({sarImages.length})</h3>
            </div>
            {sarImages.length === 0 ? (
              <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>No SAR images available</span>
            ) : (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem' }}>
                {sarImages.filter((img: any) => img.type === 'preprocessed').slice(0, 3).map((img: any, i: number) => (
                  <div key={i} style={{
                    borderRadius: 6, overflow: 'hidden',
                    border: '1px solid var(--border-primary)',
                    background: 'var(--bg-tertiary)',
                  }}>
                    <img
                      src={getSarImageUrl(img.filename)}
                      alt={img.granule_id}
                      style={{ width: '100%', height: 80, objectFit: 'cover' }}
                    />
                    <div style={{ padding: '0.35rem 0.5rem', fontSize: 9, color: 'var(--text-muted)', wordBreak: 'break-all' }}>
                      {img.granule_id.split('_').slice(-3, -1).join(' ')}
                    </div>
                  </div>
                ))}
              </div>
            )}
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
            <tr>
              <th>ID</th>
              <th>Confidence</th>
              <th>Location</th>
              <th>Detected</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={5} className={styles.empty}>Loading…</td></tr>
            )}
            {!loading && incidents.length === 0 && (
              <tr><td colSpan={5} className={styles.empty}>No incidents</td></tr>
            )}
            {incidents.slice(0, 5).map((inc) => (
              <tr key={inc.id}>
                <td className={styles.incId}>{inc.id}</td>
                <td>
                  <div className={styles.confCell}>
                    <div className={styles.confBar}>
                      <div
                        className={styles.confFill}
                        style={{
                          width: `${(inc.confidence_score || 0) * 100}%`,
                          background:
                            inc.confidence_score > 0.8 ? 'var(--success)' :
                            inc.confidence_score > 0.6 ? 'var(--warning)' : 'var(--text-dim)',
                        }}
                      />
                    </div>
                    <span className={styles.confVal}>{inc.confidence_score?.toFixed(2) ?? '--'}</span>
                  </div>
                </td>
                <td className="mono" style={{ color: 'var(--text-secondary)', fontSize: 12 }}>
                  {inc.latitude?.toFixed(1)}°N, {inc.longitude?.toFixed(1)}°E
                </td>
                <td style={{ color: 'var(--text-muted)', fontSize: 13 }}>
                  {inc.detection_time ? new Date(inc.detection_time).toLocaleString() : '--'}
                </td>
                <td>
                  <span className={`status-badge ${inc.status?.toLowerCase().replace(' ', '_')}`}>
                    {inc.status}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
