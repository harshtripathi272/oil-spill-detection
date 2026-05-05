'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  TrendingUp,
  TrendingDown,
  AlertTriangle,
  Eye,
  ChevronRight,
} from 'lucide-react';
import styles from '@/app/page.module.css';
import { fetchDashboardOverview, fetchAlerts, getWebSocketUrl } from '@/lib/api';
import Link from 'next/link';

interface DashboardData {
  stats?: any;
  alerts?: any[];
  incidents?: any[];
}

export default function DashboardOverview() {
  const [data, setData] = useState<DashboardData>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function init() {
      try {
        const overview = await fetchDashboardOverview();
        const alertsData = await fetchAlerts();
        setData({
          stats: overview.stats,
          alerts: alertsData.slice(0, 3),
          incidents: overview.recent_incidents,
        });
      } catch (err) {
        console.error('Dashboard fetch error:', err);
      } finally {
        setLoading(false);
      }
    }
    init();

    /* WebSocket for live updates */
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
    return () => { socket?.close(); };
  }, []);

  const stats = data.stats || {};
  const alerts = useMemo(() => data.alerts || [], [data.alerts]);
  const incidents = useMemo(() => data.incidents || [], [data.incidents]);

  const kpis = [
    {
      title: 'Total Incidents',
      value: stats.total_incidents ?? '--',
      trend: '+12% MoM',
      trendDir: 'up' as const,
      color: 'var(--accent-blue)',
    },
    {
      title: 'Active',
      value: stats.active_incidents ?? '--',
      trend: '+2 MoM',
      trendDir: 'up' as const,
      color: 'var(--warning)',
    },
    {
      title: 'Success Rate',
      value: stats.total_dag_runs
        ? `${Math.round((stats.successful_runs / Math.max(stats.total_dag_runs, 1)) * 100)}%`
        : stats.success_rate ?? '--',
      trend: '+0.5% MoM',
      trendDir: 'up' as const,
      color: 'var(--success)',
    },
    {
      title: 'Avg Confidence',
      value: stats.avg_confidence_score?.toFixed(2) ?? '--',
      trend: '-0.02 MoM',
      trendDir: 'down' as const,
      color: 'var(--text-secondary)',
    },
  ];

  // Mock sparkline bars
  const sparkBars = [4, 6, 3, 7, 5, 8, 6, 9, 7, 10, 8, 6];

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
              <div className={styles.sparkline}>
                {sparkBars.map((h, j) => (
                  <div key={j} className={styles.sparkBar} style={{ height: `${h * 3}px`, background: kpi.color }} />
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Middle Row: Charts + Alerts */}
      <div className={styles.midRow}>
        {/* Incident Trends */}
        <div className={`${styles.chartCard} card`}>
          <h3 className={styles.cardTitle}>Incident Trends (30d)</h3>
          <div className={styles.barChart}>
            {[4, 7, 5, 8, 12, 15, 22, 18, 14, 9, 11, 16, 19, 25, 20, 15, 10, 12, 8, 14].map((h, i) => (
              <div key={i} className={styles.barWrap}>
                <div className={styles.bar} style={{ height: `${h * 4}px` }} />
                {i % 5 === 0 && <span className={styles.barLabel}>{i}d</span>}
              </div>
            ))}
          </div>
        </div>

        {/* Status Distribution */}
        <div className={`${styles.chartCard} card`}>
          <h3 className={styles.cardTitle}>Status Distribution</h3>
          <div className={styles.distGrid}>
            <div className={styles.donut}>
              <svg viewBox="0 0 36 36" className={styles.donutSvg}>
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="var(--border-primary)" strokeWidth="3" />
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="var(--status-detected)" strokeWidth="3"
                  strokeDasharray="45 55" strokeDashoffset="25" strokeLinecap="round" />
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="var(--status-confirmed)" strokeWidth="3"
                  strokeDasharray="35 65" strokeDashoffset="80" strokeLinecap="round" />
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="var(--status-resolved)" strokeWidth="3"
                  strokeDasharray="15 85" strokeDashoffset="45" strokeLinecap="round" />
              </svg>
            </div>
            <div className={styles.distLegend}>
              <div className={styles.legendRow}>
                <span className={styles.legendDot} style={{ background: 'var(--status-detected)' }} />
                <span>Detected</span><span className={styles.legendVal}>45%</span>
              </div>
              <div className={styles.legendRow}>
                <span className={styles.legendDot} style={{ background: 'var(--status-confirmed)' }} />
                <span>Confirmed</span><span className={styles.legendVal}>35%</span>
              </div>
              <div className={styles.legendRow}>
                <span className={styles.legendDot} style={{ background: 'var(--status-false-pos)' }} />
                <span>False Pos</span><span className={styles.legendVal}>15%</span>
              </div>
              <div className={styles.legendRow}>
                <span className={styles.legendDot} style={{ background: 'var(--status-resolved)' }} />
                <span>Resolved</span><span className={styles.legendVal}>5%</span>
              </div>
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
                  <button className={styles.alertBtn}>View Details</button>
                  <button className={styles.alertBtnPrimary}>Acknowledge</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Bottom Row */}
      <div className={styles.bottomRow}>
        {/* Processing Time */}
        <div className={`${styles.smallChart} card`}>
          <h3 className={styles.cardTitle}>Processing Time</h3>
          <div className={styles.barChart} style={{ height: 120 }}>
            {[28, 45, 35, 55, 40].map((h, i) => (
              <div key={i} className={styles.barWrap} style={{ flex: 1 }}>
                <div className={styles.bar} style={{ height: `${h * 2}px`, background: 'var(--accent-blue)' }} />
              </div>
            ))}
          </div>
        </div>

        {/* Model Performance */}
        <div className={`${styles.smallChart} card`}>
          <h3 className={styles.cardTitle}>Model Performance</h3>
          <div className={styles.modelBars}>
            {['M1', 'M2', 'M3', 'M4'].map((m, i) => (
              <div key={i} className={styles.modelRow}>
                <span className={styles.modelLabel}>{m}</span>
                <div className={styles.modelTrack}>
                  <div className={styles.modelFill} style={{ width: `${75 + i * 5}%` }} />
                </div>
              </div>
            ))}
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
              <th>Time Ago</th>
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
