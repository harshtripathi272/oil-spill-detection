'use client';

import { useEffect, useState } from 'react';
import styles from './analytics.module.css';
import { API_BASE } from '@/lib/api';

export default function Analytics() {
  const [statsBreakdown, setStatsBreakdown] = useState<Record<string, number>>({});
  const [geoData, setGeoData] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const [statusRes, geoRes] = await Promise.all([
          fetch(`${API_BASE}/incidents/stats/status-breakdown`),
          fetch(`${API_BASE}/incidents/stats/geographic-distribution`),
        ]);
        if (statusRes.ok) setStatsBreakdown(await statusRes.json());
        if (geoRes.ok) setGeoData(await geoRes.json());
      } catch (err) {
        console.error('Analytics fetch error:', err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const statusColors: Record<string, string> = {
    detected: 'var(--status-detected)',
    confirmed: 'var(--status-confirmed)',
    resolved: 'var(--status-resolved)',
    false_positive: 'var(--status-false-pos)',
  };

  const totalStatus = Object.values(statsBreakdown).reduce((a, b) => a + b, 0) || 1;
  const totalGeo = Object.values(geoData).reduce((a, b) => a + b, 0) || 1;

  return (
    <div className={`${styles.page} animate-enter`}>
      <div className={styles.pageHeader}>
        <h1>Analytics & Reporting</h1>
        <p className={styles.subtitle}>
          Temporal analysis, geographic distribution, and model performance insights.
        </p>
      </div>

      <div className={styles.grid}>
        {/* Status Breakdown */}
        <div className={`${styles.chartSection} card`}>
          <h3 className={styles.cardTitle}>Incident Status Breakdown</h3>
          {loading ? (
            <p className={styles.loading}>Loading…</p>
          ) : (
            <div className={styles.barList}>
              {Object.entries(statsBreakdown).map(([status, count]) => (
                <div key={status} className={styles.barRow}>
                  <div className={styles.barLabel}>
                    <span className={styles.barDot} style={{ background: statusColors[status] || 'var(--text-dim)' }} />
                    <span>{status.replace(/_/g, ' ')}</span>
                  </div>
                  <div className={styles.barTrack}>
                    <div
                      className={styles.barFill}
                      style={{
                        width: `${(count / totalStatus) * 100}%`,
                        background: statusColors[status] || 'var(--text-dim)',
                      }}
                    />
                  </div>
                  <span className={styles.barCount}>{count}</span>
                </div>
              ))}
              {Object.keys(statsBreakdown).length === 0 && <p className={styles.empty}>No status data available</p>}
            </div>
          )}
        </div>

        {/* Geographic Distribution */}
        <div className={`${styles.chartSection} card`}>
          <h3 className={styles.cardTitle}>Geographic Distribution</h3>
          {loading ? (
            <p className={styles.loading}>Loading…</p>
          ) : (
            <div className={styles.barList}>
              {Object.entries(geoData).map(([region, count]) => (
                <div key={region} className={styles.barRow}>
                  <div className={styles.barLabel} style={{ minWidth: 120 }}>
                    <span>{region}</span>
                  </div>
                  <div className={styles.barTrack}>
                    <div
                      className={styles.barFill}
                      style={{
                        width: `${(count / totalGeo) * 100}%`,
                        background: 'var(--accent-blue)',
                      }}
                    />
                  </div>
                  <span className={styles.barCount}>{count} ({Math.round((count / totalGeo) * 100)}%)</span>
                </div>
              ))}
              {Object.keys(geoData).length === 0 && <p className={styles.empty}>No geographic data available</p>}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
