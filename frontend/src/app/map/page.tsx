'use client';

import { useEffect, useState } from 'react';
import { MapPin, ExternalLink } from 'lucide-react';
import styles from './page.module.css';
import { fetchIncidents } from '@/lib/api';

interface Incident {
  id: string;
  latitude: number;
  longitude: number;
  confidence_score: number;
  detection_time: string;
  status: string;
}

export default function MapView() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const data = await fetchIncidents();
        setIncidents(data);
      } catch (err) {
        console.error('Failed to load incidents:', err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const statusColor = (s: string) => {
    switch (s?.toLowerCase()) {
      case 'detected': return 'var(--status-detected, #f59e0b)';
      case 'confirmed': return 'var(--status-confirmed, #ef4444)';
      case 'resolved': return 'var(--status-resolved, #22c55e)';
      case 'false_positive': return 'var(--text-dim, #6b7280)';
      default: return 'var(--text-muted)';
    }
  };

  return (
    <div className={`${styles.container} animate-enter`}>
      <header className={styles.header}>
        <h1 className="text-gradient">Incident Locations</h1>
      </header>

      <div className={`${styles.mapContainer} glass-panel`}>
        {/* Legend */}
        <div className={styles.legend}>
          <div className={styles.legendTitle}>Status Legend</div>
          <div className={styles.legendList}>
            <div className={styles.legendItem}><span className={styles.dotRed}></span> Confirmed</div>
            <div className={styles.legendItem}><span className={styles.dotOrange}></span> Detected</div>
            <div className={styles.legendItem}><span className={styles.dotGreen}></span> Resolved</div>
            <div className={styles.legendItem}><span className={styles.dotGray}></span> False Positive</div>
          </div>
        </div>

        {/* Real incident data */}
        <div style={{ padding: '1rem' }}>
          {loading ? (
            <div style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '2rem' }}>
              Loading incident locations…
            </div>
          ) : incidents.length === 0 ? (
            <div style={{ color: 'var(--text-muted)', textAlign: 'center', padding: '2rem' }}>
              No incidents to display
            </div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: '0.75rem' }}>
              {incidents.map((inc) => (
                <div key={inc.id} style={{
                  background: 'var(--bg-tertiary)',
                  border: '1px solid var(--border-primary)',
                  borderRadius: 8,
                  padding: '0.75rem 1rem',
                  display: 'flex',
                  gap: '0.75rem',
                  alignItems: 'flex-start',
                }}>
                  <MapPin size={18} style={{ color: statusColor(inc.status), flexShrink: 0, marginTop: 2 }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                      <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>{inc.id}</span>
                      <span className={`status-badge ${inc.status?.toLowerCase().replace(' ', '_')}`} style={{ fontSize: 10 }}>
                        {inc.status}
                      </span>
                    </div>
                    <div style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)', marginBottom: 4 }}>
                      {inc.latitude?.toFixed(4)}° {inc.latitude >= 0 ? 'N' : 'S'},{' '}
                      {inc.longitude?.toFixed(4)}° {inc.longitude >= 0 ? 'E' : 'W'}
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
                        Conf: {((inc.confidence_score || 0) * 100).toFixed(0)}%
                      </span>
                      <a
                        href={`https://www.google.com/maps/@${inc.latitude},${inc.longitude},10z`}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{ fontSize: 10, color: 'var(--accent-blue)', display: 'flex', alignItems: 'center', gap: 3, textDecoration: 'none' }}
                      >
                        <ExternalLink size={10} /> Google Maps
                      </a>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
