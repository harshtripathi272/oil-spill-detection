'use client';

import React, { useEffect, useState } from 'react';
import { ChevronDown, Download, Filter } from 'lucide-react';
import styles from './IncidentTable.module.css';
import { fetchIncidents, updateIncidentStatus } from '@/lib/api';

interface Incident {
  id: string;
  latitude: number;
  longitude: number;
  confidence_score: number;
  detection_time: string;
  status: string;
  model_version?: string;
  processing_time?: number;
  extra_metadata?: any;
  sar_image_path?: string;
  processed_image_path?: string;
}

export default function IncidentTable() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [statusFilter, setStatusFilter] = useState('all');
  const [confThreshold, setConfThreshold] = useState(0);
  const [statusDropdownId, setStatusDropdownId] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      try {
        const data = await fetchIncidents();
        setIncidents(data);
      } catch (err) {
        setError((err as Error).message);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const toggleSelectAll = () => {
    if (selected.size === filtered.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(filtered.map((i) => i.id)));
    }
  };

  const filtered = incidents.filter((inc) => {
    if (statusFilter !== 'all' && inc.status?.toLowerCase() !== statusFilter) return false;
    if (confThreshold > 0 && (inc.confidence_score || 0) < confThreshold) return false;
    return true;
  });

  const confColor = (score: number) =>
    score > 0.8 ? 'var(--success)' : score > 0.6 ? 'var(--warning)' : 'var(--text-dim)';

  const handleStatusUpdate = async (incidentId: string, newStatus: string) => {
    try {
      await updateIncidentStatus(incidentId, newStatus);
      setIncidents((prev) =>
        prev.map((inc) => (inc.id === incidentId ? { ...inc, status: newStatus } : inc))
      );
      setStatusDropdownId(null);
    } catch (err) {
      console.error('Status update failed:', err);
    }
  };

  const handleExportSelected = () => {
    const selectedIncidents = incidents.filter((i) => selected.has(i.id));
    if (selectedIncidents.length === 0) return;

    const headers = ['ID', 'Latitude', 'Longitude', 'Confidence', 'Status', 'Detection Time'];
    const rows = selectedIncidents.map((i) => [
      i.id, i.latitude, i.longitude, i.confidence_score, i.status, i.detection_time,
    ]);
    const csv = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'incidents_export.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  const statusOptions = ['detected', 'confirmed', 'resolved', 'false_positive'];

  return (
    <div className={`${styles.page} animate-enter`}>
      {/* Filter Bar */}
      <div className={styles.filterBar}>
        <div className={styles.filterGroup}>
          <span className={styles.filterLabel}>Status</span>
          <div className={styles.chipRow}>
            {['all', 'detected', 'confirmed', 'resolved'].map((s) => (
              <button
                key={s}
                className={`${styles.chip} ${statusFilter === s ? styles.chipActive : ''}`}
                onClick={() => setStatusFilter(s)}
              >
                {s === 'all' ? 'All' : `● ${s.charAt(0).toUpperCase() + s.slice(1)}`}
              </button>
            ))}
          </div>
        </div>

        <div className={styles.filterGroup}>
          <span className={styles.filterLabel}>Confidence Threshold</span>
          <div className={styles.sliderWrap}>
            <input
              type="range"
              min="0"
              max="100"
              value={confThreshold * 100}
              onChange={(e) => setConfThreshold(Number(e.target.value) / 100)}
              className={styles.slider}
            />
            <span className={styles.sliderVal}>{(confThreshold).toFixed(2)}+</span>
          </div>
        </div>
      </div>

      {/* Table */}
      {loading ? (
        <div className={styles.loading}>Loading incidents…</div>
      ) : error ? (
        <div className={styles.error}>{error}</div>
      ) : (
        <div className={`${styles.tableWrap} card`}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th style={{ width: 36 }}>
                  <input
                    type="checkbox"
                    checked={selected.size === filtered.length && filtered.length > 0}
                    onChange={toggleSelectAll}
                    className={styles.checkbox}
                  />
                </th>
                <th>Incident ID</th>
                <th>Location (Lat/Lon)</th>
                <th>Confidence</th>
                <th>Detected Time (UTC)</th>
                <th>Status</th>
                <th style={{ width: 36 }}></th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((inc) => (
                <React.Fragment key={inc.id}>
                  <tr
                    className={`${styles.row} ${expandedId === inc.id ? styles.rowExpanded : ''} ${
                      selected.has(inc.id) ? styles.rowSelected : ''
                    }`}
                    onClick={() => setExpandedId(expandedId === inc.id ? null : inc.id)}
                  >
                    <td onClick={(e) => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        checked={selected.has(inc.id)}
                        onChange={() => toggleSelect(inc.id)}
                        className={styles.checkbox}
                      />
                    </td>
                    <td className={styles.incId}>{inc.id}</td>
                    <td className={styles.mono}>
                      ⊕ {inc.latitude?.toFixed(4)}° N, {inc.longitude?.toFixed(4)}° {inc.longitude >= 0 ? 'E' : 'W'}
                    </td>
                    <td>
                      <div className={styles.confCell}>
                        <div className={styles.confBar}>
                          <div
                            className={styles.confFill}
                            style={{
                              width: `${(inc.confidence_score || 0) * 100}%`,
                              background: confColor(inc.confidence_score || 0),
                            }}
                          />
                        </div>
                        <span className={styles.confVal}>.{Math.round((inc.confidence_score || 0) * 100)}</span>
                      </div>
                    </td>
                    <td className={styles.mono}>
                      {inc.detection_time ? new Date(inc.detection_time).toISOString().replace('T', ' ').slice(0, 19) : '--'}
                    </td>
                    <td>
                      <span className={`status-badge ${inc.status?.toLowerCase().replace(' ', '_')}`}>
                        {inc.status}
                      </span>
                    </td>
                    <td>
                      <ChevronDown
                        size={16}
                        className={`${styles.chevron} ${expandedId === inc.id ? styles.chevronOpen : ''}`}
                      />
                    </td>
                  </tr>

                  {/* Expanded Detail Row — Real Data */}
                  {expandedId === inc.id && (
                    <tr key={`${inc.id}-detail`} className={styles.detailRow}>
                      <td colSpan={7}>
                        <div className={styles.detailGrid}>
                          <div className={styles.detailSection}>
                            <h4>Vessel / Source</h4>
                            <p className={styles.detailValue}>
                              {inc.extra_metadata?.vessel_id || inc.extra_metadata?.mmsi || 'N/A'}
                            </p>
                          </div>
                          <div className={styles.detailSection}>
                            <h4>Model Version</h4>
                            <p className={styles.detailValue}>
                              {inc.model_version || 'YOLOv26n-bbox'}
                            </p>
                          </div>
                          <div className={styles.detailSection}>
                            <h4>Processing Time</h4>
                            <p className={styles.detailValue}>
                              {inc.processing_time != null ? `${inc.processing_time.toFixed(2)}s` : 'N/A'}
                            </p>
                          </div>
                          <div className={styles.detailActions}>
                            {inc.sar_image_path && (
                              <a href={inc.sar_image_path} target="_blank" rel="noopener noreferrer" className={styles.btnPrimary}>
                                View SAR Image
                              </a>
                            )}
                            <div style={{ position: 'relative' }}>
                              <button
                                className={styles.btnSecondary}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setStatusDropdownId(statusDropdownId === inc.id ? null : inc.id);
                                }}
                              >
                                Update Status ▾
                              </button>
                              {statusDropdownId === inc.id && (
                                <div style={{
                                  position: 'absolute', top: '100%', left: 0, zIndex: 10,
                                  background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
                                  borderRadius: 6, padding: '0.25rem 0', minWidth: 140,
                                }}>
                                  {statusOptions.map((s) => (
                                    <button
                                      key={s}
                                      onClick={(e) => { e.stopPropagation(); handleStatusUpdate(inc.id, s); }}
                                      style={{
                                        display: 'block', width: '100%', padding: '0.4rem 0.75rem',
                                        background: 'none', border: 'none', textAlign: 'left',
                                        color: 'var(--text-primary)', cursor: 'pointer', fontSize: 12,
                                      }}
                                      onMouseEnter={(e) => (e.currentTarget.style.background = 'var(--bg-tertiary)')}
                                      onMouseLeave={(e) => (e.currentTarget.style.background = 'none')}
                                    >
                                      {s.replace(/_/g, ' ')}
                                    </button>
                                  ))}
                                </div>
                              )}
                            </div>
                          </div>
                        </div>
                        {inc.extra_metadata && Object.keys(inc.extra_metadata).length > 0 && (
                          <div className={styles.analystNotes}>
                            <h4>Incident Metadata</h4>
                            <pre style={{ fontSize: 11, color: 'var(--text-secondary)', whiteSpace: 'pre-wrap' }}>
                              {JSON.stringify(inc.extra_metadata, null, 2)}
                            </pre>
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>

          {/* Footer */}
          <div className={styles.tableFooter}>
            <span className={styles.footerInfo}>
              Showing 1 to {filtered.length} of {incidents.length} incidents | {selected.size} selected
            </span>
            <div className={styles.footerActions}>
              <button className={styles.btnExport} disabled={selected.size === 0} onClick={handleExportSelected}>
                <Download size={14} /> Export Selected
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
