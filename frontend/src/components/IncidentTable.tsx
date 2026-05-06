'use client';

import { useEffect, useState } from 'react';
import { ChevronDown, Download, Filter, Search } from 'lucide-react';
import styles from './IncidentTable.module.css';
import { fetchIncidents } from '@/lib/api';

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
}

export default function IncidentTable() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [statusFilter, setStatusFilter] = useState('all');
  const [confThreshold, setConfThreshold] = useState(0);

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

  return (
    <div className={`${styles.page} animate-enter`}>
      {/* Filter Bar */}
      <div className={styles.filterBar}>
        <div className={styles.filterGroup}>
          <span className={styles.filterLabel}>Status</span>
          <div className={styles.chipRow}>
            {['all', 'detected', 'confirmed'].map((s) => (
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

        <div className={styles.filterGroup}>
          <span className={styles.filterLabel}>Time Range</span>
          <select className={styles.select}>
            <option>Last 30 Days</option>
            <option>Last 7 Days</option>
            <option>Last 24 Hours</option>
          </select>
        </div>

        <button className={styles.moreFilters}>
          <Filter size={14} />
          More Filters
        </button>
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
                <>
                  <tr
                    key={inc.id}
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

                  {/* Expanded Detail Row */}
                  {expandedId === inc.id && (
                    <tr key={`${inc.id}-detail`} className={styles.detailRow}>
                      <td colSpan={7}>
                        <div className={styles.detailGrid}>
                          <div className={styles.detailSection}>
                            <h4>Vessel Name</h4>
                            <p className={styles.detailValue}>UNKNOWN VESSEL</p>
                          </div>
                          <div className={styles.detailSection}>
                            <h4>MMSI / IMO</h4>
                            <p className={styles.detailValue}>--- / ---</p>
                          </div>
                          <div className={styles.detailSection}>
                            <h4>Detection Source</h4>
                            <p className={styles.detailValue}>SAR Satellite (Sentinel-1)</p>
                          </div>
                          <div className={styles.detailActions}>
                            <button className={styles.btnPrimary}>Investigate Full Details</button>
                            <button className={styles.btnSecondary}>Task Aerial Asset</button>
                            <button className={styles.btnOutline}>Update Status to Resolved</button>
                          </div>
                        </div>
                        <div className={styles.analystNotes}>
                          <h4>Analyst Notes</h4>
                          <p>High RCS signature consistent with mid-size trawler operating dark in restricted zone. No AIS transmission detected within 50nm radius.</p>
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>

          {/* Footer */}
          <div className={styles.tableFooter}>
            <span className={styles.footerInfo}>
              Showing 1 to {filtered.length} of {incidents.length} incidents | {selected.size} selected
            </span>
            <div className={styles.footerActions}>
              <button className={styles.btnExport} disabled={selected.size === 0}>
                <Download size={14} /> Export Selected
              </button>
              <button className={styles.btnUpdateStatus} disabled={selected.size === 0}>
                Update Status ▾
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
