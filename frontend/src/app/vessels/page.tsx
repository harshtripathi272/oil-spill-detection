'use client';

import { useState } from 'react';
import styles from './vessels.module.css';
import { useVessels, useVesselDetail, useVesselBehavior } from '@/lib/queries';
import { Ship, Crosshair, AlertTriangle, Navigation, Map, ShieldAlert } from 'lucide-react';
import { getSarImageUrl } from '@/lib/api';

export default function VesselsPage() {
  const [selectedVesselId, setSelectedVesselId] = useState<string | null>(null);

  const { data: vesselsData, isLoading: loadingList } = useVessels({ sort_by: 'risk_score', limit: 20 });
  const { data: vesselDetail, isLoading: loadingDetail } = useVesselDetail(selectedVesselId || '');
  const { data: vesselBehavior, isLoading: loadingBehavior } = useVesselBehavior(selectedVesselId || '');

  const vessels = vesselsData?.vessels || [];

  return (
    <div className={`${styles.page} animate-enter`}>
      <div className={styles.pageHeader}>
        <h1>Vessel Intelligence</h1>
        <p className={styles.subtitle}>
          Watchlist and behavioral profiling based on AIS anomalies and oil spill associations.
        </p>
      </div>

      <div className={styles.layout}>
        {/* Left Column: Watchlist Table */}
        <div className={styles.leftCol}>
          <div className={`${styles.tableCard} card`}>
            <div className={styles.cardHeader}>
              <h3 className={styles.cardTitle}>High-Risk Watchlist</h3>
              <span className={styles.badge}>{vesselsData?.total || 0} Tracked</span>
            </div>
            
            <div className={styles.tableWrapper}>
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th>Vessel / MMSI</th>
                    <th>Type</th>
                    <th>Incidents</th>
                    <th>Risk Score</th>
                    <th>Last Seen</th>
                  </tr>
                </thead>
                <tbody>
                  {loadingList && <tr><td colSpan={5} className={styles.empty}>Loading watchlist…</td></tr>}
                  {!loadingList && vessels.length === 0 && <tr><td colSpan={5} className={styles.empty}>No vessels tracked.</td></tr>}
                  {vessels.map((v: any) => (
                    <tr 
                      key={v.vessel_id} 
                      className={selectedVesselId === v.vessel_id ? styles.rowActive : styles.row}
                      onClick={() => setSelectedVesselId(v.vessel_id)}
                    >
                      <td className={styles.monoCell}>{v.vessel_id}</td>
                      <td className={styles.typeCell}>{v.vessel_type}</td>
                      <td>
                        <div className={styles.pillContainer}>
                          <span className={styles.pillDefault}>{v.incident_count}</span>
                          {v.oil_spill_count > 0 && <span className={styles.pillDanger}>{v.oil_spill_count} Spills</span>}
                        </div>
                      </td>
                      <td>
                        <div className={styles.scoreCell}>
                          <div className={styles.scoreBar}>
                            <div 
                              className={styles.scoreFill} 
                              style={{ 
                                width: `${v.risk_score}%`,
                                background: v.risk_score > 70 ? 'var(--danger)' : v.risk_score > 40 ? 'var(--warning)' : 'var(--info)'
                              }}
                            />
                          </div>
                          <span className={styles.scoreVal}>{v.risk_score}</span>
                        </div>
                      </td>
                      <td className={styles.timeCell}>{v.last_seen ? new Date(v.last_seen).toLocaleDateString() : '--'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* Right Column: Detail Panel */}
        <div className={styles.rightCol}>
          {!selectedVesselId ? (
            <div className={`${styles.detailCard} card ${styles.emptyPanel}`}>
              <Ship size={48} className={styles.emptyIcon} />
              <h3>Select a vessel</h3>
              <p>Click on a vessel in the watchlist to view its intelligence profile.</p>
            </div>
          ) : loadingDetail ? (
            <div className={`${styles.detailCard} card ${styles.emptyPanel}`}>
              <p>Loading profile for {selectedVesselId}…</p>
            </div>
          ) : vesselDetail ? (
            <div className={`${styles.detailCard} card animate-enter`}>
              <div className={styles.detailHeader}>
                <div className={styles.detailTitleRow}>
                  <h2>Vessel {vesselDetail.vessel_id}</h2>
                  <div className={styles.riskBadge} style={{ 
                    background: vesselDetail.risk.score > 70 ? 'rgba(239, 68, 68, 0.15)' : 'rgba(245, 158, 11, 0.15)',
                    color: vesselDetail.risk.score > 70 ? 'var(--danger)' : 'var(--warning)'
                  }}>
                    <ShieldAlert size={14} /> Risk Score: {vesselDetail.risk.score}
                  </div>
                </div>
                <div className={styles.detailMetaRow}>
                  <span><Navigation size={12}/> Type: {vesselDetail.vessel_type}</span>
                  <span><Crosshair size={12}/> Last Pos: {vesselDetail.last_lat?.toFixed(2)}°, {vesselDetail.last_lon?.toFixed(2)}°</span>
                  <span><AlertTriangle size={12}/> Anomalies: {vesselDetail.incident_count}</span>
                </div>
              </div>

              {/* Risk Factors */}
              <div className={styles.section}>
                <h4 className={styles.sectionTitle}>Risk Factor Breakdown</h4>
                <div className={styles.factorList}>
                  {Object.entries(vesselDetail.risk.factors).map(([key, factor]: [string, any]) => (
                    <div key={key} className={styles.factorRow}>
                      <div className={styles.factorLabel}>
                        <span className={styles.factorName}>{key.replace(/_/g, ' ')}</span>
                        <span className={styles.factorDesc}>{factor.description}</span>
                      </div>
                      <div className={styles.factorBarWrap}>
                        <div className={styles.factorBar}>
                          <div 
                            className={styles.factorFill} 
                            style={{ 
                              width: `${(factor.contribution / (factor.weight * 100)) * 100}%`,
                              background: key === 'oil_spill_association' ? 'var(--danger)' : 'var(--accent-blue)'
                            }} 
                          />
                        </div>
                        <span className={styles.factorVal}>+{factor.contribution}</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Behavioral Profile (Speed/Heading Chart) */}
              <div className={styles.section}>
                <h4 className={styles.sectionTitle}>Behavioral Profile</h4>
                <div className={styles.behaviorBox}>
                  {loadingBehavior ? <p className={styles.textDim}>Loading behavior…</p> : (
                    <div className={styles.behaviorStats}>
                      <div className={styles.bStat}>
                        <span className={styles.bVal}>{vesselBehavior?.summary?.avg_speed || '--'} kn</span>
                        <span className={styles.bLabel}>Avg Speed</span>
                      </div>
                      <div className={styles.bStat}>
                        <span className={styles.bVal}>{vesselBehavior?.summary?.speed_variance || '--'}</span>
                        <span className={styles.bLabel}>Speed Variance</span>
                      </div>
                      <div className={styles.bStat}>
                        <span className={styles.bVal}>{vesselBehavior?.summary?.data_points || 0}</span>
                        <span className={styles.bLabel}>Event Points</span>
                      </div>
                    </div>
                  )}
                  {/* Miniature Speed Chart */}
                  {vesselBehavior?.speeds && vesselBehavior.speeds.length > 0 && (
                    <div className={styles.miniChart}>
                      {vesselBehavior.speeds.map((s: number, i: number) => (
                        <div 
                          key={i} 
                          className={styles.miniBar} 
                          style={{ height: `${(s / Math.max(15, ...vesselBehavior.speeds)) * 100}%` }}
                          title={`Speed: ${s} kn`}
                        />
                      ))}
                    </div>
                  )}
                  <p className={styles.textDim} style={{ fontSize: 10, marginTop: 4 }}>Speed profile across recorded anomalies</p>
                </div>
              </div>

              {/* Incident History Timeline */}
              <div className={styles.section}>
                <h4 className={styles.sectionTitle}>Incident History</h4>
                <div className={styles.timeline}>
                  {vesselDetail.incidents.map((inc: any, i: number) => (
                    <div key={i} className={styles.timelineItem}>
                      <div className={styles.timelineDot} style={{ background: inc.prediction === 'oil_spill' ? 'var(--danger)' : 'var(--accent-blue)' }} />
                      <div className={styles.timelineContent}>
                        <div className={styles.timelineHeader}>
                          <span className={styles.timelineDate}>{new Date(inc.timestamp).toLocaleString()}</span>
                          <span className={styles.timelineStatus}>{inc.status}</span>
                        </div>
                        <div className={styles.timelineMeta}>
                          {inc.prediction === 'oil_spill' && <span className={styles.pillDanger}>Spill Detected</span>}
                          <span>Score: {inc.anomaly_score?.toFixed(2) || '--'}</span>
                          <span className={styles.monoCell}>{inc.incident_id.split('-')[1] || inc.incident_id}</span>
                        </div>
                        {inc.sar_image && (
                          <div className={styles.timelineImage}>
                            <img src={getSarImageUrl(inc.sar_image.split('/').pop()!)} alt="SAR Thumbnail" />
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

            </div>
          ) : (
            <div className={`${styles.detailCard} card ${styles.emptyPanel}`}>
              <p>Vessel data not found.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
