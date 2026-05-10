'use client';

import { useState } from 'react';
import styles from './analytics.module.css';
import {
  useAnalyticsTrends,
  useAnalyticsPeakHours,
  useAnalyticsRegionalDensity,
  useAnalyticsConfidenceDistribution,
  useAnalyticsOperationalKPIs,
  useAnalyticsIncidentLifecycle
} from '@/lib/queries';
import { Activity, Clock, ShieldAlert, Navigation, Target, ActivitySquare } from 'lucide-react';

export default function Analytics() {
  const [trendPeriod, setTrendPeriod] = useState<'weekly' | 'monthly'>('weekly');

  const { data: trends, isLoading: loadingTrends } = useAnalyticsTrends(trendPeriod, 12);
  const { data: peakHours, isLoading: loadingPeak } = useAnalyticsPeakHours();
  const { data: regions, isLoading: loadingRegions } = useAnalyticsRegionalDensity();
  const { data: confidence, isLoading: loadingConfidence } = useAnalyticsConfidenceDistribution();
  const { data: kpis, isLoading: loadingKPIs } = useAnalyticsOperationalKPIs();
  const { data: lifecycle, isLoading: loadingLifecycle } = useAnalyticsIncidentLifecycle();

  const maxTrend = trends?.counts ? Math.max(...trends.counts, 1) : 1;
  const maxPeak = peakHours?.counts ? Math.max(...peakHours.counts, 1) : 1;
  const maxConf = confidence?.counts ? Math.max(...confidence.counts, 1) : 1;

  return (
    <div className={`${styles.page} animate-enter`}>
      <div className={styles.pageHeader}>
        <h1>Operational Intelligence</h1>
        <p className={styles.subtitle}>
          Advanced analytics and environmental monitoring metrics.
        </p>
      </div>

      {/* KPI Row */}
      <div className={styles.kpiGrid}>
        <div className={`${styles.kpiCard} card`}>
          <div className={styles.kpiHeader}>
            <ShieldAlert size={16} className={styles.kpiIconAlert} />
            <span className={styles.kpiTitle}>Active Incidents</span>
          </div>
          <div className={styles.kpiValue}>{loadingKPIs ? '--' : kpis?.active_incidents}</div>
          <div className={styles.kpiSub}>Requiring investigation</div>
        </div>
        <div className={`${styles.kpiCard} card`}>
          <div className={styles.kpiHeader}>
            <Target size={16} className={styles.kpiIconSuccess} />
            <span className={styles.kpiTitle}>Confirmed Spills</span>
          </div>
          <div className={styles.kpiValue}>{loadingKPIs ? '--' : kpis?.confirmed_spills}</div>
          <div className={styles.kpiSub}>Total verified detections</div>
        </div>
        <div className={`${styles.kpiCard} card`}>
          <div className={styles.kpiHeader}>
            <ActivitySquare size={16} className={styles.kpiIconWarning} />
            <span className={styles.kpiTitle}>False Positive Rate</span>
          </div>
          <div className={styles.kpiValue}>{loadingKPIs ? '--' : `${kpis?.false_positive_rate}%`}</div>
          <div className={styles.kpiSub}>{kpis?.false_positives || 0} false alarms</div>
        </div>
        <div className={`${styles.kpiCard} card`}>
          <div className={styles.kpiHeader}>
            <Clock size={16} className={styles.kpiIconInfo} />
            <span className={styles.kpiTitle}>Detection Latency</span>
          </div>
          <div className={styles.kpiValue}>{loadingKPIs ? '--' : `${kpis?.avg_detection_latency_sec}s`}</div>
          <div className={styles.kpiSub}>Average anomaly-to-alert time</div>
        </div>
      </div>

      <div className={styles.grid}>
        {/* Detection Trends */}
        <div className={`${styles.chartSection} card`}>
          <div className={styles.chartHeader}>
            <h3 className={styles.cardTitle}>Detection Trends</h3>
            <div className={styles.toggleGroup}>
              <button
                className={`${styles.toggleBtn} ${trendPeriod === 'weekly' ? styles.toggleActive : ''}`}
                onClick={() => setTrendPeriod('weekly')}
              >Weekly</button>
              <button
                className={`${styles.toggleBtn} ${trendPeriod === 'monthly' ? styles.toggleActive : ''}`}
                onClick={() => setTrendPeriod('monthly')}
              >Monthly</button>
            </div>
          </div>
          {loadingTrends ? <p className={styles.loading}>Loading trends…</p> : (
            <div className={styles.trendChart}>
              {trends?.counts?.map((count: number, i: number) => (
                <div key={i} className={styles.trendCol}>
                  <div className={styles.trendBar} style={{ height: `${(count / maxTrend) * 100}%` }}>
                    <div className={styles.trendTooltip}>{count}</div>
                  </div>
                  <span className={styles.trendLabel}>{trends.labels[i].replace('2026-', '')}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Peak Detection Hours */}
        <div className={`${styles.chartSection} card`}>
          <div className={styles.chartHeader}>
            <h3 className={styles.cardTitle}>Peak Detection Hours</h3>
            <span className={styles.chartMeta}>UTC Time</span>
          </div>
          {loadingPeak ? <p className={styles.loading}>Loading peak hours…</p> : (
            <div className={styles.peakChart}>
              {peakHours?.counts?.map((count: number, i: number) => (
                <div key={i} className={styles.peakCol}>
                  <div
                    className={`${styles.peakBar} ${i === peakHours.peak_hour ? styles.peakBarMax : ''}`}
                    style={{ height: `${(count / maxPeak) * 100}%` }}
                  />
                  {i % 4 === 0 && <span className={styles.peakLabel}>{i}h</span>}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Regional Density */}
        <div className={`${styles.chartSection} card`}>
          <div className={styles.chartHeader}>
            <h3 className={styles.cardTitle}>Regional Density</h3>
            <Navigation size={16} className={styles.textMuted} />
          </div>
          {loadingRegions ? <p className={styles.loading}>Loading regions…</p> : (
            <div className={styles.barList}>
              {regions?.regions?.map((r: any) => (
                <div key={r.region} className={styles.barRow}>
                  <div className={styles.barLabel}>{r.region}</div>
                  <div className={styles.barTrack}>
                    <div className={styles.barFillBlue} style={{ width: `${r.percentage}%` }} />
                  </div>
                  <div className={styles.barValue}>{r.count}</div>
                </div>
              ))}
              {(!regions?.regions || regions.regions.length === 0) && <p className={styles.empty}>No regional data.</p>}
            </div>
          )}
        </div>

        {/* Confidence Distribution */}
        <div className={`${styles.chartSection} card`}>
          <div className={styles.chartHeader}>
            <h3 className={styles.cardTitle}>Model Confidence</h3>
            <Activity size={16} className={styles.textMuted} />
          </div>
          {loadingConfidence ? <p className={styles.loading}>Loading confidence…</p> : (
            <div className={styles.barList}>
              {confidence?.labels?.map((label: string, i: number) => {
                const count = confidence.counts[i];
                return (
                  <div key={label} className={styles.barRow}>
                    <div className={styles.barLabel}>{label}</div>
                    <div className={styles.barTrack}>
                      <div className={styles.barFillGreen} style={{ width: `${(count / maxConf) * 100}%` }} />
                    </div>
                    <div className={styles.barValue}>{count}</div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Incident Lifecycle (Full Width) */}
        <div className={`${styles.chartSection} card`} style={{ gridColumn: '1 / -1' }}>
          <div className={styles.chartHeader}>
            <h3 className={styles.cardTitle}>Incident Lifecycle Pipeline</h3>
            <span className={styles.chartMeta}>Status flow from detection to resolution</span>
          </div>
          {loadingLifecycle ? <p className={styles.loading}>Loading lifecycle…</p> : (
            <div className={styles.lifecycleFlow}>
              {lifecycle?.stages?.filter((s: any) => s.count > 0).map((stage: any, i: number, arr: any[]) => (
                <div key={stage.stage} className={styles.lifecycleNodeWrap}>
                  <div className={`${styles.lifecycleNode} ${stage.stage === 'confirmed' || stage.stage === 'resolved' ? styles.lifecycleVerified : stage.stage === 'false_positive' ? styles.lifecycleFalse : ''}`}>
                    <span className={styles.lifecycleCount}>{stage.count}</span>
                    <span className={styles.lifecycleLabel}>{stage.label}</span>
                  </div>
                  {i < arr.length - 1 && <div className={styles.lifecycleLine} />}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
