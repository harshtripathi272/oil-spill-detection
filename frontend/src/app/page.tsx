import { 
  AlertCircle,
  TrendingDown, 
  TrendingUp, 
  CheckCircle,
  Activity,
  AlertTriangle,
  Clock,
  Target
} from 'lucide-react';
import styles from './page.module.css';

export default function Dashboard() {
  return (
    <div className={`${styles.dashboard} animate-enter`}>
      <header className={styles.header}>
        <h1 className="text-gradient">Dashboard Overview</h1>
        <p className={styles.subtitle}>Command Center operational status and real-time metrics.</p>
      </header>

      {/* KPI Cards */}
      <div className={styles.kpiGrid}>
        <div className="glass-panel">
          <div className={styles.kpiCard}>
            <div className={styles.kpiHeader}>
              <span className={styles.kpiTitle}>Total Incidents</span>
              <Activity className={styles.kpiIcon} size={20} />
            </div>
            <div className={styles.kpiValue}>847</div>
            <div className={`${styles.kpiTrend} ${styles.trendUp}`}>
              <TrendingUp size={16} />
              <span>12% MoM</span>
            </div>
          </div>
        </div>
        
        <div className="glass-panel">
          <div className={styles.kpiCard}>
            <div className={styles.kpiHeader}>
              <span className={styles.kpiTitle}>Active Incidents</span>
              <AlertCircle className={styles.kpiIcon} style={{color: 'var(--warning-orange)'}} size={20} />
            </div>
            <div className={styles.kpiValue}>12</div>
            <div className={`${styles.kpiTrend} ${styles.trendDown}`}>
              <TrendingDown size={16} />
              <span>3% WoW</span>
            </div>
          </div>
        </div>

        <div className="glass-panel">
          <div className={styles.kpiCard}>
            <div className={styles.kpiHeader}>
              <span className={styles.kpiTitle}>Success Rate</span>
              <CheckCircle className={styles.kpiIcon} style={{color: 'var(--success-green)'}} size={20} />
            </div>
            <div className={styles.kpiValue}>98.2%</div>
            <div className={`${styles.kpiTrend} ${styles.trendNeutral}`}>
              <CheckCircle size={16} />
              <span>Healthy</span>
            </div>
          </div>
        </div>

        <div className="glass-panel">
          <div className={styles.kpiCard}>
            <div className={styles.kpiHeader}>
              <span className={styles.kpiTitle}>Avg Conf. Score</span>
              <Target className={styles.kpiIcon} size={20} />
            </div>
            <div className={styles.kpiValue}>0.87</div>
            <div className={`${styles.kpiTrend} ${styles.trendUp}`}>
              <TrendingUp size={16} />
              <span>5% Trend</span>
            </div>
          </div>
        </div>
      </div>

      <div className={styles.mainGrid}>
        {/* Active Alerts List */}
        <div className={`${styles.alertsSection} glass-panel`}>
          <div className={styles.sectionHeader}>
            <h2 className={styles.sectionTitle}>⚠️ Active Alerts (2)</h2>
          </div>
          <div className={styles.alertList}>
            <div className={`${styles.alertItem} ${styles.alertCritical}`}>
              <div className={styles.alertIconWrapper}>
                <AlertCircle size={20} />
              </div>
              <div className={styles.alertContent}>
                <div className={styles.alertTitle}>HIGH: Incident #INC-20260505-001 | 0.92 conf</div>
                <div className={styles.alertDesc}>Location: 25.8°N, 80.1°W | Status: PENDING_IMAGERY</div>
                <div className={styles.alertTime}>2 hours ago</div>
              </div>
            </div>

            <div className={`${styles.alertItem} ${styles.alertWarning}`}>
              <div className={styles.alertIconWrapper}>
                <AlertTriangle size={20} />
              </div>
              <div className={styles.alertContent}>
                <div className={styles.alertTitle}>MEDIUM: Incident #INC-20260505-002 | 0.74 conf</div>
                <div className={styles.alertDesc}>Location: 26.2°N, 79.5°W | Status: DETECTED</div>
                <div className={styles.alertTime}>4 hours ago</div>
              </div>
            </div>
          </div>
        </div>

        {/* Charts Grid */}
        <div className={styles.chartsGrid}>
          <div className={`${styles.chartCard} glass-panel`}>
            <h3 className={styles.chartTitle}>Incidents Over Time (30d)</h3>
            <div className={styles.chartPlaceholder}>
              <div className={styles.barContainer}>
                {[4,7,5,8,12,15,22,18,14,9,11,16,19,25,20,15,10,12].map((h, i) => (
                  <div key={i} className={styles.bar} style={{ height: `${h * 4}px` }}></div>
                ))}
              </div>
            </div>
          </div>
          
          <div className={`${styles.chartCard} glass-panel`}>
            <h3 className={styles.chartTitle}>Status Distribution</h3>
            <div className={styles.chartPlaceholder}>
              <div className={styles.pieChart}></div>
              <div className={styles.pieLegend}>
                <div className={styles.legendItem}><span style={{background: 'var(--status-detected)'}}></span> Detected 45%</div>
                <div className={styles.legendItem}><span style={{background: 'var(--status-confirmed)'}}></span> Confirmed 35%</div>
                <div className={styles.legendItem}><span style={{background: 'var(--status-false-pos)'}}></span> False Pos 15%</div>
                <div className={styles.legendItem}><span style={{background: 'var(--status-resolved)'}}></span> Resolved 5%</div>
              </div>
            </div>
          </div>
        </div>
        
        {/* Map / Regions Layout */}
        <div className={styles.bottomGrid}>
          <div className={`${styles.regionsCard} glass-panel`}>
            <h3 className={styles.chartTitle}>🌍 Geographic Distribution</h3>
            <div className={styles.regionsList}>
              <div className={styles.regionItem}>
                <div className={styles.regionInfo}><span>North Atlantic</span><span>245 (29%)</span></div>
                <div className={styles.progressBar}><div style={{width: '60%'}}></div></div>
              </div>
              <div className={styles.regionItem}>
                <div className={styles.regionInfo}><span>South Atlantic</span><span>183 (22%)</span></div>
                <div className={styles.progressBar}><div style={{width: '45%'}}></div></div>
              </div>
              <div className={styles.regionItem}>
                <div className={styles.regionInfo}><span>Pacific Ocean</span><span>156 (18%)</span></div>
                <div className={styles.progressBar}><div style={{width: '35%'}}></div></div>
              </div>
              <div className={styles.regionItem}>
                <div className={styles.regionInfo}><span>Mediterranean</span><span>142 (17%)</span></div>
                <div className={styles.progressBar}><div style={{width: '30%'}}></div></div>
              </div>
            </div>
          </div>
          
          <div className={`${styles.tableCard} glass-panel`}>
            <div className={styles.sectionHeader}>
              <h3 className={styles.chartTitle}>📊 Recent Incidents</h3>
              <a href="/incidents" className={styles.viewAll}>View All</a>
            </div>
            <table className={styles.incidentsTable}>
              <thead>
                <tr>
                  <th>Incident</th>
                  <th>Conf.</th>
                  <th>Location</th>
                  <th>Time </th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className={styles.incidentId}>INC-001</td>
                  <td><div className={styles.confHigh}>0.92</div></td>
                  <td className={styles.locationCell}>25.8,-80.1</td>
                  <td>2h ago</td>
                  <td><span className="status-badge detected">PENDING</span></td>
                </tr>
                <tr>
                  <td className={styles.incidentId}>INC-002</td>
                  <td><div className={styles.confMed}>0.74</div></td>
                  <td className={styles.locationCell}>26.2,-79.5</td>
                  <td>4h ago</td>
                  <td><span className="status-badge detected">DETECTED</span></td>
                </tr>
                <tr>
                  <td className={styles.incidentId}>INC-003</td>
                  <td><div className={styles.confMed}>0.68</div></td>
                  <td className={styles.locationCell}>24.9,-81.0</td>
                  <td>6h ago</td>
                  <td><span className="status-badge confirmed">CONFIRMED</span></td>
                </tr>
                <tr>
                  <td className={styles.incidentId}>INC-004</td>
                  <td><div className={styles.confLow}>0.45</div></td>
                  <td className={styles.locationCell}>27.1,-78.9</td>
                  <td>12h ago</td>
                  <td><span className="status-badge false-pos">FALSE POS</span></td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

      </div>
    </div>
  );
}
