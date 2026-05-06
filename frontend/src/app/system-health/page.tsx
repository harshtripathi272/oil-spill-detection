'use client';

import { useEffect, useState } from 'react';
import { 
  CheckCircle2, AlertTriangle, Monitor, HardDrive, Cpu, Server, 
  Activity, Zap, Clock, ShieldCheck, Download, RefreshCw 
} from 'lucide-react';
import styles from './system-health.module.css';
import { fetchSystemHealth, fetchSystemResources } from '@/lib/api';

export default function SystemTelemetry() {
  const [health, setHealth] = useState<any>(null);
  const [resources, setResources] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const [healthData, resourceData] = await Promise.all([
          fetchSystemHealth(),
          fetchSystemResources(),
        ]);
        setHealth(healthData);
        setResources(resourceData);
      } catch (err) {
        console.error("Failed to load telemetry:", err);
      } finally {
        setLoading(false);
      }
    }
    load();
    const int = setInterval(load, 30000);
    return () => clearInterval(int);
  }, []);

  const cpuUsage = resources?.cpu?.usage_percent ?? 42;
  const memUsage = resources?.memory ? ((resources.memory.used / resources.memory.total) * 100).toFixed(0) : 78;
  const diskPerc = resources?.disk?.percent ?? 68;

  const backendServices = [
    { name: 'FastAPI Gateway', status: 'Healthy', uptime: '14d 02h 45m', cpu: '12%', mem: '1.2GB', metricLabel: 'Req/s', metricVal: '4,521', icon: Activity },
    { name: 'AIS Ingestion Node', status: 'Healthy', uptime: '05d 11h 20m', cpu: '28%', mem: '4.5GB', metricLabel: 'Msgs/s', metricVal: '12,050', icon: Zap },
    { name: 'Kafka Stream Processor', status: 'Lagging', uptime: '30d 14h 10m', cpu: '85%', mem: '12.0GB', metricLabel: 'Offset Lag', metricVal: '45k', icon: Server, warning: true },
    { name: 'Anomaly Detector (ML)', status: 'Healthy', uptime: '02d 08h 05m', cpu: '65%', mem: '8.2GB', metricLabel: 'Inf Time', metricVal: '45ms', icon: Cpu },
    { name: 'Airflow Scheduler', status: 'Healthy', uptime: '14d 02h 45m', cpu: '05%', mem: '0.8GB', metricLabel: 'Active DAGs', metricVal: '12', icon: Clock },
  ];

  return (
    <div className={`${styles.page} animate-enter`}>
      {/* Header */}
      <div className={styles.telemetryHeader}>
        <div className={styles.headerLeft}>
          <Monitor size={20} className={styles.titleIcon} />
          <div>
            <h1>System Telemetry</h1>
            <p className={styles.subtitle}>Real-time infrastructure monitoring and service status.</p>
          </div>
        </div>
        <div className={styles.liveIndicator}>LIVE WEBSOCKET</div>
      </div>

      {/* Top 4 Panels */}
      <div className={styles.topPanels}>
        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>System Status</span>
            <CheckCircle2 size={16} className={styles.iconHealthy} />
          </div>
          <div className={styles.panelStatus}>OPERATIONAL</div>
          <div className={styles.panelSub}><span>Uptime:</span> 99.98%</div>
          <div className={styles.panelMetric}>
            <span>CPU UTILIZATION</span>
            <span className={styles.metricVal}>{cpuUsage}%</span>
          </div>
        </div>

        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>Database Cluster</span>
            <CheckCircle2 size={16} className={styles.iconHealthy} />
          </div>
          <div className={styles.panelStatus}>HEALTHY</div>
          <div className={styles.panelSub}><span>Replica Lag:</span> 4ms</div>
          <div className={styles.panelMetric}>
            <span>MEMORY (RAM)</span>
            <span className={styles.metricVal}>{memUsage}%</span>
          </div>
        </div>

        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>Microservices</span>
            <AlertTriangle size={16} className={styles.iconWarning} />
          </div>
          <div className={styles.panelStatus}>11/12 ONLINE</div>
          <div className={styles.panelSub}><span>Avg Response:</span> 42ms</div>
          <div className={styles.panelMetric}>
            <span>DISK I/O</span>
            <span className={styles.metricVal}>1.2 GB/s</span>
          </div>
        </div>

        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>Storage Array</span>
            <CheckCircle2 size={16} className={styles.iconHealthy} />
          </div>
          <div className={styles.panelStatus}>HEALTHY</div>
          <div className={styles.panelSub}><span>Capacity:</span> {diskPerc}%</div>
          <div className={styles.panelMetric}>
            <span>NETWORK TX/RX</span>
            <span className={styles.metricVal}>4.5 Gbps</span>
          </div>
        </div>
      </div>

      {/* Backend Services Table */}
      <div className={`${styles.card} card`}>
        <div className={styles.cardHeader}>
          <h3>Backend Services</h3>
          <button className={styles.exportBtn}>Export Metrics</button>
        </div>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>SERVICE</th>
              <th>STATUS</th>
              <th>UPTIME</th>
              <th>CPU / MEM</th>
              <th>KEY METRIC</th>
              <th>ACTION</th>
            </tr>
          </thead>
          <tbody>
            {backendServices.map((svc) => {
              const Icon = svc.icon;
              return (
                <tr key={svc.name}>
                  <td>
                    <div className={styles.svcName}>
                      <Icon size={14} className={styles.svcIcon} />
                      {svc.name}
                    </div>
                  </td>
                  <td>
                    <span className={`${styles.chip} ${svc.warning ? styles.chipWarning : styles.chipHealthy}`}>
                      {svc.status}
                    </span>
                  </td>
                  <td className={styles.mono}>{svc.uptime}</td>
                  <td className={styles.mono}>{svc.cpu} / {svc.mem}</td>
                  <td className={styles.mono}>{svc.metricLabel}: {svc.metricVal}</td>
                  <td>
                    <button className={styles.actionBtn}>Restart</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Bottom Layout */}
      <div className={styles.bottomLayout}>
        <div className={`${styles.eventsCard} card`}>
          <div className={styles.cardHeader}>
            <h3>Recent System Events</h3>
          </div>
          <div className={styles.logs}>
            <div className={styles.logLine}>
              <span className={styles.logTime}>14:32:05 UTC</span>
              <span className={styles.logWarn}>[WARN]</span>
              <span>Kafka consumer lag threshold exceeded on topic: ais_raw_stream</span>
            </div>
            <div className={styles.logLine}>
              <span className={styles.logTime}>14:15:00 UTC</span>
              <span className={styles.logInfo}>[INFO]</span>
              <span>Scheduled snapshot backup completed successfully. Size: 4.2TB</span>
            </div>
            <div className={styles.logLine}>
              <span className={styles.logTime}>13:45:22 UTC</span>
              <span className={styles.logInfo}>[INFO]</span>
              <span>New ML model weights deployed to Anomaly Detector nodes.</span>
            </div>
            <div className={styles.logLine}>
              <span className={styles.logTime}>12:10:05 UTC</span>
              <span className={styles.logErr}>[ERROR]</span>
              <span>Database connection timeout on replica-03. Auto-recovering.</span>
            </div>
          </div>
        </div>

        <div className={`${styles.toolsCard} card`}>
          <div className={styles.cardHeader}>
            <h3>Maintenance Tools</h3>
          </div>
          <div className={styles.toolsList}>
            <button className={styles.toolBtn}>
              <ShieldCheck size={16} />
              Trigger Backup
            </button>
            <button className={styles.toolBtn}>
              <RefreshCw size={16} />
              Cluster Restart
            </button>
            <button className={styles.toolBtn}>
              <Download size={16} />
              Download Full Logs
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
