'use client';

import { useEffect, useState } from 'react';
import { 
  CheckCircle2, AlertTriangle, Monitor, HardDrive, Cpu, Server, 
  Activity, Zap, Clock, Download, RefreshCw 
} from 'lucide-react';
import styles from './system-health.module.css';
import { fetchSystemHealth, fetchSystemResources, fetchServicesLive, fetchLogFileContent } from '@/lib/api';

export default function SystemTelemetry() {
  const [health, setHealth] = useState<any>(null);
  const [resources, setResources] = useState<any>(null);
  const [services, setServices] = useState<any[]>([]);
  const [recentEvents, setRecentEvents] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);

  const load = async () => {
    setLoading(true);
    try {
      const [healthData, resourceData, svcData, logsData] = await Promise.all([
        fetchSystemHealth(),
        fetchSystemResources(),
        fetchServicesLive().catch(() => ({ services: [] })),
        fetchLogFileContent('anomaly_detector.log', 15).catch(() => ({ content: '' })),
      ]);
      setHealth(healthData);
      setResources(resourceData);
      setServices(svcData.services || []);
      const lines = (logsData.content || '').split('\n').filter((l: string) => l.trim()).slice(-10);
      setRecentEvents(lines);
    } catch (err) {
      console.error("Failed to load telemetry:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const int = setInterval(load, 30000);
    return () => clearInterval(int);
  }, []);

  const cpuUsage = resources?.cpu?.usage_percent ?? 0;
  const memUsage = resources?.memory ? ((resources.memory.used / resources.memory.total) * 100) : 0;
  const diskPerc = resources?.disk?.percent ?? 0;
  const netSent = resources?.network?.bytes_sent ?? 0;
  const netRecv = resources?.network?.bytes_recv ?? 0;

  const formatBytes = (b: number) => {
    if (b === 0) return '0 B';
    const i = Math.floor(Math.log(b) / Math.log(1024));
    return (b / Math.pow(1024, i)).toFixed(1) + ' ' + ['B', 'KB', 'MB', 'GB', 'TB'][i];
  };

  const formatUptime = (seconds: number) => {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${days}d ${hours}h ${minutes}m`;
  };

  const healthyCount = health?.components?.filter((c: any) => c.status === 'healthy').length ?? 0;
  const totalCount = health?.components?.length ?? 0;

  const svcIcons: Record<string, any> = {
    'FastAPI Backend': Activity,
    'Anomaly Detector': Cpu,
    'Stream Processor': Zap,
    'Trigger Bridge': Server,
    'Kafka Broker': Zap,
    'Airflow Scheduler': Clock,
  };

  const parseLogLevel = (line: string) => {
    if (line.includes('[ANOMALY DETECTED]') || line.includes('🚨')) return 'warn';
    if (line.includes('ERROR')) return 'error';
    return 'info';
  };

  const handleDownloadLogs = () => {
    const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000/api/v1';
    window.open(`${API_BASE}/logs/download/anomaly_detector.log`, '_blank');
  };

  const handleExportMetrics = () => {
    if (!resources) return;
    const blob = new Blob([JSON.stringify(resources, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'system_metrics.json';
    a.click();
    URL.revokeObjectURL(url);
  };

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
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          {loading && <RefreshCw size={16} className="animate-spin" style={{ color: 'var(--accent-primary)' }} />}
          <button onClick={load} className={styles.liveIndicator} style={{ cursor: 'pointer', border: 'none' }}>
            REFRESH
          </button>
        </div>
      </div>

      {/* Top 4 Panels — Real Data */}
      <div className={styles.topPanels}>
        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>System Status</span>
            {health?.overall_status === 'healthy'
              ? <CheckCircle2 size={16} className={styles.iconHealthy} />
              : <AlertTriangle size={16} className={styles.iconWarning} />}
          </div>
          <div className={styles.panelStatus}>
            {health?.overall_status?.toUpperCase() || 'LOADING'}
          </div>
          <div className={styles.panelSub}>
            <span>Uptime:</span> {health?.uptime ? formatUptime(health.uptime) : '--'}
          </div>
          <div className={styles.panelMetric}>
            <span>CPU UTILIZATION</span>
            <span className={styles.metricVal}>{cpuUsage.toFixed(1)}%</span>
          </div>
        </div>

        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>Database</span>
            {health?.components?.find((c: any) => c.component === 'database')?.status === 'healthy'
              ? <CheckCircle2 size={16} className={styles.iconHealthy} />
              : <AlertTriangle size={16} className={styles.iconWarning} />}
          </div>
          <div className={styles.panelStatus}>
            {health?.components?.find((c: any) => c.component === 'database')?.status?.toUpperCase() || '--'}
          </div>
          <div className={styles.panelSub}>
            <span>Type:</span> {health?.components?.find((c: any) => c.component === 'database')?.details?.connection || 'sqlite'}
          </div>
          <div className={styles.panelMetric}>
            <span>MEMORY (RAM)</span>
            <span className={styles.metricVal}>{memUsage.toFixed(1)}%</span>
          </div>
        </div>

        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>Services</span>
            {healthyCount < totalCount
              ? <AlertTriangle size={16} className={styles.iconWarning} />
              : <CheckCircle2 size={16} className={styles.iconHealthy} />}
          </div>
          <div className={styles.panelStatus}>{healthyCount}/{totalCount} HEALTHY</div>
          <div className={styles.panelSub}>
            <span>Checked:</span> {health?.last_updated ? new Date(health.last_updated).toLocaleTimeString() : '--'}
          </div>
          <div className={styles.panelMetric}>
            <span>DISK USAGE</span>
            <span className={styles.metricVal}>{diskPerc.toFixed(1)}%</span>
          </div>
        </div>

        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <span>Network</span>
            <CheckCircle2 size={16} className={styles.iconHealthy} />
          </div>
          <div className={styles.panelStatus}>ACTIVE</div>
          <div className={styles.panelSub}>
            <span>TX:</span> {formatBytes(netSent)}
          </div>
          <div className={styles.panelMetric}>
            <span>RX</span>
            <span className={styles.metricVal}>{formatBytes(netRecv)}</span>
          </div>
        </div>
      </div>

      {/* Backend Services Table — Real Data */}
      <div className={`${styles.card} card`}>
        <div className={styles.cardHeader}>
          <h3>Backend Services</h3>
          <button className={styles.exportBtn} onClick={handleExportMetrics}>Export Metrics</button>
        </div>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>SERVICE</th>
              <th>STATUS</th>
              <th>UPTIME</th>
              <th>CPU / MEM</th>
              <th>ACTION</th>
            </tr>
          </thead>
          <tbody>
            {services.length === 0 ? (
              <tr><td colSpan={5} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '1rem' }}>
                {loading ? 'Loading services…' : 'No services detected'}
              </td></tr>
            ) : services.map((svc) => {
              const Icon = svcIcons[svc.name] || Server;
              return (
                <tr key={svc.name}>
                  <td>
                    <div className={styles.svcName}>
                      <Icon size={14} className={styles.svcIcon} />
                      {svc.name}
                    </div>
                  </td>
                  <td>
                    <span className={`${styles.chip} ${svc.running ? styles.chipHealthy : styles.chipWarning}`}>
                      {svc.status}
                    </span>
                  </td>
                  <td className={styles.mono}>{svc.uptime || '--'}</td>
                  <td className={styles.mono}>{svc.cpu_percent}% / {svc.memory_mb} MB</td>
                  <td>
                    <button className={styles.actionBtn} onClick={load} title="Refresh status">
                      <RefreshCw size={12} /> Check
                    </button>
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
            {recentEvents.length === 0 ? (
              <div className={styles.logLine}>
                <span style={{ color: 'var(--text-muted)' }}>No recent events</span>
              </div>
            ) : recentEvents.map((line, i) => {
              const level = parseLogLevel(line);
              const ts = line.match(/^(\d{2}:\d{2}:\d{2})/)?.[1] || 
                         line.match(/(\d{2}:\d{2}:\d{2})/)?.[1] || '';
              const msg = line.replace(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ - \S+ - \w+ - /, '');
              return (
                <div key={i} className={styles.logLine}>
                  <span className={styles.logTime}>{ts}</span>
                  <span className={level === 'error' ? styles.logErr : level === 'warn' ? styles.logWarn : styles.logInfo}>
                    [{level.toUpperCase()}]
                  </span>
                  <span>{msg.substring(0, 120)}</span>
                </div>
              );
            })}
          </div>
        </div>

        <div className={`${styles.toolsCard} card`}>
          <div className={styles.cardHeader}>
            <h3>Quick Actions</h3>
          </div>
          <div className={styles.toolsList}>
            <button className={styles.toolBtn} onClick={load}>
              <RefreshCw size={16} />
              Force Refresh
            </button>
            <button className={styles.toolBtn} onClick={handleDownloadLogs}>
              <Download size={16} />
              Download Logs
            </button>
            <button className={styles.toolBtn} onClick={handleExportMetrics}>
              <HardDrive size={16} />
              Export System Metrics
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
