'use client';

import { useEffect, useState } from 'react';
import { Activity, Database, Zap, Server, FileText, CheckCircle, AlertTriangle, XCircle, Search, RefreshCw, Cpu, HardDrive, MemoryStick } from 'lucide-react';
import styles from './SystemHealthPanel.module.css';
import { fetchSystemHealth, fetchSystemResources, fetchLogFiles, fetchLogFileContent } from '@/lib/api';
import { formatBytes } from '@/lib/utils';

/* --- Types --- */
interface ComponentStatus {
  component: string;
  status: 'healthy' | 'warning' | 'error';
  last_check: string;
  details: any;
}

interface SystemHealthData {
  overall_status: 'healthy' | 'warning' | 'error';
  components: ComponentStatus[];
  uptime: number;
  last_updated: string;
}

/* --- Component --- */
export default function SystemHealthPanel() {
  const [health, setHealth] = useState<SystemHealthData | null>(null);
  const [resources, setResources] = useState<any>(null);
  const [logFiles, setLogFiles] = useState<any[]>([]);
  const [selectedLog, setSelectedLog] = useState<string | null>(null);
  const [logContent, setLogContent] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [logSearch, setLogSearch] = useState('');

  const loadData = async () => {
    setLoading(true);
    try {
      const [healthData, resourceData, logFilesData] = await Promise.all([
        fetchSystemHealth(),
        fetchSystemResources(),
        fetchLogFiles(),
      ]);
      setHealth(healthData);
      setResources(resourceData);
      setLogFiles(logFilesData);
      setError(null);
    } catch (err) {
      console.error('Failed to load system data:', err);
      setError('Failed to sync with backend services. Please check connection.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, []);

  const loadLogContent = async (filename: string) => {
    try {
      const data = await fetchLogFileContent(filename, 100, logSearch || undefined);
      setSelectedLog(filename);
      setLogContent(data.content);
    } catch (err) {
      setError(`Failed to read log file ${filename}`);
    }
  };

  /* --- Helpers --- */
  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'healthy': return <CheckCircle size={18} className={styles.statusHealthy} />;
      case 'warning': return <AlertTriangle size={18} className={styles.statusWarning} />;
      case 'error': return <XCircle size={18} className={styles.statusError} />;
      default: return <Activity size={18} />;
    }
  };

  const getComponentIcon = (component: string) => {
    switch (component) {
      case 'database': return <Database size={16} />;
      case 'cpu': return <Cpu size={16} />;
      case 'memory': return <MemoryStick size={16} />;
      case 'disk': return <HardDrive size={16} />;
      case 'kafka': return <Zap size={16} />;
      case 'airflow': return <Activity size={16} />;
      case 'api_server': return <Server size={16} />;
      default: return <Activity size={16} />;
    }
  };

  return (
    <div className={styles.page}>
      {/* Header */}
      <div className={styles.pageHeader}>
        <div>
          <h1>System Telemetry &amp; Administration</h1>
          <p className={styles.subtitle}>Real-time infrastructure monitoring and system configuration.</p>
        </div>
        <div className={styles.headerActions}>
          {loading && <RefreshCw size={16} className="animate-spin" style={{ color: 'var(--accent-primary)' }} />}
          <button onClick={loadData} className={styles.btnSecondary}>Force Sync</button>
        </div>
      </div>

      <div className={styles.dashboardGrid}>
        {/* Left Column: Health & Logs */}
        <div className={styles.mainCol}>
          {/* System Health Overview */}
          <div className={`${styles.panel} card`}>
            <div className={styles.panelHeader}>
              <Activity size={20} />
              <h3>Service Infrastructure Health</h3>
              {health && (
                <div className={`${styles.statusPill} ${styles[health.overall_status]}`}>
                  {health.overall_status.toUpperCase()}
                </div>
              )}
            </div>

            <div className={styles.metricsGrid}>
              {health?.components.map((c) => (
                <div key={c.component} className={styles.metricItem}>
                  <div className={styles.metricLabel}>
                    {getComponentIcon(c.component)}
                    <span>{c.component.replace('_', ' ').toUpperCase()}</span>
                  </div>
                  <div className={styles.metricStatus}>
                    {getStatusIcon(c.status)}
                    <span>{new Date(c.last_check).toLocaleTimeString()}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Log Explorer */}
          <div className={`${styles.panel} card`}>
            <div className={styles.panelHeader}>
              <FileText size={20} />
              <h3>System Log Files</h3>
              <div className={styles.headerControl}>
                <Search size={14} />
                <input
                  type="text"
                  placeholder="Filter logs..."
                  className={styles.smallInput}
                  value={logSearch}
                  onChange={(e) => setLogSearch(e.target.value)}
                  onKeyDown={(e) => { if (e.key === 'Enter' && selectedLog) loadLogContent(selectedLog); }}
                />
              </div>
            </div>
            
            <div className={styles.logContainer}>
              <aside className={styles.logSidebar}>
                {logFiles.map(file => (
                  <button 
                    key={file.filename}
                    onClick={() => loadLogContent(file.filename)}
                    className={`${styles.logFileBtn} ${selectedLog === file.filename ? styles.active : ''}`}
                  >
                    <span className={styles.fileName}>{file.service}</span>
                    <span className={styles.fileMeta}>{formatBytes(file.size)}</span>
                  </button>
                ))}
              </aside>
              <main className={styles.logContent}>
                <pre>
                  {logContent || "// Select a log file to view contents"}
                </pre>
              </main>
            </div>
          </div>
        </div>

        {/* Right Column: Resources */}
        <div className={styles.sideCol}>
          {/* Resource Usage */}
          {resources && (
            <div className={`${styles.panel} card`}>
              <div className={styles.panelHeader}>
                <Cpu size={20} />
                <h3>Infrastructure Resources</h3>
              </div>
              <div className={styles.resourceGroup}>
                <div className={styles.resRow}>
                  <span>CPU Usage</span>
                  <span className={styles.resPercentage}>{resources.cpu.usage_percent.toFixed(1)}%</span>
                </div>
                <div className={styles.progressBar}><div className={styles.progressFill} style={{ width: `${resources.cpu.usage_percent}%` }} /></div>
                
                <div className={styles.resRow}>
                  <span>Memory Usage</span>
                  <span className={styles.resPercentage}>{((resources.memory.used / resources.memory.total) * 100).toFixed(1)}%</span>
                </div>
                <div className={styles.progressBar}><div className={styles.progressFill} style={{ width: `${(resources.memory.used / resources.memory.total) * 100}%` }} /></div>

                <div className={styles.resRow}>
                  <span>Disk Capacity</span>
                  <span className={styles.resPercentage}>{resources.disk.percent.toFixed(1)}%</span>
                </div>
                <div className={styles.progressBar}><div className={styles.progressFill} style={{ width: `${resources.disk.percent}%` }} /></div>

                <div className={styles.resRow} style={{ marginTop: '0.75rem' }}>
                  <span>Network TX</span>
                  <span className={styles.resPercentage}>{formatBytes(resources.network.bytes_sent)}</span>
                </div>
                <div className={styles.resRow}>
                  <span>Network RX</span>
                  <span className={styles.resPercentage}>{formatBytes(resources.network.bytes_recv)}</span>
                </div>
              </div>
            </div>
          )}

          {/* System Info */}
          <div className={`${styles.panel} card`}>
            <div className={styles.panelHeader}>
              <Server size={20} />
              <h3>System Info</h3>
            </div>
            <div className={styles.adminActionList}>
              {resources && (
                <>
                  <div className={styles.actionItem} style={{ cursor: 'default' }}>
                    <Cpu size={14} />
                    <span>CPU Cores: {resources.cpu.cores}</span>
                  </div>
                  <div className={styles.actionItem} style={{ cursor: 'default' }}>
                    <MemoryStick size={14} />
                    <span>RAM: {formatBytes(resources.memory.total)}</span>
                  </div>
                  <div className={styles.actionItem} style={{ cursor: 'default' }}>
                    <HardDrive size={14} />
                    <span>Disk: {formatBytes(resources.disk.total)}</span>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      {error && (
        <div className={styles.errorBanner}>
          <AlertTriangle size={16} />
          <span>{error}</span>
          <button onClick={() => setError(null)} className={styles.closeBtn}>×</button>
        </div>
      )}
    </div>
  );
}
