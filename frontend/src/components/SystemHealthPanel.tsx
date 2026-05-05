'use client';

import { useEffect, useState } from 'react';
import { Shield, Key, Bell, Users, Settings, Cpu, HardDrive, MemoryStick, MoreVertical } from 'lucide-react';
import styles from './SystemHealthPanel.module.css';
import { fetchSystemHealth, fetchSystemResources } from '@/lib/api';

interface User {
  id: number;
  username: string;
  full_name?: string;
  email?: string;
  role: string;
  enabled: boolean;
}

export default function SystemHealthPanel() {
  const [health, setHealth] = useState<any>(null);
  const [resources, setResources] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

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
        setError((err as Error).message);
      }
    }
    load();
  }, []);

  const mockUsers: User[] = [
    { id: 1, username: 'j.sterling', full_name: 'J. Sterling', role: 'ADMIN', enabled: true },
    { id: 2, username: 'm.chen', full_name: 'M. Chen', role: 'ANALYST', enabled: true },
    { id: 3, username: 't.reynolds', full_name: 'T. Reynolds', role: 'OPERATOR', enabled: false },
  ];

  return (
    <div className={`${styles.page} animate-enter`}>
      <div className={styles.pageHeader}>
        <h1>Settings & Administration</h1>
        <p className={styles.subtitle}>Manage system preferences, security policies, and operational personnel.</p>
      </div>

      {/* Top Grid: Profile, Security, API */}
      <div className={styles.topGrid}>
        {/* Profile Management */}
        <div className={`${styles.section} card`}>
          <div className={styles.sectionHeader}>
            <Users size={16} />
            <h3>Profile Management</h3>
          </div>
          <div className={styles.profileRow}>
            <div className={styles.profileAvatar}>
              <img src="https://api.dicebear.com/7.x/initials/svg?seed=JS&backgroundColor=3B82F6&fontSize=40" alt="User" />
            </div>
            <div>
              <div className={styles.profileName}>Cmdr. James Sterling</div>
              <div className={styles.profileRole}>Global Operations Lead</div>
            </div>
          </div>
          <div className={styles.fieldGroup}>
            <label className={styles.fieldLabel}>Email Address</label>
            <input className={styles.fieldInput} defaultValue="j.sterling@vesselwatch.gov" readOnly />
          </div>
          <button className={styles.btnOutline}>Update Profile</button>
        </div>

        {/* Security Settings */}
        <div className={`${styles.section} card`}>
          <div className={styles.sectionHeader}>
            <Shield size={16} className={styles.iconDanger} />
            <h3>Security Settings</h3>
          </div>
          <div className={styles.settingRow}>
            <div>
              <div className={styles.settingTitle}>Multi-Factor Auth (MFA)</div>
              <div className={styles.settingHint} style={{ color: 'var(--success)' }}>Currently Enforced</div>
            </div>
            <button className={styles.configBtn}>Configure</button>
          </div>
          <div className={styles.settingRow}>
            <div>
              <div className={styles.settingTitle}>Session Timeout (Minutes)</div>
            </div>
            <div className={styles.inlineInput}>
              <input className={styles.fieldInput} defaultValue="15" style={{ width: 60 }} />
              <button className={styles.applyBtn}>Apply</button>
            </div>
          </div>
          <div className={styles.settingRow}>
            <div>
              <div className={styles.settingTitle}>IP Whitelist Config</div>
            </div>
          </div>
          <textarea className={styles.codeArea} defaultValue={"192.168.1.0/24\n10.0.0.0/8"} rows={3} />
          <button className={styles.editLink}>✏️ Edit Rules</button>
        </div>

        {/* API Configuration */}
        <div className={`${styles.section} card`}>
          <div className={styles.sectionHeader}>
            <Key size={16} />
            <h3>API Configuration</h3>
          </div>
          <div className={styles.settingRow}>
            <div className={styles.settingTitle}>Active Key (Read-Only)</div>
            <span className={styles.activeBadge}>Active</span>
          </div>
          <div className={styles.apiKeyBox}>
            <span className={styles.apiKeyVal}>vw_live_78x91...kl2p</span>
            <button className={styles.copyBtn}>📋</button>
          </div>
          <div className={styles.separator} />
          <div className={styles.settingTitle}>Usage Quota</div>
          <div className={styles.settingHint}>Current Billing Cycle</div>
          <div className={styles.quotaRow}>
            <span>45,201 / 100k</span>
          </div>
          <div className={styles.quotaBar}>
            <div className={styles.quotaFill} style={{ width: '45%' }} />
          </div>
          <button className={styles.btnOutline} style={{ marginTop: 12 }}>📊 View Usage Logs</button>
        </div>
      </div>

      {/* Middle Grid: Preferences + Alert Routing */}
      <div className={styles.midGrid}>
        {/* Dashboard Preferences */}
        <div className={`${styles.section} card`}>
          <div className={styles.sectionHeader}>
            <Settings size={16} />
            <h3>Dashboard Preferences</h3>
          </div>
          <div className={styles.settingRow}>
            <div>
              <div className={styles.settingTitle}>Data Refresh Rate</div>
              <div className={styles.settingHint}>WebSocket sync interval</div>
            </div>
            <select className={styles.selectField}>
              <option>30 Seconds</option>
              <option>15 Seconds</option>
              <option>60 Seconds</option>
            </select>
          </div>
          <div className={styles.settingRow}>
            <div>
              <div className={styles.settingTitle}>High-Contrast Mode</div>
              <div className={styles.settingHint}>Enhance map markers</div>
            </div>
            <label className={styles.toggle}>
              <input type="checkbox" defaultChecked />
              <span className={styles.toggleSlider} />
            </label>
          </div>
        </div>

        {/* Alert Routing */}
        <div className={`${styles.section} card`}>
          <div className={styles.sectionHeader}>
            <Bell size={16} />
            <h3>Alert Routing</h3>
          </div>
          <div className={styles.settingRow}>
            <div>
              <div className={styles.settingTitle}>High-Confidence Events</div>
              <div className={styles.settingHint}>In-app & Email</div>
            </div>
            <label className={styles.toggle}>
              <input type="checkbox" defaultChecked />
              <span className={styles.toggleSlider} />
            </label>
          </div>
          <div className={styles.settingRow}>
            <div>
              <div className={styles.settingTitle}>System Health Warnings</div>
              <div className={styles.settingHint}>In-app only</div>
            </div>
            <label className={styles.toggle}>
              <input type="checkbox" defaultChecked />
              <span className={styles.toggleSlider} />
            </label>
          </div>

          {/* System resources */}
          {resources && (
            <>
              <div className={styles.separator} />
              <div className={styles.sysResGrid}>
                <div className={styles.resItem}>
                  <Cpu size={14} />
                  <span>CPU</span>
                  <span className={styles.resVal}>{resources.cpu.usage_percent}%</span>
                </div>
                <div className={styles.resItem}>
                  <MemoryStick size={14} />
                  <span>Memory</span>
                  <span className={styles.resVal}>{((resources.memory.used / resources.memory.total) * 100).toFixed(0)}%</span>
                </div>
                <div className={styles.resItem}>
                  <HardDrive size={14} />
                  <span>Disk</span>
                  <span className={styles.resVal}>{resources.disk.percent}%</span>
                </div>
              </div>
            </>
          )}

          {error && <div className={styles.errorSmall}>{error}</div>}
        </div>
      </div>

      {/* User Management */}
      <div className={`${styles.userSection} card`}>
        <div className={styles.userHeader}>
          <div className={styles.sectionHeader}>
            <Users size={16} />
            <h3>User Management</h3>
          </div>
          <button className={styles.addUserBtn}>👤+ Add User</button>
        </div>
        <table className={styles.userTable}>
          <thead>
            <tr>
              <th>Name</th>
              <th>Role</th>
              <th>Status</th>
              <th>Last Login</th>
              <th style={{ width: 40 }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {mockUsers.map((user) => (
              <tr key={user.id}>
                <td>
                  <div className={styles.userCell}>
                    <div className={styles.userAvatar}>
                      {user.full_name?.split(' ').map(n => n[0]).join('') || '?'}
                    </div>
                    <span>{user.full_name || user.username}</span>
                  </div>
                </td>
                <td>
                  <span className={styles.roleBadge}>{user.role}</span>
                </td>
                <td>
                  <span className={user.enabled ? styles.statusActive : styles.statusOffline}>
                    ● {user.enabled ? 'Active' : 'Offline'}
                  </span>
                </td>
                <td className={styles.monoSmall}>2023-10-27 14:02Z</td>
                <td>
                  <button className={styles.moreBtn}><MoreVertical size={16} /></button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
