'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Radio, RefreshCw, Bell } from 'lucide-react';
import styles from './Header.module.css';
import { useEffect, useState } from 'react';
import { fetchAlerts } from '@/lib/api';

const navTabs = [
  { label: 'Dashboard', href: '/' },
  { label: 'Map', href: '/map' },
  { label: 'Incidents', href: '/incidents' },
  { label: 'Analytics', href: '/analytics' },
  { label: 'System Health', href: '/system-health' },
  { label: 'Settings', href: '/settings' },
];

export default function Header() {
  const pathname = usePathname();
  const [alertCount, setAlertCount] = useState(0);

  useEffect(() => {
    fetchAlerts()
      .then((alerts) => setAlertCount(Array.isArray(alerts) ? alerts.length : 0))
      .catch(() => setAlertCount(0));
  }, []);

  return (
    <header className={styles.header}>
      {/* Nav Tabs */}
      <nav className={styles.navTabs}>
        {navTabs.map((tab) => (
          <Link
            key={tab.label}
            href={tab.href}
            className={`${styles.tab} ${pathname === tab.href ? styles.tabActive : ''}`}
          >
            {tab.label}
          </Link>
        ))}
      </nav>

      {/* Actions */}
      <div className={styles.actions}>
        <div className={styles.liveIndicator}>
          <span className={styles.liveDot}></span>
          LIVE
        </div>
        <button className={styles.iconBtn} title="WebSocket Status">
          <Radio size={16} />
        </button>
        <button className={styles.iconBtn} title="Refresh" onClick={() => window.location.reload()}>
          <RefreshCw size={16} />
        </button>
        <Link href="/" className={styles.iconBtn} title="Notifications">
          <Bell size={16} />
          {alertCount > 0 && <span className={styles.badge}>{alertCount}</span>}
        </Link>
        <div className={styles.avatar}>
          <img src="https://api.dicebear.com/7.x/initials/svg?seed=AK&backgroundColor=3B82F6&fontSize=40" alt="User" />
        </div>
      </div>
    </header>
  );
}
