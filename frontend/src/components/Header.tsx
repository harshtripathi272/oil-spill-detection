'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Search, Radio, RefreshCw, Bell } from 'lucide-react';
import styles from './Header.module.css';

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

      {/* Search */}
      <div className={styles.searchBar}>
        <Search size={14} className={styles.searchIcon} />
        <input
          type="text"
          placeholder="Search logs..."
          className={styles.searchInput}
        />
      </div>

      {/* Actions */}
      <div className={styles.actions}>
        <div className={styles.liveIndicator}>
          <span className={styles.liveDot}></span>
          LIVE
        </div>
        <button className={styles.iconBtn} title="WebSocket Status">
          <Radio size={16} />
        </button>
        <button className={styles.iconBtn} title="Refresh">
          <RefreshCw size={16} />
        </button>
        <button className={styles.iconBtn} title="Notifications">
          <Bell size={16} />
          <span className={styles.badge}>3</span>
        </button>
        <div className={styles.avatar}>
          <img src="https://api.dicebear.com/7.x/initials/svg?seed=JS&backgroundColor=3B82F6&fontSize=40" alt="User" />
        </div>
      </div>
    </header>
  );
}
