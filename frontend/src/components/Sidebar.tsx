'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import {
  Globe,
  HeartPulse,
  HelpCircle,
} from 'lucide-react';
import styles from './Sidebar.module.css';

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className={styles.sidebar}>
      {/* Brand */}
      <div className={styles.brand}>
        <span className={styles.brandName}>VesselWatch</span>
      </div>

      {/* Overview Link */}
      <div className={styles.section}>
        <h4 className={styles.sectionLabel}>Navigation</h4>
        <p className={styles.sectionHint}>Active Surveillance System</p>
      </div>

      <nav className={styles.regionNav}>
        <Link
          href="/"
          className={`${styles.regionItem} ${pathname === '/' ? styles.regionActive : ''}`}
        >
          <Globe size={16} />
          <span>Dashboard</span>
        </Link>
      </nav>

      {/* Bottom */}
      <div className={styles.bottomNav}>
        <Link href="/system-health" className={`${styles.bottomItem} ${pathname === '/system-health' ? styles.bottomActive : ''}`}>
          <HeartPulse size={16} />
          <span>System Health</span>
        </Link>
        <Link href="/settings" className={`${styles.bottomItem} ${pathname === '/settings' ? styles.bottomActive : ''}`}>
          <HelpCircle size={16} />
          <span>Support / Settings</span>
        </Link>
      </div>
    </aside>
  );
}
