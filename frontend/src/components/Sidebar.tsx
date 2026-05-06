'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import {
  Globe,
  Anchor,
  Waves,
  Compass,
  Snowflake,
  Plus,
  HeartPulse,
  HelpCircle,
} from 'lucide-react';
import styles from './Sidebar.module.css';

const regions = [
  { label: 'Global View', icon: Globe, href: '/' },
  { label: 'North Atlantic', icon: Anchor, href: '/?region=north-atlantic' },
  { label: 'South Pacific', icon: Waves, href: '/?region=south-pacific' },
  { label: 'Mediterranean', icon: Compass, href: '/?region=mediterranean' },
  { label: 'Arctic Ops', icon: Snowflake, href: '/?region=arctic' },
];

export default function Sidebar() {
  const pathname = usePathname();
  const isHome = pathname === '/';

  return (
    <aside className={styles.sidebar}>
      {/* Brand */}
      <div className={styles.brand}>
        <span className={styles.brandName}>VesselWatch</span>
      </div>

      {/* Regional Filters */}
      <div className={styles.section}>
        <h4 className={styles.sectionLabel}>Regional Filters</h4>
        <p className={styles.sectionHint}>Active Surveillance Zones</p>
      </div>

      <nav className={styles.regionNav}>
        {regions.map((r) => {
          const Icon = r.icon;
          const isActive = isHome && (r.href === '/' ? !new URLSearchParams().has('region') : false);
          return (
            <Link
              key={r.label}
              href={r.href}
              className={`${styles.regionItem} ${r.href === '/' && isHome ? styles.regionActive : ''}`}
            >
              <Icon size={16} />
              <span>{r.label}</span>
            </Link>
          );
        })}
      </nav>

      {/* New Incident */}
      <div className={styles.actionArea}>
        <button className={styles.newIncidentBtn}>
          <Plus size={16} />
          <span>New Incident</span>
        </button>
      </div>

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
