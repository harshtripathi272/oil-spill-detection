import Link from 'next/link';
import { 
  LayoutDashboard, 
  Map, 
  AlertTriangle, 
  BarChart3, 
  Settings, 
  LogOut 
} from 'lucide-react';
import styles from './Sidebar.module.css';

export default function Sidebar() {
  return (
    <aside className={`${styles.sidebar} glass-panel`}>
      <div className={styles.brand}>
        <div className={styles.logoIcon}></div>
        <div className={styles.brandText}>VesselWatch</div>
      </div>

      <nav className={styles.navigation}>
        <Link href="/" className={`${styles.navItem} ${styles.active}`}>
          <LayoutDashboard size={20} />
          <span>Overview</span>
        </Link>
        <Link href="/map" className={styles.navItem}>
          <Map size={20} />
          <span>Map View</span>
        </Link>
        <Link href="/incidents" className={styles.navItem}>
          <AlertTriangle size={20} />
          <span>Incidents</span>
        </Link>
        <Link href="/analytics" className={styles.navItem}>
          <BarChart3 size={20} />
          <span>Analytics</span>
        </Link>
      </nav>

      <div className={styles.bottomNav}>
        <div className={styles.navDivider}></div>
        <Link href="/settings" className={styles.navItem}>
          <Settings size={20} />
          <span>System Settings</span>
        </Link>
        <button className={styles.navItem}>
          <LogOut size={20} />
          <span>Logout</span>
        </button>
      </div>
    </aside>
  );
}
