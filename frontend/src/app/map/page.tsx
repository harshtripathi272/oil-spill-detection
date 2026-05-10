'use client';

import dynamic from 'next/dynamic';
import styles from './page.module.css';

const IncidentMap = dynamic(() => import('@/components/map/IncidentMap'), {
  ssr: false,
  loading: () => (
    <div className={styles.mapLoading}>
      <span>Loading map…</span>
    </div>
  ),
});

export default function MapPage() {
  return (
    <div className={`${styles.pageRoot} animate-enter`}>
      <header className={styles.header}>
        <h1 className="text-gradient">Operations map</h1>
        <p className={styles.subtitle}>
          Live incident positions from the existing incidents API, with optional density heatmap.
        </p>
      </header>

      <div className={styles.mapShell}>
        <IncidentMap />
      </div>
    </div>
  );
}
