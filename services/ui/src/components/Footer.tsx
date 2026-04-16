import styles from './Footer.module.css'

export default function Footer() {
  return (
    <footer className={styles.footer} id="about">
      <div className={`container ${styles.content}`}>
        <div className={styles.grid}>
          <div className={styles.brand}>
            <div className={styles.logoRow}>
              <svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
                <circle cx="16" cy="16" r="14" stroke="#FF7B00" strokeWidth="1.5" opacity="0.3"/>
                <circle cx="16" cy="16" r="9" stroke="#FF7B00" strokeWidth="1.5" opacity="0.5"/>
                <circle cx="16" cy="16" r="4" stroke="#FF7B00" strokeWidth="1.5" opacity="0.8"/>
                <circle cx="16" cy="16" r="2" fill="#FF7B00"/>
                <path d="M16 16 L24 8" stroke="#FF7B00" strokeWidth="1.5" strokeLinecap="round" opacity="0.7"/>
              </svg>
              <span className={styles.logoText}>VesselWatch</span>
            </div>
            <p className={styles.brandDesc}>
              AI-powered maritime oil spill detection using real-time AIS tracking,
              Sentinel-1 SAR imagery, and deep learning classification.
            </p>
            <div className={styles.statusRow}>
              <span className={styles.statusDot} />
              <span className={styles.statusText}>All Systems Operational</span>
            </div>
          </div>

          <div className={styles.col}>
            <h4 className={styles.colTitle}>Platform</h4>
            <a href="#pipeline" className={styles.colLink}>Detection Pipeline</a>
            <a href="#detection" className={styles.colLink}>SAR Analysis</a>
            <a href="#impact" className={styles.colLink}>Environmental Impact</a>
            <a href="#" className={styles.colLink}>API Documentation</a>
          </div>

          <div className={styles.col}>
            <h4 className={styles.colTitle}>Technology</h4>
            <a href="#" className={styles.colLink}>Apache Kafka</a>
            <a href="#" className={styles.colLink}>Apache Airflow</a>
            <a href="#" className={styles.colLink}>Sentinel-1 SAR</a>
            <a href="#" className={styles.colLink}>PyTorch Models</a>
          </div>

          <div className={styles.col}>
            <h4 className={styles.colTitle}>Resources</h4>
            <a href="#" className={styles.colLink}>GitHub Repository</a>
            <a href="#" className={styles.colLink}>Research Paper</a>
            <a href="#" className={styles.colLink}>System Architecture</a>
            <a href="#" className={styles.colLink}>Contact</a>
          </div>
        </div>

        <div className={styles.bottom}>
          <p className={styles.copyright}>
            &copy; {new Date().getFullYear()} VesselWatch. Maritime Intelligence Division.
          </p>
          <p className={styles.version}>v1.0.0 — Build {new Date().toISOString().slice(0, 10)}</p>
        </div>
      </div>
    </footer>
  )
}
