import { useIntersectionObserver } from '../hooks/useIntersectionObserver'
import RadarScanner from './RadarScanner'
import styles from './Results.module.css'

const mockResults = [
  {
    id: 'SAR-2026-0417-A',
    status: 'VERIFIED',
    prediction: 'oil_spill',
    confidence: 0.947,
    vessel: 'MMSI 241004000',
    coordinates: '45.25°N, 10.55°E',
    area: '2.4 km²',
    timestamp: '2026-04-17T08:32:00Z',
    model: 'CNN-ResNet50-SAR-v3',
    sentinel: 'S1A_IW_GRDH_1SDV',
  },
  {
    id: 'SAR-2026-0415-C',
    status: 'FALSE_POSITIVE',
    prediction: 'natural_seep',
    confidence: 0.312,
    vessel: 'MMSI 256111000',
    coordinates: '38.45°N, 20.18°E',
    area: '0.8 km²',
    timestamp: '2026-04-15T14:07:00Z',
    model: 'CNN-ResNet50-SAR-v3',
    sentinel: 'S1B_IW_GRDH_1SDV',
  },
  {
    id: 'SAR-2026-0413-B',
    status: 'VERIFIED',
    prediction: 'oil_spill',
    confidence: 0.891,
    vessel: 'MMSI 311045000',
    coordinates: '28.92°N, -89.34°W',
    area: '5.1 km²',
    timestamp: '2026-04-13T22:15:00Z',
    model: 'CNN-ResNet50-SAR-v3',
    sentinel: 'S1A_IW_GRDH_1SDV',
  },
]

export default function Results() {
  const [ref, isVisible] = useIntersectionObserver(0.1)

  return (
    <section className={styles.section} ref={ref as React.RefObject<HTMLElement>}>
      <RadarScanner opacity={0.03} scale={1.2} />
      <div className="container" style={{ position: 'relative', zIndex: 10 }}>
        <div className={styles.header}>
          <p className={styles.eyebrow}>Detection History</p>
          <h2 className={styles.title}>Recent Analysis Results</h2>
          <p className={styles.subtitle}>
            Real-time event log showing SAR-verified anomaly investigations, correlated with AIS vessel data
            and Sentinel-1 imagery. Each entry tracks the complete lifecycle from anomaly detection through
            satellite verification.
          </p>
        </div>

        <div className={`${styles.tableWrap} ${isVisible ? styles.visible : ''}`}>
          <div className={styles.tableHeader}>
            <span>Event ID</span>
            <span>Status</span>
            <span>Confidence</span>
            <span>Vessel</span>
            <span>Coordinates</span>
            <span>Spill Area</span>
            <span>Timestamp</span>
          </div>

          {mockResults.map((r, i) => (
            <div
              key={r.id}
              className={styles.tableRow}
              style={{ animationDelay: `${i * 0.1}s` }}
            >
              <span className={styles.eventId}>{r.id}</span>
              <span>
                <span className={`${styles.statusBadge} ${r.status === 'VERIFIED' ? styles.verified : styles.falsePos}`}>
                  {r.status === 'VERIFIED' ? (
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="20 6 9 17 4 12" /></svg>
                  ) : (
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" /></svg>
                  )}
                  {r.status === 'VERIFIED' ? 'Verified' : 'False Positive'}
                </span>
              </span>
              <span>
                <span className={`${styles.confBar}`}>
                  <span
                    className={`${styles.confFill} ${r.confidence > 0.7 ? styles.high : styles.low}`}
                    style={{ width: `${r.confidence * 100}%` }}
                  />
                </span>
                <span className={styles.confText}>{(r.confidence * 100).toFixed(1)}%</span>
              </span>
              <span className={styles.mono}>{r.vessel}</span>
              <span className={styles.mono}>{r.coordinates}</span>
              <span className={styles.mono}>{r.area}</span>
              <span className={styles.timestamp}>
                {new Date(r.timestamp).toLocaleDateString('en-US', {
                  month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
                })}
              </span>
            </div>
          ))}
        </div>

        <div className={styles.footer}>
          <span className={styles.footerText}>
            Showing 3 of 152 events
          </span>
          <button 
            className="btn btn-outline" 
            style={{ padding: '8px 20px', fontSize: '0.8125rem' }}
            onClick={() => alert('Global Event Log module is coming soon!')}
          >
            View All Events
          </button>
        </div>
      </div>
    </section>
  )
}
