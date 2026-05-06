import { Maximize, Layers, MapPin } from 'lucide-react';
import styles from './page.module.css';

export default function MapView() {
  return (
    <div className={`${styles.container} animate-enter`}>
      <header className={styles.header}>
        <h1 className="text-gradient">Interactive Map View</h1>
      </header>

      <div className={`${styles.mapContainer} glass-panel`}>
        <div className={styles.mapToolsTop}>
          <div className={styles.filterBar}>
            <div className={styles.filterBtn}>Status: All</div>
            <div className={styles.filterBtn}>Region: Global</div>
            <input type="text" placeholder="Search location..." className={styles.searchInput} />
          </div>
          <div className={styles.mapActions}>
            <button className={styles.actionBtn}><Layers size={18} /></button>
            <button className={styles.actionBtn}><Maximize size={18} /></button>
          </div>
        </div>

        <div className={styles.mapMock}>
          <div className={styles.mapOverlay}>
            <div className={`${styles.marker} ${styles.markerRed}`} style={{top: '40%', left: '30%'}}>
              <MapPin size={24} />
            </div>
            <div className={`${styles.marker} ${styles.markerOrange}`} style={{top: '45%', left: '35%'}}>
              <MapPin size={20} />
            </div>
            <div className={`${styles.marker} ${styles.markerOrange}`} style={{top: '60%', left: '50%'}}>
              <MapPin size={20} />
            </div>
            <div className={`${styles.marker} ${styles.markerGreen}`} style={{top: '30%', left: '70%'}}>
              <MapPin size={20} />
            </div>
            
            {/* Popover mock */}
            <div className={styles.popover} style={{top: '25%', left: '32%'}}>
              <div className={styles.popoverTitle}>🔴 INC-20260505-001</div>
              <div className={styles.popoverBody}>
                <div>Loc: 25.80°N, 80.10°W</div>
                <div>Conf: 0.92</div>
                <div>Status: PENDING_IMAGERY</div>
              </div>
              <button className={styles.detailsBtn}>View Details</button>
            </div>
          </div>
        </div>

        <div className={styles.legend}>
          <div className={styles.legendTitle}>Legend</div>
          <div className={styles.legendList}>
            <div className={styles.legendItem}><span className={styles.dotRed}></span> Confirmed</div>
            <div className={styles.legendItem}><span className={styles.dotOrange}></span> Detected</div>
            <div className={styles.legendItem}><span className={styles.dotGreen}></span> Resolved</div>
            <div className={styles.legendItem}><span className={styles.dotGray}></span> False Positive</div>
          </div>
        </div>
      </div>
    </div>
  );
}
