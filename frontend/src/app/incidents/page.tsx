import { Filter, Download, ChevronDown, Search, MoreHorizontal } from 'lucide-react';
import styles from './page.module.css';

export default function Incidents() {
  const mockIncidents = [
    { id: 'INC-001', vessel: 'Tug Boat', sn: '245,132', loc: '25.8,-80.1', conf: 0.92, time: '2h ago', status: 'PENDING', badgeClass: 'detected' },
    { id: 'INC-002', vessel: 'Cargo Ship', sn: '246,521', loc: '26.2,-79.5', conf: 0.74, time: '4h ago', status: 'DETECTED', badgeClass: 'detected' },
    { id: 'INC-003', vessel: 'Tanker', sn: '247,108', loc: '24.9,-81.0', conf: 0.68, time: '6h ago', status: 'CONFIRMED', badgeClass: 'confirmed' },
    { id: 'INC-004', vessel: 'Fishing', sn: '247,654', loc: '27.1,-78.9', conf: 0.45, time: '12h ago', status: 'FALSE POS', badgeClass: 'false-pos' },
    { id: 'INC-005', vessel: 'Oil Tanker', sn: '248,201', loc: '25.3,-80.8', conf: 0.91, time: '14h ago', status: 'RESOLVED', badgeClass: 'resolved' },
    { id: 'INC-006', vessel: 'Bulk Carrier', sn: '249,112', loc: '26.8,-79.1', conf: 0.88, time: '1d ago', status: 'CONFIRMED', badgeClass: 'confirmed' },
    { id: 'INC-007', vessel: 'Unknown', sn: '---', loc: '28.1,-80.5', conf: 0.61, time: '1d ago', status: 'DETECTED', badgeClass: 'detected' },
  ];

  return (
    <div className={`${styles.container} animate-enter`}>
      <header className={styles.header}>
        <h1 className="text-gradient">Incident Management</h1>
        
        <div className={styles.headerActions}>
          <button className={styles.actionBtn}>
            <Filter size={16} /> Filters
          </button>
          <button className={styles.actionBtn}>
            <Download size={16} /> Export CSV
          </button>
        </div>
      </header>

      <div className={`${styles.filterBar} glass-panel`}>
        <div className={styles.filterGroup}>
          <span className={styles.filterLabel}>Status:</span>
          <div className={styles.dropdownBtn}>All <ChevronDown size={14} /></div>
        </div>
        <div className={styles.filterGroup}>
          <span className={styles.filterLabel}>Confidence:</span>
          <div className={styles.sliderMock}>
            <div className={styles.sliderTrack}><div className={styles.sliderFill}></div></div>
          </div>
        </div>
        <div className={styles.filterGroup}>
          <span className={styles.filterLabel}>Time:</span>
          <div className={styles.dropdownBtn}>30d <ChevronDown size={14} /></div>
        </div>
        <div className={styles.searchBox}>
          <Search size={14} className={styles.searchIcon} />
          <input type="text" placeholder="Search incidents..." />
        </div>
      </div>

      <div className={styles.tableStats}>
        Showing {mockIncidents.length} incidents (Page 1 of 3)
      </div>

      <div className={`${styles.tableContainer} glass-panel`}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th style={{ width: 40 }}><input type="checkbox" /></th>
              <th>Incident</th>
              <th>Location</th>
              <th>Confidence</th>
              <th>Detected</th>
              <th>Status</th>
              <th style={{ width: 60 }}></th>
            </tr>
          </thead>
          <tbody>
            {mockIncidents.map((inc, index) => (
              <tr key={index}>
                <td><input type="checkbox" /></td>
                <td>
                  <div className={styles.colMain}>{inc.id}</div>
                  <div className={styles.colSub}>Vessel: {inc.vessel} | S/N: {inc.sn}</div>
                </td>
                <td className={styles.fontMono}>{inc.loc}</td>
                <td>
                  <div className={styles.confBar}>
                    <div 
                      className={styles.confFill} 
                      style={{
                        width: `${inc.conf * 100}%`,
                        backgroundColor: inc.conf > 0.8 ? 'var(--success-green)' : inc.conf > 0.6 ? 'var(--warning-orange)' : 'var(--status-false-pos)'
                      }}
                    ></div>
                  </div>
                  <span className={styles.confText}>{inc.conf}</span>
                </td>
                <td>{inc.time}</td>
                <td>
                  <span className={`status-badge ${inc.badgeClass}`}>{inc.status}</span>
                </td>
                <td>
                  <button className={styles.moreBtn}><MoreHorizontal size={18} /></button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        
        <div className={styles.pagination}>
          <button className={styles.pageBtn} disabled>&lt; Previous</button>
          <span>Page 1 of 3</span>
          <button className={styles.pageBtn}>Next &gt;</button>
        </div>
      </div>
    </div>
  );
}
