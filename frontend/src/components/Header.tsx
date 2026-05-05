import { Bell, Search } from 'lucide-react';
import styles from './Header.module.css';

export default function Header() {
  return (
    <header className={`${styles.header} glass-panel`}>
      <div className={styles.searchBar}>
        <Search size={18} className={styles.searchIcon} />
        <input 
          type="text" 
          placeholder="Search by ID, coordinates, or vessel..."
          className={styles.searchInput}
        />
      </div>

      <div className={styles.actions}>
        <div className={styles.systemStatus}>
          <span className={styles.statusDot}></span>
          System Online
        </div>
        <button className={styles.notificationBtn}>
          <Bell size={20} />
          <span className={styles.badge}>3</span>
        </button>
        <div className={styles.profile}>
          <div className={styles.avatar}>OP</div>
          <div className={styles.userInfo}>
            <div className={styles.userName}>Cmdr. Operator</div>
            <div className={styles.userRole}>Duty Officer</div>
          </div>
        </div>
      </div>
    </header>
  );
}
