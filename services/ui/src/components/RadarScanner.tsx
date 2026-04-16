import { motion } from 'framer-motion';
import styles from './RadarScanner.module.css';

interface RadarScannerProps {
  opacity?: number;
  scale?: number;
  color?: string;
}

export default function RadarScanner({ 
  opacity = 0.05, 
  scale = 1,
  color = 'var(--color-primary)'
}: RadarScannerProps) {
  return (
    <div className={styles.container} style={{ opacity, transform: `scale(${scale})` }}>
      {/* Central Axis */}
      <div className={styles.axis} style={{ borderColor: color }} />
      
      {/* Rotating Sweep */}
      <motion.div 
        className={styles.sweep}
        animate={{ rotate: 360 }}
        transition={{ duration: 6, repeat: Infinity, ease: "linear" }}
        style={{ 
          background: `conic-gradient(from 0deg at 50% 50%, transparent 270deg, ${color}33 360deg)` 
        }}
      />
      
      {/* Concentric Rings */}
      <div className={styles.ring} style={{ width: '25%', height: '25%', borderColor: color }} />
      <div className={styles.ring} style={{ width: '50%', height: '50%', borderColor: color }} />
      <div className={styles.ring} style={{ width: '75%', height: '75%', borderColor: color }} />
      <div className={styles.ring} style={{ width: '100%', height: '100%', borderColor: color }} />

      {/* Random Pulse Points */}
      <motion.div 
        className={styles.point}
        animate={{ opacity: [0, 1, 0] }}
        transition={{ duration: 2, repeat: Infinity, delay: 1 }}
        style={{ top: '20%', left: '30%', backgroundColor: color }}
      />
      <motion.div 
        className={styles.point}
        animate={{ opacity: [0, 1, 0] }}
        transition={{ duration: 3, repeat: Infinity, delay: 0.5 }}
        style={{ top: '60%', left: '70%', backgroundColor: color }}
      />
    </div>
  );
}
