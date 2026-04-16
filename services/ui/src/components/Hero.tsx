import styles from './Hero.module.css'

export default function Hero() {
  return (
    <section className={styles.hero} id="hero">
      {/* Animated grid background */}
      <div className={styles.gridBg}>
        <svg width="100%" height="100%" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <pattern id="heroGrid" width="60" height="60" patternUnits="userSpaceOnUse">
              <path d="M 60 0 L 0 0 0 60" fill="none" stroke="rgba(255,123,0,0.04)" strokeWidth="0.5"/>
            </pattern>
          </defs>
          <rect width="100%" height="100%" fill="url(#heroGrid)" />
        </svg>
      </div>

      {/* Sonar pulse rings */}
      <div className={styles.sonarContainer}>
        <div className={styles.sonarRing} style={{ animationDelay: '0s' }} />
        <div className={styles.sonarRing} style={{ animationDelay: '1.5s' }} />
        <div className={styles.sonarRing} style={{ animationDelay: '3s' }} />
      </div>

      <div className={`container ${styles.content}`}>
        <div className={styles.textBlock}>
          <p className={styles.eyebrow}>Maritime Intelligence Platform</p>
          <h1 className={styles.headline}>
            Detecting{' '}
            <span className={styles.glowText}>Oil Spills</span>
            {' '}Before They Become Disasters
          </h1>
          <p className={styles.subheadline}>
            VesselWatch fuses real-time AIS vessel tracking with Sentinel-1 SAR satellite imagery
            and deep learning to identify maritime oil spills within minutes — not days. Our event-driven
            pipeline processes thousands of position reports per second, scoring vessel anomalies and
            triggering automated satellite verification.
          </p>
          <div className={styles.actions}>
            <a href="#detection" className="btn btn-primary">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>
              </svg>
              Start Detection
            </a>
            <a href="#pipeline" className="btn btn-outline">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
              </svg>
              View Pipeline
            </a>
          </div>
        </div>

        <div className={styles.visual}>
          <div className={styles.radarDisplay}>
            {/* Outer ring */}
            <svg className={styles.radarSvg} viewBox="0 0 400 400" fill="none" xmlns="http://www.w3.org/2000/svg">
              {/* Concentric circles */}
              <circle cx="200" cy="200" r="180" stroke="rgba(255,123,0,0.08)" strokeWidth="0.5"/>
              <circle cx="200" cy="200" r="140" stroke="rgba(255,123,0,0.1)" strokeWidth="0.5"/>
              <circle cx="200" cy="200" r="100" stroke="rgba(255,123,0,0.12)" strokeWidth="0.5"/>
              <circle cx="200" cy="200" r="60" stroke="rgba(255,123,0,0.15)" strokeWidth="0.5"/>
              {/* Cross hairs */}
              <line x1="200" y1="15" x2="200" y2="385" stroke="rgba(255,123,0,0.06)" strokeWidth="0.5"/>
              <line x1="15" y1="200" x2="385" y2="200" stroke="rgba(255,123,0,0.06)" strokeWidth="0.5"/>
              <line x1="60" y1="60" x2="340" y2="340" stroke="rgba(255,123,0,0.04)" strokeWidth="0.5"/>
              <line x1="340" y1="60" x2="60" y2="340" stroke="rgba(255,123,0,0.04)" strokeWidth="0.5"/>
              {/* Sweep line */}
              <line x1="200" y1="200" x2="200" y2="20" stroke="url(#sweepGrad)" strokeWidth="1.5" className={styles.sweepLine}/>
              <defs>
                <linearGradient id="sweepGrad" x1="200" y1="200" x2="200" y2="20">
                  <stop offset="0%" stopColor="#FF7B00" stopOpacity="0.6"/>
                  <stop offset="100%" stopColor="#FF7B00" stopOpacity="0"/>
                </linearGradient>
              </defs>
              {/* Vessel dots */}
              <circle cx="145" cy="120" r="3" fill="#FF7B00" opacity="0.8">
                <animate attributeName="opacity" values="0.8;0.3;0.8" dur="3s" repeatCount="indefinite"/>
              </circle>
              <circle cx="260" cy="150" r="3" fill="#FF7B00" opacity="0.6">
                <animate attributeName="opacity" values="0.6;0.2;0.6" dur="4s" repeatCount="indefinite"/>
              </circle>
              <circle cx="170" cy="260" r="3" fill="#FF7B00" opacity="0.7">
                <animate attributeName="opacity" values="0.7;0.3;0.7" dur="3.5s" repeatCount="indefinite"/>
              </circle>
              <circle cx="310" cy="230" r="3" fill="#FF6B35" opacity="0.9">
                <animate attributeName="opacity" values="0.9;0.4;0.9" dur="2s" repeatCount="indefinite"/>
              </circle>
              {/* Vessel trail */}
              <path d="M310 230 L290 250 L265 245 L250 260 L240 255" stroke="#FF6B35" strokeWidth="1" opacity="0.4" strokeDasharray="4 3"/>
              {/* Oil spill blob */}
              <ellipse cx="310" cy="230" rx="25" ry="15" fill="rgba(220,38,38,0.15)" stroke="rgba(220,38,38,0.3)" strokeWidth="0.5" transform="rotate(30 310 230)"/>
              {/* Coordinate labels */}
              <text x="15" y="198" fill="rgba(148,163,184,0.4)" fontSize="8" fontFamily="JetBrains Mono">45.2°N</text>
              <text x="185" y="395" fill="rgba(148,163,184,0.4)" fontSize="8" fontFamily="JetBrains Mono">10.5°E</text>
            </svg>
            {/* Radar sweep glow cone */}
            <div className={styles.sweepCone} />
          </div>
        </div>
      </div>

      {/* Stats bar */}
      <div className={styles.statsBar}>
        <div className={`container ${styles.statsInner}`}>
          <div className={styles.statCard}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#FF7B00" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/>
            </svg>
            <div>
              <span className={styles.statValue}>98.7%</span>
              <span className={styles.statLabel}>Detection Accuracy</span>
            </div>
          </div>
          <div className={styles.statCard}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#FF7B00" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>
            </svg>
            <div>
              <span className={styles.statValue}>&lt; 4 min</span>
              <span className={styles.statLabel}>Response Time</span>
            </div>
          </div>
          <div className={styles.statCard}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#FF7B00" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M2 20L5 17H10L12 15L14 17H19L22 20"/><path d="M4 15C4 15 6 11 12 11C18 11 20 15 20 15"/><circle cx="12" cy="7" r="2"/>
            </svg>
            <div>
              <span className={styles.statValue}>2,400+</span>
              <span className={styles.statLabel}>Vessels Tracked</span>
            </div>
          </div>
          <div className={styles.statCard}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#FF7B00" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="10" opacity="0.3"/>
              <circle cx="12" cy="12" r="6" opacity="0.5"/>
              <circle cx="12" cy="12" r="2" fill="#FF7B00"/>
            </svg>
            <div>
              <span className={styles.statValue}>150+</span>
              <span className={styles.statLabel}>Spills Detected</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
