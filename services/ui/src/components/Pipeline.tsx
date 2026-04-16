import { motion, useScroll, useTransform, useSpring } from 'framer-motion'
import { useRef } from 'react'
import { useIntersectionObserver } from '../hooks/useIntersectionObserver'
import styles from './Pipeline.module.css'

const pipelineSteps = [
  {
    id: 'ais',
    title: 'AIS Ingestion',
    desc: 'Real-time vessel position reports consumed via WebSocket from aisstream.io into Kafka',
    icon: (
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M2 20L5 17H10L12 14L14 17H19L22 20"/><path d="M4 14C4 14 6 10 12 10C18 10 20 14 20 14"/><path d="M8 10C8 10 9 6 12 6C15 6 16 10 16 10"/>
      </svg>
    ),
  },
  {
    id: 'stream',
    title: 'Stream Processing',
    desc: 'Normalization, validation, and temporal feature engineering with sliding windows',
    icon: (
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/>
      </svg>
    ),
  },
  {
    id: 'anomaly',
    title: 'Anomaly Detection',
    desc: 'Model-based scoring of vessel behavior anomalies using Isolation Forest algorithms',
    icon: (
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
      </svg>
    ),
  },
  {
    id: 'trigger',
    title: 'SAR Trigger',
    desc: 'Filtering high-confidence events for automated satellite imagery acquisition',
    icon: (
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4.93 4.93A10 10 0 0019.07 19.07"/><path d="M7.76 7.76a6 6 0 008.48 8.48"/><circle cx="12" cy="12" r="2"/>
      </svg>
    ),
  },
  {
    id: 'sentinel',
    title: 'Sentinel-1 Search',
    desc: 'Automated search and download of radar imagery from ESA Copernicus',
    icon: (
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="2" y="7" width="6" height="10" rx="1"/><path d="M22 12L14 7V17L22 12Z"/><line x1="8" y1="12" x2="14" y2="12" strokeDasharray="2 2"/>
        <circle cx="5" cy="5" r="1" fill="currentColor" opacity="0.5"/><path d="M5 5L8 8" strokeWidth="1" opacity="0.4"/>
      </svg>
    ),
  },
  {
    id: 'inference',
    title: 'AI Inference',
    desc: 'CNN-based classification of synthetic aperture radar images for oil slicks',
    icon: (
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M12 2a4 4 0 014 4c0 1.5-.8 2.8-2 3.5V11h3a4 4 0 014 4v1"/><path d="M8 9.5A4 4 0 016 6a4 4 0 014-4"/><path d="M6 16a4 4 0 01-4-4v-1"/><circle cx="12" cy="16" r="4"/><circle cx="12" cy="16" r="1.5" fill="currentColor"/>
      </svg>
    ),
  },
  {
    id: 'alert',
    title: 'Alert & Response',
    desc: 'Real-time alerts with spill coordinates, confidence score, and recommendations',
    icon: (
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 01-3.46 0"/><circle cx="18" cy="4" r="3" fill="#DC2626" stroke="#DC2626"/>
      </svg>
    ),
  },
]

const techStack = [
  'Apache Kafka',
  'Apache Airflow',
  'Sentinel-1 SAR',
  'PyTorch CNN',
  'FastAPI',
  'React',
  'Redis',
  'PostGIS',
]

/**
 * Animated Beam Component heavily inspired by Aceternity/Magic UI.
 * Creates a glowing path that animates along its stroke.
 */
function AnimatedBeam({ pathD }: { pathD: string }) {
  return (
    <svg className={styles.beamSvg} preserveAspectRatio="none" viewBox="0 0 800 100">
      <path
        d={pathD}
        stroke="rgba(255, 123, 0, 0.15)"
        strokeWidth="2"
        fill="none"
        strokeLinecap="round"
      />
      <motion.path
        d={pathD}
        stroke="url(#gradient)"
        strokeWidth="4"
        fill="none"
        strokeLinecap="round"
        initial={{ pathLength: 0, opacity: 0 }}
        animate={{ pathLength: 1, opacity: 1 }}
        transition={{
          pathLength: { duration: 3, ease: 'linear', repeat: Infinity },
          opacity: { duration: 0.5 },
        }}
      />
      <defs>
        <linearGradient id="gradient">
          <stop stopColor="rgba(255,123,0,0)" stopOpacity="0" />
          <stop stopColor="#FF7B00" stopOpacity="1" />
          <stop offset="1" stopColor="rgba(255,123,0,0)" stopOpacity="0" />
        </linearGradient>
      </defs>
    </svg>
  )
}

export default function Pipeline() {
  const [ref, isVisible] = useIntersectionObserver(0.1)
  const containerRef = useRef<HTMLElement>(null)
  
  // Create a scroll-linked animation for the cards spreading out
  const { scrollYProgress } = useScroll({
    target: containerRef,
    offset: ["start end", "end start"]
  })
  
  const ySpring = useSpring(scrollYProgress, { stiffness: 100, damping: 30 })
  const gapSize = useTransform(ySpring, [0, 1], [0, 24])

  return (
    <section className={styles.section} id="pipeline" ref={containerRef}>
      <div className="container" ref={ref as React.RefObject<HTMLDivElement>}>
        <div className={styles.header}>
          <p className={styles.eyebrow}>System Architecture</p>
          <h2 className={styles.title}>End-to-End Detection Flow</h2>
          <p className={styles.subtitle}>
            An event-driven pipeline powered by Apache Kafka, evaluating vessel behavior in real-time
            and triggering automated Sentinel-1 satellite imagery for verifiable, high-confidence oil
            slick classification.
          </p>
        </div>

        <div className={styles.pipelineContainer}>
          {/* Animated beam acting as the pipeline track in background */}
          <div className={styles.beamWrapper}>
             <AnimatedBeam pathD="M 0,50 L 800,50" />
          </div>

          <motion.div 
            className={styles.pipelineNodes}
            style={{ gap: gapSize }}
          >
            {pipelineSteps.map((step, i) => (
              <motion.div
                key={step.id}
                className={styles.stepCard}
                initial={{ opacity: 0, y: 30 }}
                animate={isVisible ? { 
                  opacity: 1, 
                  y: 0, 
                  transition: { delay: i * 0.05, type: "spring", stiffness: 400, damping: 25 }
                } : {}}
                whileHover={{ 
                  y: -6, 
                  scale: 1.02, 
                  boxShadow: "0 0 20px rgba(255, 123, 0, 0.2)",
                  transition: { duration: 0.05 }
                }}
              >
                <div className={styles.iconWrap}>{step.icon}</div>
                <h3 className={styles.stepTitle}>{step.title}</h3>
                <p className={styles.stepDesc}>{step.desc}</p>
                <span className={styles.stepNum}>{String(i + 1).padStart(2, '0')}</span>
              </motion.div>
            ))}
          </motion.div>
        </div>

        <div className={styles.techBar}>
          {techStack.map((tech) => (
            <span key={tech} className={styles.techChip}>{tech}</span>
          ))}
        </div>
      </div>
    </section>
  )
}
