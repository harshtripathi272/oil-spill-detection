import { useEffect, useState, useRef } from 'react'
import { useIntersectionObserver } from '../hooks/useIntersectionObserver'
import RadarScanner from './RadarScanner'
import styles from './Impact.module.css'

function AnimatedCounter({ target, suffix = '', prefix = '' }: { target: number; suffix?: string; prefix?: string }) {
  const [count, setCount] = useState(0)
  const [ref, isVisible] = useIntersectionObserver(0.3)
  const hasAnimated = useRef(false)

  useEffect(() => {
    if (!isVisible || hasAnimated.current) return
    hasAnimated.current = true

    const duration = 2000
    const steps = 60
    const increment = target / steps
    let current = 0
    const timer = setInterval(() => {
      current += increment
      if (current >= target) {
        current = target
        clearInterval(timer)
      }
      setCount(Math.round(current))
    }, duration / steps)

    return () => clearInterval(timer)
  }, [isVisible, target])

  return (
    <span ref={ref as React.RefObject<HTMLSpanElement>} className={styles.counterValue}>
      {prefix}{count.toLocaleString()}{suffix}
    </span>
  )
}

export default function Impact() {
  const [sectionRef, isVisible] = useIntersectionObserver(0.1)

  return (
    <section
      className={styles.section}
      id="impact"
      ref={sectionRef as React.RefObject<HTMLElement>}
    >
      {/* Ocean gradient overlay */}
      <div className={styles.oceanBg} />
      <RadarScanner opacity={0.04} scale={1.8} />

      <div className={`container ${styles.content}`}>
        <div className={styles.header}>
          <p className={styles.eyebrow}>Environmental Impact</p>
          <h2 className={styles.title}>
            Protecting the World's Oceans Through Intelligence
          </h2>
        </div>

        <div className={`${styles.grid} ${isVisible ? styles.visible : ''}`}>
          <div className={styles.textCol}>
            <p className={styles.lead}>
              Every year, approximately <strong>1.3 million tonnes</strong> of petroleum enters the
              world's oceans through operational discharges, tanker accidents, and illegal dumping.
              These spills devastate marine ecosystems, destroy coastal economies, and contaminate water
              supplies for decades.
            </p>
            <p className={styles.body}>
              Traditional detection methods rely on aerial surveillance and manual reporting — methods
              that are slow, expensive, and limited by weather and daylight. By the time a spill is
              identified through conventional means, irreversible ecological damage has often already
              occurred. Oil slicks can spread across hundreds of square kilometers within hours,
              suffocating marine life and contaminating fragile coastal habitats.
            </p>
            <p className={styles.body}>
              VesselWatch changes this paradigm. By combining real-time vessel behavior analysis with
              all-weather synthetic aperture radar satellites, we detect anomalous maritime activity
              and verify oil spills autonomously — reducing response time from days to minutes. Our
              system operates continuously across every ocean, in any weather condition, day or night.
            </p>
            <p className={styles.emphasis}>
              The difference between a 4-minute response and a 4-hour response can mean the
              difference between containing 200 litres or confronting 200,000 litres of crude oil
              in open water.
            </p>
          </div>

          <div className={styles.statsCol}>
            <div className={styles.impactCard}>
              <AnimatedCounter target={1300000} suffix=" tonnes" />
              <span className={styles.cardLabel}>Oil entering oceans annually</span>
            </div>
            <div className={styles.impactCard}>
              <AnimatedCounter target={94} suffix="%" />
              <span className={styles.cardLabel}>Faster detection vs. aerial surveys</span>
            </div>
            <div className={styles.impactCard}>
              <AnimatedCounter prefix="$" target={18} suffix="B" />
              <span className={styles.cardLabel}>Annual economic damage from spills</span>
            </div>
            <div className={styles.impactCard}>
              <AnimatedCounter target={240} suffix="x" />
              <span className={styles.cardLabel}>Coverage area vs. manual patrol</span>
            </div>
          </div>
        </div>

        <div className={styles.quoteBlock}>
          <blockquote className={styles.quote}>
            "The ocean is not a dustbin. It is the planet's most critical life-support system.
            Technology that accelerates spill detection by orders of magnitude is not an innovation
            — it is an obligation."
          </blockquote>
          <p className={styles.quoteAttrib}>
            — Maritime Environmental Protection Committee, IMO
          </p>
        </div>
      </div>
    </section>
  )
}
