import { useState, useEffect } from 'react'
import styles from './Navbar.module.css'

export default function Navbar() {
  const [scrolled, setScrolled] = useState(false)
  const [mobileOpen, setMobileOpen] = useState(false)

  useEffect(() => {
    const handler = () => setScrolled(window.scrollY > 40)
    window.addEventListener('scroll', handler, { passive: true })
    return () => window.removeEventListener('scroll', handler)
  }, [])

  return (
    <nav className={`${styles.navbar} ${scrolled ? styles.scrolled : ''}`}>
      <div className={styles.inner}>
        <a href="#" className={styles.logo}>
          <svg width="32" height="32" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="16" cy="16" r="14" stroke="#FF7B00" strokeWidth="1.5" opacity="0.3"/>
            <circle cx="16" cy="16" r="9" stroke="#FF7B00" strokeWidth="1.5" opacity="0.5"/>
            <circle cx="16" cy="16" r="4" stroke="#FF7B00" strokeWidth="1.5" opacity="0.8"/>
            <line x1="16" y1="2" x2="16" y2="12" stroke="#FF7B00" strokeWidth="1.5" strokeLinecap="round" opacity="0.6"/>
            <circle cx="16" cy="16" r="2" fill="#FF7B00"/>
            <path d="M16 16 L24 8" stroke="#FF7B00" strokeWidth="1.5" strokeLinecap="round" opacity="0.7"/>
          </svg>
          <span className={styles.logoText}>VesselWatch</span>
        </a>

        <div className={`${styles.links} ${mobileOpen ? styles.open : ''}`}>
          <a href="#pipeline" className={styles.link} onClick={() => setMobileOpen(false)}>Pipeline</a>
          <a href="#detection" className={styles.link} onClick={() => setMobileOpen(false)}>Detection</a>
          <a href="#impact" className={styles.link} onClick={() => setMobileOpen(false)}>Impact</a>
          <a href="#about" className={styles.link} onClick={() => setMobileOpen(false)}>About</a>
        </div>

        <a href="#detection" className={`btn btn-primary ${styles.cta}`}>
          Launch Console
        </a>

        <button
          className={`${styles.hamburger} ${mobileOpen ? styles.active : ''}`}
          onClick={() => setMobileOpen(!mobileOpen)}
          aria-label="Toggle menu"
        >
          <span /><span /><span />
        </button>
      </div>
    </nav>
  )
}
