import { useState } from 'react'
import { useFileUpload } from '../hooks/useFileUpload'
import { useIntersectionObserver } from '../hooks/useIntersectionObserver'
import RadarScanner from './RadarScanner'
import styles from './Upload.module.css'

export default function Upload() {
  const [ref, isVisible] = useIntersectionObserver(0.1)
  const upload = useFileUpload()
  const [analyzing, setAnalyzing] = useState(false)
  const [result, setResult] = useState<null | { prediction: string; confidence: number }>(null)

  const handleAnalyze = () => {
    if (!upload.file) return
    setAnalyzing(true)
    setResult(null)
    upload.simulateUpload()

    // Simulate AI analysis
    setTimeout(() => {
      setAnalyzing(false)
      setResult({
        prediction: 'oil_spill',
        confidence: 0.947,
      })
    }, 4000)
  }

  const handleReset = () => {
    upload.reset()
    setResult(null)
    setAnalyzing(false)
  }

  return (
    <section
      className={styles.section}
      id="detection"
      ref={ref as React.RefObject<HTMLElement>}
    >
      <RadarScanner opacity={0.02} scale={1.5} />
      <div className="container" style={{ position: 'relative', zIndex: 10 }}>
        <div className={styles.header}>
          <p className={styles.eyebrow}>SAR Image Analysis</p>
          <h2 className={styles.title}>Upload & Detect</h2>
          <p className={styles.subtitle}>
            Upload synthetic aperture radar imagery for automated oil spill classification. Our CNN model
            analyzes the image structure, identifies dark formations consistent with oil slicks, and returns
            a confidence-scored prediction within seconds.
          </p>
        </div>

        <div className={`${styles.uploadArea} ${isVisible ? styles.visible : ''}`}>
          {!upload.file ? (
            <div
              className={`${styles.dropZone} ${upload.isDragging ? styles.dragging : ''}`}
              onDragOver={upload.onDragOver}
              onDragLeave={upload.onDragLeave}
              onDrop={upload.onDrop}
              onClick={upload.triggerFileInput}
              role="button"
              tabIndex={0}
              aria-label="Upload SAR image"
            >
              <input
                ref={upload.inputRef}
                type="file"
                accept=".tif,.tiff,.png,.jpg,.jpeg,.webp"
                onChange={upload.onFileSelect}
                className={styles.fileInput}
              />
              <div className={styles.dropIcon}>
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M4 14.899A7 7 0 1115.71 8h1.79a4.5 4.5 0 012.5 8.242" />
                  <path d="M12 12v9" /><path d="M8 17l4-4 4 4" />
                </svg>
                {/* Sonar rings */}
                <div className={styles.sonarRing1} />
                <div className={styles.sonarRing2} />
              </div>
              <h3 className={styles.dropTitle}>Drop SAR image here</h3>
              <p className={styles.dropText}>or click to browse files</p>
              <div className={styles.formatBadges}>
                <span className={styles.badge}>GeoTIFF</span>
                <span className={styles.badge}>PNG</span>
                <span className={styles.badge}>JPEG</span>
              </div>
              <p className={styles.sizeLimit}>Maximum file size: 50MB</p>
            </div>
          ) : (
            <div className={styles.previewArea}>
              <div className={styles.previewLeft}>
                <div className={styles.imageContainer}>
                  {upload.preview && (
                    <img src={upload.preview} alt="SAR preview" className={styles.previewImage} />
                  )}
                  {analyzing && (
                    <div className={styles.scanOverlay}>
                      <div className={styles.scanLine} />
                    </div>
                  )}
                  {result && result.prediction === 'oil_spill' && (
                    <div className={styles.detectionOverlay}>
                      <svg className={styles.detectionSvg} viewBox="0 0 100 100" preserveAspectRatio="none">
                        <ellipse cx="62" cy="45" rx="22" ry="14" fill="rgba(220,38,38,0.2)" stroke="rgba(220,38,38,0.6)" strokeWidth="0.8" strokeDasharray="3 2" transform="rotate(25 62 45)" />
                        <ellipse cx="58" cy="52" rx="15" ry="8" fill="rgba(220,38,38,0.15)" stroke="rgba(220,38,38,0.4)" strokeWidth="0.5" transform="rotate(15 58 52)" />
                      </svg>
                    </div>
                  )}
                </div>
                <div className={styles.fileInfo}>
                  <span className={styles.fileName}>{upload.file.name}</span>
                  <span className={styles.fileSize}>
                    {(upload.file.size / (1024 * 1024)).toFixed(2)} MB
                  </span>
                </div>
              </div>

              <div className={styles.previewRight}>
                {upload.isUploading || analyzing ? (
                  <div className={styles.processingState}>
                    <div className={styles.radarWrap}>
                      <div className={styles.radarPulse} />
                      <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="var(--color-primary)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                        <circle cx="12" cy="12" r="10" opacity="0.2" />
                        <circle cx="12" cy="12" r="6" opacity="0.4" />
                        <circle cx="12" cy="12" r="2" fill="var(--color-primary)" />
                      </svg>
                    </div>
                    <h3 className={styles.processTitle}>Analyzing SAR Imagery</h3>
                    <p className={styles.processText}>
                      Running CNN inference on radar backscatter patterns...
                    </p>
                    <div className={styles.progressBar}>
                      <div
                        className={styles.progressFill}
                        style={{ width: `${upload.progress}%` }}
                      />
                    </div>
                    <span className={styles.progressLabel}>
                      {Math.round(upload.progress)}% complete
                    </span>
                  </div>
                ) : result ? (
                  <div className={styles.resultState}>
                    <div className={`${styles.resultBadge} ${styles.spill}`}>
                      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
                        <line x1="12" y1="9" x2="12" y2="13" />
                        <line x1="12" y1="17" x2="12.01" y2="17" />
                      </svg>
                      OIL SPILL DETECTED
                    </div>
                    <div className={styles.confidenceRing}>
                      <svg viewBox="0 0 120 120" className={styles.ringChart}>
                        <circle cx="60" cy="60" r="52" fill="none" stroke="rgba(255,123,0,0.1)" strokeWidth="8" />
                        <circle
                          cx="60" cy="60" r="52"
                          fill="none" stroke="var(--color-primary)"
                          strokeWidth="8" strokeLinecap="round"
                          strokeDasharray={`${2 * Math.PI * 52 * result.confidence} ${2 * Math.PI * 52}`}
                          transform="rotate(-90 60 60)"
                          className={styles.ringFill}
                        />
                      </svg>
                      <div className={styles.ringValue}>
                        <span className={styles.confNum}>{(result.confidence * 100).toFixed(1)}%</span>
                        <span className={styles.confLabel}>Confidence</span>
                      </div>
                    </div>
                    <div className={styles.metaGrid}>
                      <div className={styles.metaItem}>
                        <span className={styles.metaLabel}>Model</span>
                        <span className={styles.metaValue}>CNN-ResNet50-SAR-v3</span>
                      </div>
                      <div className={styles.metaItem}>
                        <span className={styles.metaLabel}>Timestamp</span>
                        <span className={styles.metaValue}>{new Date().toISOString().slice(0, 19)}Z</span>
                      </div>
                      <div className={styles.metaItem}>
                        <span className={styles.metaLabel}>Coordinates</span>
                        <span className={styles.metaValue}>45.25°N, 10.55°E</span>
                      </div>
                      <div className={styles.metaItem}>
                        <span className={styles.metaLabel}>Spill Area</span>
                        <span className={styles.metaValue}>~2.4 km²</span>
                      </div>
                    </div>
                    <button className="btn btn-outline" onClick={handleReset}>
                      Analyze Another Image
                    </button>
                  </div>
                ) : (
                  <div className={styles.readyState}>
                    <h3>Image Ready</h3>
                    <p className={styles.readyText}>
                      SAR image loaded successfully. Click below to run the oil spill detection model.
                    </p>
                    <button className="btn btn-primary" onClick={handleAnalyze}>
                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <circle cx="11" cy="11" r="8" /><path d="m21 21-4.3-4.3" />
                      </svg>
                      Run Detection
                    </button>
                    <button className="btn btn-outline" onClick={handleReset} style={{ marginTop: '12px' }}>
                      Remove
                    </button>
                  </div>
                )}
              </div>
            </div>
          )}

          {upload.error && (
            <div className={styles.errorBar}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="12" cy="12" r="10" /><line x1="12" y1="8" x2="12" y2="12" /><line x1="12" y1="16" x2="12.01" y2="16" />
              </svg>
              {upload.error}
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
