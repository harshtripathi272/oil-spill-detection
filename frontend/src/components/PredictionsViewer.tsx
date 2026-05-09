'use client';

import React, { useEffect, useState } from 'react';
import { Image, RefreshCw, Eye } from 'lucide-react';
import styles from '@/app/page.module.css';
import { API_BASE, API_ROOT } from '@/lib/api';

interface Prediction {
  id?: number;
  incident_id?: string;
  image_path?: string;
  prediction_image_path?: string;
  prediction_image_url?: string;
  prediction?: string;
  confidence?: number;
  bbox_coordinates?: any;
  created_at?: string;
}

interface PredictionsViewerProps {
  maxImages?: number;
  refreshInterval?: number;
  predictions?: Prediction[];
}

export default function PredictionsViewer({ maxImages = 6, refreshInterval = 30000, predictions: externalPredictions }: PredictionsViewerProps) {
  const [predictions, setPredictions] = useState<Prediction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const getPredictionKey = (pred: Prediction, idx: number) => {
    // Ensure unique keys by combining multiple identifiers with index as final fallback
    const baseKey = (
      (pred.id != null ? `id:${pred.id}` : null) ??
      (pred.prediction_image_url ? `url:${pred.prediction_image_url}` : null) ??
      (pred.image_path ? `img:${pred.image_path}` : null) ??
      (pred.incident_id || pred.created_at ? `meta:${pred.incident_id ?? 'na'}:${pred.created_at ?? 'na'}` : null) ??
      `idx:${idx}`
    );
    // Add index to ensure uniqueness even if base key is the same
    return `${baseKey}:${idx}`;
  };

  const fetchPredictionFiles = async () => {
    const response = await fetch(`${API_BASE}/pipeline/prediction-files?limit=20`);
    if (!response.ok) throw new Error('Failed to fetch fallback prediction files');
    const data = await response.json();
    return data.images?.map((item: any) => ({
      id: item.prediction_id,
      prediction_image_url: item.url?.startsWith('/') ? `${API_ROOT}${item.url}` : item.url,
      prediction: item.prediction ?? 'unknown',
      confidence: item.confidence ?? 0,
      incident_id: item.incident_id,
      image_path: item.source_image,
      created_at: item.modified,
    })) || [];
  };

  const fetchPredictions = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE}/predictions?limit=20&sort=-created_at`);
      if (!response.ok) throw new Error('Failed to fetch predictions');
      const data = await response.json();
      const results = data.slice(0, maxImages);
      if (results.length === 0) {
        const fallback = await fetchPredictionFiles();
        setPredictions(fallback.slice(0, maxImages));
      } else {
        setPredictions(results);
      }
      setError(null);
    } catch (err) {
      try {
        const fallback = await fetchPredictionFiles();
        setPredictions(fallback.slice(0, maxImages));
        setError(null);
      } catch (fallbackErr) {
        setError(fallbackErr instanceof Error ? fallbackErr.message : 'Unknown error');
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (externalPredictions && externalPredictions.length > 0) {
      setPredictions(externalPredictions.slice(0, maxImages));
      setLoading(false);
      return;
    }

    fetchPredictions();
    const interval = setInterval(fetchPredictions, refreshInterval);
    return () => clearInterval(interval);
  }, [refreshInterval, maxImages, externalPredictions]);

  const getPredictionColor = (prediction?: string) => {
    if (!prediction || prediction === 'unknown') return 'var(--text-muted)';
    return prediction === 'oil_spill' ? 'var(--error)' : 'var(--success)';
  };

  const getConfidenceColor = (confidence?: number) => {
    if (confidence == null) return 'var(--text-secondary)';
    if (confidence >= 0.8) return 'var(--success)';
    if (confidence >= 0.6) return 'var(--warning)';
    return 'var(--error)';
  };

  if (error) {
    return (
      <div className={`${styles.chartCard} card`}>
        <div className={styles.cardTitle}>
          <Image size={16} style={{ marginRight: 8 }} />
          Recent Predictions
        </div>
        <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--error)' }}>
          Error loading predictions: {error}
        </div>
      </div>
    );
  }

  return (
    <div className={`${styles.chartCard} card`}>
      <div className={styles.cardTitle}>
        <Image size={16} style={{ marginRight: 8 }} />
        Recent Predictions
        <button
          onClick={fetchPredictions}
          disabled={loading}
          style={{
            marginLeft: 'auto',
            background: 'none',
            border: 'none',
            cursor: loading ? 'not-allowed' : 'pointer',
            color: 'var(--text-secondary)',
            padding: '4px',
            borderRadius: '4px'
          }}
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
        </button>
      </div>

      {predictions.length === 0 ? (
        <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-muted)' }}>
          {loading ? 'Loading predictions...' : 'No predictions available'}
        </div>
      ) : (
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
          gap: '1rem',
          padding: '1rem'
        }}>
          {predictions.map((pred, idx) => (
            <div
              key={getPredictionKey(pred, idx)}
              style={{
                border: '1px solid var(--border-primary)',
                borderRadius: '8px',
                overflow: 'hidden',
                background: 'var(--bg-secondary)'
              }}
            >
              {/* Prediction Image */}
              <div style={{ position: 'relative', height: '120px', background: 'var(--bg-primary)' }}>
                {(pred.prediction_image_path || pred.prediction_image_url) ? (
                  <img
                    src={pred.prediction_image_url ?? `${API_ROOT}/api/v1/predictions/${pred.id}/image`}
                    alt={`Prediction ${pred.id ?? pred.image_path ?? pred.prediction_image_url}`}
                    style={{
                      width: '100%',
                      height: '100%',
                      objectFit: 'cover'
                    }}
                    onError={(e) => {
                      e.currentTarget.style.display = 'none';
                      e.currentTarget.nextElementSibling!.style.display = 'flex';
                    }}
                  />
                ) : null}
                <div
                  style={{
                    display: pred.prediction_image_path || pred.prediction_image_url ? 'none' : 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    height: '100%',
                    color: 'var(--text-muted)',
                    fontSize: '12px'
                  }}
                >
                  <Eye size={24} />
                  <span style={{ marginLeft: '8px' }}>No image</span>
                </div>

                {/* Prediction Badge */}
                <div
                  style={{
                    position: 'absolute',
                    top: '8px',
                    right: '8px',
                    background: getPredictionColor(pred.prediction),
                    color: 'white',
                    padding: '2px 6px',
                    borderRadius: '4px',
                    fontSize: '10px',
                    fontWeight: 'bold'
                  }}
                >
                  {(pred.prediction ?? 'unknown').replace('_', ' ').toUpperCase()}
                </div>
              </div>

              {/* Prediction Details */}
              <div style={{ padding: '12px' }}>
                <div style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  marginBottom: '8px'
                }}>
                  <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
                    ID: {pred.id ?? 'N/A'}
                  </span>
                  <span style={{
                    fontSize: '12px',
                    color: getConfidenceColor(pred.confidence),
                    fontWeight: 'bold'
                  }}>
                    {pred.confidence != null ? `${(pred.confidence * 100).toFixed(1)}%` : '--'}
                  </span>
                </div>

                {pred.incident_id && (
                  <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginBottom: '4px' }}>
                    Incident: {pred.incident_id}
                  </div>
                )}

                <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                  {pred.created_at ? new Date(pred.created_at).toLocaleString() : 'Unknown time'}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}