'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { MapContainer, TileLayer, useMap } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import 'leaflet.heat';
import { fetchIncidents, resolveIncidentAssetUrl } from '@/lib/api';
import styles from './IncidentMap.module.css';

/** One request powers markers + heatmap (no duplicate incident list APIs). */
const MAP_FETCH_LIMIT = 1500;

export type MapIncident = {
  id: string;
  latitude: number;
  longitude: number;
  confidence_score?: number | null;
  detection_time: string;
  status: string;
  extra_metadata?: Record<string, unknown> | null;
  sar_image_path?: string | null;
  processed_image_path?: string | null;
  model_version?: string | null;
  processing_time?: number | null;
};

function isResolvedStatus(status: string): boolean {
  return status.trim().toLowerCase() === 'resolved';
}

function isDetectedPulseStatus(status: string): boolean {
  return status.trim().toLowerCase() === 'detected';
}

/** In-progress / verified spill — visible but not the same as initial “detected” pulse */
function isActiveNonDetectedStatus(status: string): boolean {
  const l = status.trim().toLowerCase();
  return (
    l === 'confirmed' ||
    l === 'pending_imagery' ||
    l === 'imagery_available' ||
    l === 'downloading' ||
    l === 'processing' ||
    l === 'verified' // Support legacy just in case
  );
}

function makeMarkerIcon(inc: MapIncident): L.DivIcon {
  const resolved = isResolvedStatus(inc.status);
  const pulse = isDetectedPulseStatus(inc.status) && !resolved;
  const active = isActiveNonDetectedStatus(inc.status) && !resolved;

  let inner: string;
  if (resolved) {
    inner = `<div class="${styles.dotGreen}"></div>`;
  } else if (pulse) {
    inner = `<div class="${styles.dotPulse}"></div>`;
  } else if (active) {
    inner = `<div class="${styles.dotOther}" style="background:#f59e0b;width:12px;height:12px;border:2px solid rgba(255,255,255,0.35);border-radius:50%"></div>`;
  } else if (statusFalsePositive(inc.status)) {
    inner = `<div class="${styles.dotOther}" style="background:#6b7280"></div>`;
  } else {
    inner = `<div class="${styles.dotOther}" style="background:#94a3b8"></div>`;
  }

  return L.divIcon({
    className: styles.divIconRoot,
    html: `<div class="${styles.markerAnchor}">${inner}</div>`,
    iconSize: [22, 22],
    iconAnchor: [11, 11],
  });
}

function statusFalsePositive(status: string): boolean {
  const l = status.trim().toLowerCase();
  const u = status.trim().toUpperCase();
  return l === 'false_positive' || u === 'FALSE_POSITIVE';
}

type HeatLeaflet = typeof L & {
  heatLayer: (
    latlngs: [number, number, number][],
    options?: {
      radius?: number;
      blur?: number;
      maxZoom?: number;
      minOpacity?: number;
      max?: number;
      gradient?: Record<number, string>;
    }
  ) => L.Layer;
};

function MapEffects({
  incidents,
  heatmapOn,
  onPick,
}: {
  incidents: MapIncident[];
  heatmapOn: boolean;
  onPick: (inc: MapIncident) => void;
}) {
  const map = useMap();
  const onPickRef = useRef(onPick);
  onPickRef.current = onPick;
  const didFitRef = useRef(false);

  useEffect(() => {
    const valid = incidents.filter(
      (i) =>
        Number.isFinite(i.latitude) &&
        Number.isFinite(i.longitude) &&
        i.latitude >= -90 &&
        i.latitude <= 90 &&
        i.longitude >= -180 &&
        i.longitude <= 180
    );

    const group = L.layerGroup();
    for (const inc of valid) {
      const icon = makeMarkerIcon(inc);
      const m = L.marker([inc.latitude, inc.longitude], { icon });
      m.on('click', (e: L.LeafletMouseEvent) => {
        L.DomEvent.stopPropagation(e);
        onPickRef.current(inc);
      });
      m.addTo(group);
    }
    group.addTo(map);

    if (valid.length > 0 && !didFitRef.current) {
      const bounds = L.latLngBounds(valid.map((i) => [i.latitude, i.longitude] as L.LatLngTuple));
      if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [48, 48], maxZoom: 12, animate: false });
      }
      didFitRef.current = true;
    }

    return () => {
      map.removeLayer(group);
    };
  }, [map, incidents]);

  useEffect(() => {
    let layer: L.Layer | null = null;
    if (!heatmapOn) {
      return () => {
        if (layer) map.removeLayer(layer);
      };
    }

    const valid = incidents.filter(
      (i) =>
        Number.isFinite(i.latitude) &&
        Number.isFinite(i.longitude) &&
        i.latitude >= -90 &&
        i.latitude <= 90 &&
        i.longitude >= -180 &&
        i.longitude <= 180
    );
    if (!valid.length) {
      return () => {
        if (layer) map.removeLayer(layer);
      };
    }

    const pts: [number, number, number][] = valid.map((i) => [i.latitude, i.longitude, 0.5]);
    const Lheat = L as HeatLeaflet;
    layer = Lheat.heatLayer(pts, {
      radius: 24,
      blur: 18,
      maxZoom: 15,
      minOpacity: 0.28,
      max: 0.85,
      gradient: {
        0.35: '#1e3a5f',
        0.55: '#f59e0b',
        0.75: '#ef4444',
        1.0: '#dc2626',
      },
    });
    layer.addTo(map);

    return () => {
      if (layer) map.removeLayer(layer);
    };
  }, [map, incidents, heatmapOn]);

  return null;
}

type DetailTab = 'incident' | 'vessel' | 'timeline' | 'evidence';

const DETAIL_TABS: { id: DetailTab; label: string }[] = [
  { id: 'incident', label: 'Incident' },
  { id: 'vessel', label: 'Vessel' },
  { id: 'timeline', label: 'Timeline' },
  { id: 'evidence', label: 'Evidence' },
];

function formatUtc(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${d.toISOString().replace('T', ' ').slice(0, 19)} UTC`;
}

function VesselBlock({ meta }: { meta: Record<string, unknown> | null | undefined }) {
  if (!meta || typeof meta !== 'object') {
    return <p className={styles.panelEmpty}>No vessel metadata on this incident.</p>;
  }
  const vesselId = meta.vessel_id ?? meta.vesselId;
  const mmsi = meta.mmsi ?? meta.MMSI;
  const name = meta.vessel_name ?? meta.name ?? meta.ship_name;
  
  const features = (meta.raw as any)?.features || {};
  const modelMeta = (meta.model as any)?.metadata || {};

  const rows: { k: string; v: string }[] = [];
  if (vesselId != null) rows.push({ k: 'Vessel ID', v: String(vesselId) });
  if (mmsi != null) rows.push({ k: 'MMSI', v: String(mmsi) });
  if (name != null) rows.push({ k: 'Name', v: String(name) });
  if (features.vessel_type) rows.push({ k: 'Type', v: String(features.vessel_type) });
  if (features.speed_knots != null) rows.push({ k: 'Speed', v: `${Number(features.speed_knots).toFixed(1)} kn` });
  if (features.heading_deg != null) rows.push({ k: 'Heading', v: `${Number(features.heading_deg).toFixed(1)}°` });
  if (modelMeta.global_score != null) rows.push({ k: 'Risk Score', v: Number(modelMeta.global_score).toFixed(2) });

  if (!rows.length) {
    return (
      <pre style={{ fontSize: 11, color: 'var(--text-secondary)', whiteSpace: 'pre-wrap', margin: 0 }}>
        {JSON.stringify(meta, null, 2)}
      </pre>
    );
  }
  return (
    <div className={styles.vesselGrid}>
      {rows.map((r) => (
        <div key={r.k} className={styles.vesselRow}>
          <span className={styles.vesselKey}>{r.k}</span>
          <span className={styles.vesselVal}>{r.v}</span>
        </div>
      ))}
    </div>
  );
}

function IncidentLifecycleTimeline({ status }: { status: string }) {
  const steps = ['DETECTED', 'CONFIRMED', 'RESOLVED'];
  let currentIdx = 0;
  const s = status.toLowerCase();
  if (s === 'detected') currentIdx = 0;
  else if (s === 'confirmed' || s === 'processing' || s === 'downloading' || s === 'verified') currentIdx = 1;
  else if (s === 'resolved') currentIdx = 2;
  else if (s === 'false_positive' || s === 'failed') currentIdx = -1; // special state

  return (
    <div className={styles.lifecycleStepper}>
      {steps.map((step, i) => {
        let stateClass = styles.stepPending;
        if (currentIdx === -1) {
          stateClass = styles.stepFailed;
        } else if (i < currentIdx) {
          stateClass = styles.stepComplete;
        } else if (i === currentIdx) {
          stateClass = styles.stepActive;
        }
        
        return (
          <div key={step} className={`${styles.step} ${stateClass}`}>
            <div className={styles.stepDot} />
            <span className={styles.stepLabel}>{step}</span>
            {i < steps.length - 1 && <div className={styles.stepLine} />}
          </div>
        );
      })}
    </div>
  );
}

export default function IncidentMap() {
  const [selected, setSelected] = useState<MapIncident | null>(null);
  const [heatmapOn, setHeatmapOn] = useState(true);
  const [panelExpanded, setPanelExpanded] = useState(true);
  const [detailTab, setDetailTab] = useState<DetailTab>('incident');

  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['incidents', 'map', MAP_FETCH_LIMIT],
    queryFn: () => fetchIncidents({ limit: MAP_FETCH_LIMIT }),
    staleTime: 45_000,
  });

  const incidents = useMemo(() => (Array.isArray(data) ? (data as MapIncident[]) : []), [data]);

  const onPick = useCallback((inc: MapIncident) => {
    setSelected(inc);
  }, []);

  const sarUrl = selected ? resolveIncidentAssetUrl(selected.sar_image_path, 'sar') : null;
  const predUrl = selected ? resolveIncidentAssetUrl(selected.processed_image_path, 'prediction') : null;

  useEffect(() => {
    if (selected) setDetailTab('incident');
  }, [selected?.id]);

  return (
    <div className={styles.shell}>
      <div className={styles.mapPane}>
        {error ? (
          <div className={styles.errorBanner} role="alert">
            {(error as Error).message || 'Could not load incidents'}
          </div>
        ) : null}

        <MapContainer
          center={[15, 0]}
          zoom={2}
          style={{ height: '100%', width: '100%' }}
          scrollWheelZoom
          worldCopyJump
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            subdomains="abcd"
            maxZoom={20}
          />
          {!isLoading && incidents.length > 0 ? (
            <MapEffects incidents={incidents} heatmapOn={heatmapOn} onPick={onPick} />
          ) : null}
        </MapContainer>

        <div className={styles.toolbar}>
          <div className={styles.toolbarInner}>
            <label>
              <input
                type="checkbox"
                checked={heatmapOn}
                onChange={(e) => setHeatmapOn(e.target.checked)}
              />
              Heatmap
            </label>
            {isFetching ? <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Updating…</span> : null}
          </div>
        </div>

        <div className={styles.legend}>
          <strong style={{ color: 'var(--text-primary)' }}>Legend</strong>
          <div className={styles.legendRow}>
            <span className={styles.dotPulse} />
            Detected
          </div>
          <div className={styles.legendRow}>
            <span className={styles.dotGreen} />
            Resolved
          </div>
          <div className={styles.legendRow}>
            <span
              className={styles.dotOther}
              style={{
                background: '#f59e0b',
                width: 12,
                height: 12,
                borderRadius: '50%',
                border: '2px solid rgba(255,255,255,0.35)',
              }}
            />
            Confirmed / in progress
          </div>
          <div className={styles.legendRow} style={{ marginTop: 8, fontSize: 10, opacity: 0.85 }}>
            Heatmap uses the same incident payload (no extra list API).
          </div>
        </div>

        {isLoading ? (
          <div
            style={{
              position: 'absolute',
              inset: 0,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              background: 'rgba(11, 17, 32, 0.65)',
              zIndex: 500,
              color: 'var(--text-muted)',
              fontSize: 14,
            }}
          >
            Loading map data…
          </div>
        ) : null}

        {!isLoading && incidents.length === 0 ? (
          <div
            style={{
              position: 'absolute',
              inset: 0,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 400,
              color: 'var(--text-muted)',
              fontSize: 14,
            }}
          >
            No incidents returned. Seed the API or widen filters.
          </div>
        ) : null}
      </div>

      <div
        className={`${styles.sideColumn} ${
          panelExpanded ? styles.sideColumnExpanded : styles.sideColumnCollapsed
        }`}
      >
        {!panelExpanded ? (
          <button
            type="button"
            className={styles.expandRail}
            onClick={() => setPanelExpanded(true)}
            title="Show details"
            aria-label="Expand details panel"
          >
            <ChevronLeft size={18} aria-hidden />
          </button>
        ) : (
          <aside className={styles.panel}>
            <div className={styles.panelTopBar}>
              <span className={styles.panelId} title={selected?.id ?? undefined}>
                {selected ? `INC-${selected.id.split('-')[1] || selected.id.substring(0,8).toUpperCase()}` : 'Details'}
              </span>
              <button
                type="button"
                className={styles.collapseBtn}
                onClick={() => setPanelExpanded(false)}
                title="Hide panel"
                aria-label="Collapse details panel"
              >
                <ChevronRight size={18} aria-hidden />
              </button>
            </div>

            {!selected ? (
              <div className={styles.panelEmpty}>
                Select a marker for incident, vessel, timeline, and evidence tabs — data from{' '}
                <code style={{ fontSize: 10 }}>/api/v1/incidents</code>.
              </div>
            ) : (
              <>
                <div className={styles.tabList} role="tablist" aria-label="Incident detail sections">
                  {DETAIL_TABS.map((t) => (
                    <button
                      key={t.id}
                      type="button"
                      role="tab"
                      id={`tab-${t.id}`}
                      aria-selected={detailTab === t.id}
                      aria-controls={`panel-${t.id}`}
                      className={`${styles.tabBtn} ${detailTab === t.id ? styles.tabBtnActive : ''}`}
                      onClick={() => setDetailTab(t.id)}
                    >
                      {t.label}
                    </button>
                  ))}
                </div>

                <div
                  className={styles.panelBody}
                  role="tabpanel"
                  id={`panel-${detailTab}`}
                  aria-labelledby={`tab-${detailTab}`}
                >
                  {detailTab === 'incident' ? (
                    <>
                      <div className={styles.panelHeader} style={{ paddingTop: 0, borderBottom: 'none' }}>
                        <div className={styles.panelMeta}>
                          <span className={`status-badge ${selected.status?.toLowerCase().replace(/\s+/g, '_')}`}>
                            {selected.status}
                          </span>
                          {' · '}
                          {Number.isFinite(selected.confidence_score as number)
                            ? `${Math.round(((selected.confidence_score as number) || 0) * 100)}% conf`
                            : '—'}
                        </div>
                      </div>
                      
                      <IncidentLifecycleTimeline status={selected.status} />

                      <div className={styles.timelineList}>
                        <div className={styles.timelineRow}>
                          <div className={styles.timelineLabel}>Position</div>
                          <div className={styles.timelineValue}>
                            {selected.latitude?.toFixed(5)}°, {selected.longitude?.toFixed(5)}°
                          </div>
                        </div>
                        {selected.model_version ? (
                          <div className={styles.timelineRow}>
                            <div className={styles.timelineLabel}>Model</div>
                            <div className={styles.timelineValue}>{selected.model_version}</div>
                          </div>
                        ) : null}
                        {selected.extra_metadata?.vessel_id ? (
                          <div className={styles.timelineRow}>
                            <div className={styles.timelineLabel}>Vessel</div>
                            <div className={styles.timelineValue} style={{fontFamily: 'var(--font-mono)'}}>
                              {String(selected.extra_metadata.vessel_id)}
                            </div>
                          </div>
                        ) : null}
                        {selected.processing_time != null ? (
                          <div className={styles.timelineRow}>
                            <div className={styles.timelineLabel}>Processing</div>
                            <div className={styles.timelineValue}>{selected.processing_time.toFixed(2)}s</div>
                          </div>
                        ) : null}
                      </div>
                      <div className={styles.links}>
                        <a
                          className={styles.link}
                          href={`https://www.google.com/maps?q=${selected.latitude},${selected.longitude}`}
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          Google Maps
                        </a>
                        <a
                          className={styles.link}
                          href={`https://www.openstreetmap.org/?mlat=${selected.latitude}&mlon=${selected.longitude}#map=10/${selected.latitude}/${selected.longitude}`}
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          OSM
                        </a>
                      </div>
                    </>
                  ) : null}

                  {detailTab === 'vessel' ? <VesselBlock meta={selected.extra_metadata} /> : null}

                  {detailTab === 'timeline' ? (
                    <div className={styles.timelineList}>
                      <div className={styles.timelineRow}>
                        <div className={styles.timelineLabel}>Detection (UTC)</div>
                        <div className={styles.timelineValue}>
                          {selected.detection_time ? formatUtc(selected.detection_time) : '—'}
                        </div>
                      </div>
                      {selected.model_version ? (
                        <div className={styles.timelineRow}>
                          <div className={styles.timelineLabel}>Model version</div>
                          <div className={styles.timelineValue}>{selected.model_version}</div>
                        </div>
                      ) : null}
                      {selected.processing_time != null ? (
                        <div className={styles.timelineRow}>
                          <div className={styles.timelineLabel}>Processing time</div>
                          <div className={styles.timelineValue}>{selected.processing_time.toFixed(2)}s</div>
                        </div>
                      ) : null}
                      <p className={styles.monoMuted}>
                        DAG run history:{' '}
                        <code style={{ fontSize: 10 }}>/api/v1/incidents/:id/dag-runs</code> (optional follow-up).
                      </p>
                    </div>
                  ) : null}

                  {detailTab === 'evidence' ? (
                    <>
                      {sarUrl ? (
                        <div className={styles.imgBlock}>
                          <div className={styles.imgLabel}>SAR</div>
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img className={styles.img} src={sarUrl} alt="SAR" loading="lazy" />
                          <div className={styles.links}>
                            <a className={styles.link} href={sarUrl} target="_blank" rel="noopener noreferrer">
                              Open SAR
                            </a>
                          </div>
                        </div>
                      ) : (
                        <p className={styles.panelEmpty} style={{ marginTop: 0 }}>
                          No SAR path on record.
                        </p>
                      )}
                      {predUrl ? (
                        <div className={styles.imgBlock}>
                          <div className={styles.imgLabel}>Processed</div>
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img className={styles.img} src={predUrl} alt="Processed" loading="lazy" />
                          <div className={styles.links}>
                            <a className={styles.link} href={predUrl} target="_blank" rel="noopener noreferrer">
                              Open processed
                            </a>
                          </div>
                        </div>
                      ) : (
                        <p className={styles.monoMuted}>No processed image path.</p>
                      )}
                    </>
                  ) : null}
                </div>
              </>
            )}
          </aside>
        )}
      </div>
    </div>
  );
}
