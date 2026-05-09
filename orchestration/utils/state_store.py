import os
import json
import logging
import sqlite3
from datetime import datetime, timezone
import requests

logger = logging.getLogger(__name__)

# Define valid states
STATE_DETECTED = "DETECTED"
STATE_PENDING_IMAGERY = "PENDING_IMAGERY"
STATE_IMAGERY_AVAILABLE = "IMAGERY_AVAILABLE"
STATE_DOWNLOADING = "DOWNLOADING"
STATE_PROCESSING = "PROCESSING"
STATE_VERIFIED = "VERIFIED"
STATE_FALSE_POSITIVE = "FALSE_POSITIVE"
STATE_FAILED = "FAILED"

class StateStore:
    """
    A state store for managing incident lifecycles.
    Uses Supabase by default, with an automatic local SQLite fallback.
    """
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL", "https://xyzcompany.supabase.co")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY", "")
        
        # Check if Supabase is enabled/configured
        self.is_supabase_configured = (
            self.supabase_url and 
            "xyzcompany" not in self.supabase_url and 
            self.supabase_key and 
            "..." not in self.supabase_key
        )
        
        self.headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        self.base_api = f"{self.supabase_url}/rest/v1/incidents"
        
        # Always initialize SQLite if needed as a fallback
        # Use the same database as the backend: oilspill.db
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../backend/oilspill.db"))
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_sqlite()

        if not self.is_supabase_configured:
            logger.info("Supabase not configured. Using unified SQLite store at %s", self.db_path)

    def _init_sqlite(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    id TEXT PRIMARY KEY,
                    latitude FLOAT,
                    longitude FLOAT,
                    confidence_score FLOAT,
                    detection_time TEXT,
                    status TEXT,
                    extra_metadata TEXT,
                    sar_image_path TEXT,
                    processed_image_path TEXT,
                    model_version TEXT,
                    processing_time FLOAT
                )
            """)

    def get_incident_state(self, incident_id: str) -> dict:
        if self.is_supabase_configured:
            try:
                url = f"{self.base_api}?id=eq.{incident_id}"
                response = requests.get(url, headers=self.headers, timeout=5)
                response.raise_for_status()
                data = response.json()
                if data:
                    item = data[0]
                    return {
                        "state": item.get("status"),
                        "created_at": item.get("detection_time"),
                        "updated_at": item.get("detection_time"),
                        "metadata": item.get("extra_metadata", {})
                    }
            except Exception as e:
                logger.warning("Supabase get failed, trying SQLite: %s", e)

        # Fallback to SQLite
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute("SELECT * FROM incidents WHERE id = ?", (incident_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "state": row["status"],
                        "created_at": row["detection_time"],
                        "updated_at": row["detection_time"],
                        "metadata": json.loads(row["extra_metadata"] or "{}")
                    }
        except Exception as e:
            logger.error("Failed to get incident from SQLite: %s", e)
        return None

    def update_incident_state(self, incident_id: str, new_state: str, metadata: dict = None):
        existing = self.get_incident_state(incident_id)
        current_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        
        merged_metadata = (existing.get("metadata", {}) if existing else {})
        if metadata:
            merged_metadata.update(metadata)
        
        created_at = (existing.get("created_at", current_time) if existing else current_time)
        
        # Extract fields for oilspill.db schema
        lat = merged_metadata.get('lat') or merged_metadata.get('latitude')
        lon = merged_metadata.get('lon') or merged_metadata.get('longitude')
        score = merged_metadata.get('score') or merged_metadata.get('confidence_score')

        # Attempt Supabase
        if self.is_supabase_configured:
            payload = {
                "id": incident_id,
                "status": new_state,
                "detection_time": created_at,
                "extra_metadata": merged_metadata,
                "latitude": lat,
                "longitude": lon,
                "confidence_score": score
            }

            try:
                # Prefer PATCH if the incident already exists
                patch_headers = dict(self.headers)
                patch_headers["Prefer"] = "return=representation"
                patch_params = {"id": f"eq.{incident_id}"}
                patch_response = requests.patch(self.base_api, headers=patch_headers, params=patch_params, json=payload, timeout=5)
                if patch_response.status_code in (200, 204):
                    return

                # If PATCH did not update an existing row, fallback to insert/upsert.
                post_headers = dict(self.headers)
                post_headers["Prefer"] = "return=representation,resolution=merge-duplicates"
                post_response = requests.post(self.base_api, headers=post_headers, json=payload, timeout=5)
                post_response.raise_for_status()
                return
            except requests.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else None
                if status_code == 409:
                    logger.warning("Supabase insert conflict for incident %s, retrying patch", incident_id)
                    try:
                        patch_response = requests.patch(self.base_api, headers=patch_headers, params=patch_params, json=payload, timeout=5)
                        patch_response.raise_for_status()
                        return
                    except Exception as patch_exc:
                        logger.error("Supabase patch retry failed for incident %s: %s", incident_id, patch_exc)
                logger.warning("Supabase update failed, falling back to SQLite: %s", e)
            except Exception as e:
                logger.warning("Supabase update failed, falling back to SQLite: %s", e)

        # Local Persistence - Unifying with oilspill.db schema
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO incidents (id, status, detection_time, extra_metadata, latitude, longitude, confidence_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        status=excluded.status,
                        detection_time=excluded.detection_time,
                        extra_metadata=excluded.extra_metadata,
                        latitude=COALESCE(excluded.latitude, latitude),
                        longitude=COALESCE(excluded.longitude, longitude),
                        confidence_score=COALESCE(excluded.confidence_score, confidence_score)
                """, (incident_id, new_state, created_at, json.dumps(merged_metadata), lat, lon, score))
        except Exception as e:
            logger.error("Failed to update incident in SQLite: %s", e)

    def list_incidents_by_state(self, state: str) -> list:
        if self.is_supabase_configured:
            try:
                url = f"{self.base_api}?status=eq.{state}&select=id"
                response = requests.get(url, headers=self.headers, timeout=5)
                response.raise_for_status()
                return [item["id"] for item in response.json()]
            except Exception:
                pass

        # Fallback to SQLite
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute("SELECT id FROM incidents WHERE status = ?", (state,))
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error("Failed to list incidents from SQLite: %s", e)
            return []
