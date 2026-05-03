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
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../infra/incidents.db"))
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_sqlite()

        if not self.is_supabase_configured:
            logger.info("Supabase not configured. Using local SQLite store at %s", self.db_path)

    def _init_sqlite(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    incident_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    metadata TEXT
                )
            """)

    def get_incident_state(self, incident_id: str) -> dict:
        if self.is_supabase_configured:
            try:
                url = f"{self.base_api}?incident_id=eq.{incident_id}"
                response = requests.get(url, headers=self.headers, timeout=5)
                response.raise_for_status()
                data = response.json()
                if data:
                    item = data[0]
                    return {
                        "state": item.get("state"),
                        "created_at": item.get("created_at"),
                        "updated_at": item.get("updated_at"),
                        "metadata": item.get("metadata", {})
                    }
            except Exception as e:
                logger.warning("Supabase get failed, trying SQLite: %s", e)

        # Fallback to SQLite
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute("SELECT * FROM incidents WHERE incident_id = ?", (incident_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "state": row["state"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                        "metadata": json.loads(row["metadata"] or "{}")
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

        # Attempt Supabase
        if self.is_supabase_configured:
            try:
                payload = {
                    "incident_id": incident_id,
                    "state": new_state,
                    "updated_at": current_time,
                    "created_at": created_at,
                    "metadata": merged_metadata
                }
                headers = dict(self.headers)
                headers["Prefer"] = "return=representation,resolution=merge-duplicates"
                response = requests.post(self.base_api, headers=headers, json=payload, timeout=5)
                response.raise_for_status()
                return # Success
            except Exception as e:
                logger.warning("Supabase update failed, falling back to SQLite: %s", e)

        # Local Persistence (always or as fallback)
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO incidents (incident_id, state, created_at, updated_at, metadata)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(incident_id) DO UPDATE SET
                        state=excluded.state,
                        updated_at=excluded.updated_at,
                        metadata=excluded.metadata
                """, (incident_id, new_state, created_at, current_time, json.dumps(merged_metadata)))
        except Exception as e:
            logger.error("Failed to update incident in SQLite: %s", e)

    def list_incidents_by_state(self, state: str) -> list:
        if self.is_supabase_configured:
            try:
                url = f"{self.base_api}?state=eq.{state}&select=incident_id"
                response = requests.get(url, headers=self.headers, timeout=5)
                response.raise_for_status()
                return [item["incident_id"] for item in response.json()]
            except Exception:
                pass

        # Fallback to SQLite
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.execute("SELECT incident_id FROM incidents WHERE state = ?", (state,))
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error("Failed to list incidents from SQLite: %s", e)
            return []
