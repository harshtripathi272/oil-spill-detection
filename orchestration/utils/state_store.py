import os
import json
import logging
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
    A state store for managing incident lifecycles backed by Supabase via REST API.
    """
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL", "https://xyzcompany.supabase.co")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
        self.headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        self.base_api = f"{self.supabase_url}/rest/v1/incidents"

    def get_incident_state(self, incident_id: str) -> dict:
        """
        Retrieves the state and metadata for a given incident.
        """
        try:
            url = f"{self.base_api}?incident_id=eq.{incident_id}"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data and len(data) > 0:
                item = data[0]
                return {
                    "state": item.get("state"),
                    "created_at": item.get("created_at"),
                    "updated_at": item.get("updated_at"),
                    "metadata": item.get("metadata", {})
                }
        except Exception as exc:
            logger.error("Failed to get incident state from Supabase for %s: %s", incident_id, exc)
        return None

    def update_incident_state(self, incident_id: str, new_state: str, metadata: dict = None):
        """
        Updates the state of an incident via Supabase upsert.
        """
        existing = self.get_incident_state(incident_id)
        current_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        
        payload = {
            "incident_id": incident_id,
            "state": new_state,
            "updated_at": current_time
        }

        if existing:
            merged_metadata = existing.get("metadata", {})
            if metadata:
                merged_metadata.update(metadata)
            payload["metadata"] = merged_metadata
            payload["created_at"] = existing.get("created_at", current_time)
        else:
            payload["metadata"] = metadata or {}
            payload["created_at"] = current_time

        try:
            headers = dict(self.headers)
            headers["Prefer"] = "return=representation,resolution=merge-duplicates"
            response = requests.post(self.base_api, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
        except Exception as exc:
            logger.error("Failed to update incident state in Supabase for %s: %s", incident_id, exc)

    def list_incidents_by_state(self, state: str) -> list:
        """Returns a list of incident IDs currently in the given state."""
        try:
            url = f"{self.base_api}?state=eq.{state}&select=incident_id"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return [item["incident_id"] for item in response.json()]
        except Exception as exc:
            logger.error("Failed to list incidents by state from Supabase for state %s: %s", state, exc)
            return []
