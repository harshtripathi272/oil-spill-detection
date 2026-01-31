"""
State Store Utility Module.

This module provides a mechanism to persist and retrieve the processing state of
oil spill incidents across different Airflow DAG runs. It ensures that incidents
move through valid state transitions (e.g., detected -> pending -> processing -> verified)
and allows for idempotent processing.
"""

import json
import os
from datetime import datetime
from threading import Lock

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
    A simple file-based state store for managing incident lifecycles.
    
    In a production environment, this should be replaced by a database 
    (e.g., PostgreSQL, DynamoDB) or the Airflow XCom/Variable backend 
    if appropriate for smaller payloads.
    """
    
    def __init__(self, storage_path: str = "/tmp/incident_state.json"):
        self.storage_path = storage_path
        self._lock = Lock()
        self._ensure_storage()

    def _ensure_storage(self):
        """Ensures the storage file exists."""
        if not os.path.exists(self.storage_path):
            with open(self.storage_path, "w") as f:
                json.dump({}, f)

    def _load_state(self) -> dict:
        """Loads the current state from disk."""
        try:
            with open(self.storage_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}

    def _save_state(self, state: dict):
        """Persists the state to disk."""
        with open(self.storage_path, "w") as f:
            json.dump(state, f, indent=4)

    def get_incident_state(self, incident_id: str) -> dict:
        """
        Retrieves the state and metadata for a given incident.
        
        Args:
            incident_id: Unique identifier for the incident.
            
        Returns:
            Dict containing state and metadata, or None if not found.
        """
        with self._lock:
            data = self._load_state()
            return data.get(incident_id)

    def update_incident_state(self, incident_id: str, new_state: str, metadata: dict = None):
        """
        Updates the state of an incident.
        
        Args:
            incident_id: Unique identifier for the incident.
            new_state: The new state string.
            metadata: data dict to merge/update with existing metadata.
        """
        with self._lock:
            data = self._load_state()
            if incident_id not in data:
                data[incident_id] = {
                    "state": new_state,
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                    "metadata": metadata or {}
                }
            else:
                current = data[incident_id]
                current["state"] = new_state
                current["updated_at"] = datetime.utcnow().isoformat()
                if metadata:
                    current["metadata"].update(metadata)
            
            self._save_state(data)

    def list_incidents_by_state(self, state: str) -> list:
        """Returns a list of incident IDs currently in the given state."""
        with self._lock:
            data = self._load_state()
            return [kid for kid, v in data.items() if v.get("state") == state]
