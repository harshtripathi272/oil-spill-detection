import os
import json
import logging
from typing import Any, Dict, List, Optional

import requests
from app.config import settings

logger = logging.getLogger(__name__)

class SupabaseStore:
    def __init__(self):
        self.supabase_url = settings.supabase_url or os.getenv("SUPABASE_URL", "")
        self.supabase_service_key = settings.supabase_service_key or os.getenv("SUPABASE_SERVICE_KEY", "")
        self.headers = {
            "apikey": self.supabase_service_key,
            "Authorization": f"Bearer {self.supabase_service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        self.is_configured = bool(
            self.supabase_url and
            self.supabase_service_key and
            "xyzcompany" not in self.supabase_url and
            "..." not in self.supabase_service_key
        )

        if not self.is_configured:
            logger.warning("Supabase not configured or has placeholder values. Supabase APIs will be disabled.")

    def table_url(self, table: str) -> str:
        return f"{self.supabase_url.rstrip('/')}/rest/v1/{table}"

    def request(
        self,
        method: str,
        table: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> requests.Response:
        if not self.is_configured:
            raise RuntimeError("Supabase is not configured")

        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)

        response = requests.request(
            method,
            self.table_url(table),
            headers=request_headers,
            params=params,
            json=json_body,
            timeout=timeout,
        )
        response.raise_for_status()
        return response

    def select(
        self,
        table: str,
        select: str = "*",
        filters: Optional[Dict[str, str]] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"select": select}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self.request("GET", table, params=params)
        return response.json()

    def upsert(
        self,
        table: str,
        payload: Any,
        on_conflict: Optional[str] = None,
        resolve_duplicates: bool = True,
    ) -> Any:
        params: Dict[str, str] = {}
        headers: Dict[str, str] = {}

        if on_conflict:
            params["on_conflict"] = on_conflict
            resolution = "merge-duplicates" if resolve_duplicates else "ignore"
            headers["Prefer"] = f"return=representation,resolution={resolution}"
        else:
            headers["Prefer"] = "return=representation"

        response = self.request("POST", table, params=params, json_body=payload, headers=headers)
        return response.json()

    def update(
        self,
        table: str,
        payload: Any,
        filters: Dict[str, str],
    ) -> Any:
        response = self.request("PATCH", table, params=filters, json_body=payload)
        try:
            return response.json()
        except ValueError:
            return {}

    def delete(self, table: str, filters: Dict[str, str]) -> Any:
        response = self.request("DELETE", table, params=filters)
        try:
            return response.json()
        except ValueError:
            return {}

    def normalize_incident(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        metadata = raw.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}
        metadata = metadata or {}

        return {
            "id": raw.get("incident_id") or raw.get("id"),
            "status": raw.get("state") or raw.get("status"),
            "created_at": raw.get("created_at"),
            "updated_at": raw.get("updated_at"),
            "metadata": metadata,
            "latitude": raw.get("latitude") or metadata.get("latitude"),
            "longitude": raw.get("longitude") or metadata.get("longitude"),
            "confidence_score": raw.get("confidence_score") or metadata.get("confidence_score"),
            "bbox_coordinates": raw.get("bbox_coordinates") or metadata.get("bbox_coordinates"),
            "model_version": raw.get("model_version") or metadata.get("model_version"),
            "processing_time": raw.get("processing_time") or metadata.get("processing_time"),
            "sar_image_path": raw.get("sar_image_path") or metadata.get("sar_image_path"),
            "processed_image_path": raw.get("processed_image_path") or metadata.get("processed_image_path"),
        }

    def get_incident_by_id(self, incident_id: str) -> Optional[Dict[str, Any]]:
        results = self.select(
            "incidents",
            select="*",
            filters={"incident_id": f"eq.{incident_id}"},
            limit=1,
        )
        if results:
            return self.normalize_incident(results[0])
        return None

    def list_incidents(
        self,
        status: Optional[str] = None,
        min_confidence: Optional[float] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        filters: Dict[str, str] = {}
        if status:
            filters["state"] = f"ilike.{status}"
        if min_confidence is not None:
            filters["confidence_score"] = f"gte.{min_confidence}"

        rows = self.select(
            "incidents",
            select="*",
            filters=filters,
            limit=limit,
            offset=offset,
        )
        return [self.normalize_incident(raw) for raw in rows]

    def upsert_incident(self, incident_id: str, state: str, metadata: Optional[Dict[str, Any]] = None) -> Any:
        existing = self.get_incident_by_id(incident_id)
        merged_metadata = dict(existing.get("metadata", {})) if existing else {}
        if metadata:
            merged_metadata.update(metadata)

        payload = {
            "incident_id": incident_id,
            "state": state,
            "updated_at": metadata.get("updated_at") if metadata and metadata.get("updated_at") else None,
            "created_at": existing.get("created_at") if existing else None,
            "metadata": merged_metadata,
        }
        if payload["created_at"] is None:
            payload["created_at"] = metadata.get("created_at") if metadata else None
        payload["created_at"] = payload["created_at"] or metadata.get("created_at") if metadata else payload["created_at"]

        # Remove None values for Supabase insert
        payload = {k: v for k, v in payload.items() if v is not None}
        return self.upsert("incidents", payload, on_conflict="incident_id")

    def count_incidents(self, status: Optional[str] = None) -> int:
        rows = self.list_incidents(status=status, limit=10000)
        return len(rows)

    def list_incidents_by_state(self, state: str) -> List[str]:
        rows = self.select("incidents", select="incident_id", filters={"state": f"eq.{state}"}, limit=10000)
        return [item["incident_id"] for item in rows]


supabase_store = SupabaseStore()