"""
Sentinel Search Operator.

This operator interacts with the Sentinel satellite catalog to find scenes
matching the specific spatial and temporal criteria derived from an AIS event.
It outputs metadata required for subsequent download and processing steps.
"""

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import logging
from datetime import datetime, timedelta

import asf_search as asf

class SentinelSearchOperator(BaseOperator):
    """
    Operator to search for Sentinel-1 SAR products.
    
    Pushes the list of found product IDs/metadata to XCom.
    """

    template_fields = ("roi_wkt", "search_start", "search_end", "event_time")

    def __init__(
        self,
        roi_wkt: str,
        search_start: str,
        search_end: str,
        event_time: str,
        max_results: int = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.roi_wkt = roi_wkt
        self.search_start = search_start
        self.search_end = search_end
        self.event_time = event_time
        self.max_results = max_results

    def execute(self, context):
        logging.info(f"🔍 Searching Sentinel-1 products for ROI: {self.roi_wkt}")
        logging.info(f"📅 Initial window: {self.search_start} to {self.search_end}")

        if not self.roi_wkt or not self.search_start or not self.search_end or not self.event_time:
            raise AirflowException("roi_wkt, search_start, search_end, and event_time are required for Sentinel search.")

        try:
            event_dt = self._parse_event_time(self.event_time)
        except Exception as exc:
            raise AirflowException(f"Invalid event_time format: {self.event_time}. Expected ISO-8601.") from exc

        search_windows = [
            ("±24h", self.search_start, self.search_end),
            ("±72h",
                (event_dt - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                (event_dt + timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%SZ')),
            ("preceding 7d",
                (event_dt - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                event_dt.strftime('%Y-%m-%dT%H:%M:%SZ')),
        ]

        for label, start_window, end_window in search_windows:
            logging.info(f"🔁 Searching window {label}: {start_window} to {end_window}")
            try:
                found_products = self._search_products(start_window, end_window)
            except Exception as exc:
                logging.error(f"❌ ASF search failed for window {label}: {exc}")
                raise AirflowException(f"Sentinel search failed for window {label}: {exc}") from exc

            if found_products:
                logging.info(f"✅ Found {len(found_products)} products in window {label}")
                for i, product in enumerate(found_products[:5]):
                    logging.info(
                        f"   Product {i+1} ({label}): {product.get('fileName', 'unknown')} - {product.get('startTime', 'unknown')}"
                    )
                if len(found_products) > 5:
                    logging.info(f"   ... and {len(found_products) - 5} more products")
                
                # Apply result limit if specified
                results_to_return = found_products
                if self.max_results is not None:
                    results_to_return = found_products[:self.max_results]
                    if len(found_products) > self.max_results:
                        logging.info(f"⚠️ Limiting search results to {self.max_results} of {len(found_products)} products")
                
                return results_to_return

        error_msg = (
            f"❌ No Sentinel-1 products found for ROI={self.roi_wkt} "
            f"after checking windows: {[label for label, _, _ in search_windows]}"
        )
        logging.error(error_msg)
        raise AirflowException(error_msg)

    def _search_products(self, start: str, end: str):
        """Query ASF for Sentinel-1 GRD products in the requested window and ROI."""
        query_kwargs = {
            "platform": ["SENTINEL-1A", "SENTINEL-1B"],
            # "processingLevel": "GRD",
            "beamMode": "IW",
            "polarization": ["VV", "VH"], 
            "intersectsWith": self.roi_wkt,
            "start": start,
            "end": end,
        }
        logging.debug("ASF search query parameters: %s", query_kwargs)

        results = asf.search(**query_kwargs)
        if results is None:
            logging.debug("ASF returned no results for query %s", query_kwargs)
            return []

        formatted = []
        for product in list(results):
            file_name = self._extract_product_field(product, "fileName") or self._extract_product_field(product, "fileID")
            start_time = self._extract_product_field(product, "startTime")
            url = self._extract_product_field(product, "url")
            product_id = (
                self._extract_product_field(product, "sceneName")
                or self._extract_product_field(product, "fileID")
                or file_name
            )

            formatted_product = {
                "product_id": product_id,
                "filename": file_name,
                "fileName": file_name,
                "url": url,
                "startTime": str(start_time) if start_time is not None else None,
                "acquisition_date": str(start_time) if start_time is not None else None,
                "platform": "SENTINEL-1",
                "processingLevel": "GRD",
                "title": file_name,
            }
            logging.debug("ASF product mapped for XCom: %s", formatted_product)
            formatted.append(formatted_product)

        return formatted

    @staticmethod
    def _extract_product_field(product, field_name: str):
        """Read a field from ASFProduct-like objects with best-effort compatibility."""
        if isinstance(product, dict):
            return product.get(field_name)

        properties = getattr(product, "properties", None)
        if isinstance(properties, dict) and field_name in properties:
            return properties.get(field_name)

        direct = getattr(product, field_name, None)
        if direct is not None:
            return direct

        if hasattr(product, "geojson") and callable(product.geojson):
            try:
                geojson_obj = product.geojson()
                if isinstance(geojson_obj, dict):
                    props = geojson_obj.get("properties", {})
                    if isinstance(props, dict):
                        return props.get(field_name)
            except Exception:
                return None

        return None

    @staticmethod
    def _parse_event_time(event_time: str) -> datetime:
        """Parse event time string, handling various formats including Z suffix."""
        # Handle Z suffix (UTC) by converting to +00:00
        normalized = event_time.replace('Z', '+00:00')
        return datetime.fromisoformat(normalized)
