"""
Sentinel Search Operator.

This operator interacts with the Sentinel satellite catalog to find scenes
matching the specific spatial and temporal criteria derived from an AIS event.
It outputs metadata required for subsequent download and processing steps.
"""

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
import logging

import asf_search as asf

class SentinelSearchOperator(BaseOperator):
    """
    Operator to search for Sentinel-1 SAR products.
    
    Pushes the list of found product IDs/metadata to XCom.
    """

    template_fields = ("roi_wkt", "start_date", "end_date")

    @apply_defaults
    def __init__(
        self,
        roi_wkt: str,
        start_date: str,
        end_date: str,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.roi_wkt = roi_wkt
        self.start_date = start_date
        self.end_date = end_date

    def execute(self, context):
        logging.info(f"Searching Sentinel-1 products for ROI: {self.roi_wkt} from {self.start_date} to {self.end_date}")

        if not self.roi_wkt or not self.start_date or not self.end_date:
            raise AirflowException("roi_wkt, start_date, and end_date are required for Sentinel search.")

        try:
            found_products = self._search_products()
        except Exception as exc:
            raise AirflowException(f"Sentinel search failed: {exc}") from exc
        
        if not found_products:
            logging.warning("No products found matching criteria.")
            return []
            
        logging.info(f"Found {len(found_products)} products.")
        return found_products

    def _search_products(self):
        """Query ASF for Sentinel-1 GRD products in the requested window and ROI."""
        query_kwargs = {
            "platform": "SENTINEL-1",
            "processingLevel": "GRD",
            "intersectsWith": self.roi_wkt,
            "start": self.start_date,
            "end": self.end_date,
        }
        logging.debug("ASF search query parameters: %s", query_kwargs)

        results = asf.search(**query_kwargs)
        if results is None:
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
