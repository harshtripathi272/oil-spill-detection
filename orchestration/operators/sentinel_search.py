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
import os

from sentinelsat import SentinelAPI

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

        username = os.getenv("COPERNICUS_USER")
        password = os.getenv("COPERNICUS_PASSWORD")

        if not username or not password:
            raise ValueError(
                "Missing Copernicus credentials. Set COPERNICUS_USER and COPERNICUS_PASSWORD environment variables."
            )

        try:
            found_products = self._search_products(username=username, password=password)
        except Exception as exc:
            raise AirflowException(f"Sentinel search failed: {exc}") from exc
        
        if not found_products:
            logging.warning("No products found matching criteria.")
            return []
            
        logging.info(f"Found {len(found_products)} products.")
        return found_products

    def _search_products(self, username: str, password: str):
        """Query Copernicus catalog for Sentinel-1 GRD products in the requested window and ROI."""
        api = SentinelAPI(
            user=username,
            password=password,
            api_url="https://apihub.copernicus.eu/apihub",
        )

        products = api.query(
            area=self.roi_wkt,
            date=(self.start_date, self.end_date),
            platformname="Sentinel-1",
            producttype="GRD",
            sensoroperationalmode="IW",
        )

        if not isinstance(products, dict):
            raise AirflowException("Unexpected response type from Sentinel API query.")

        formatted = []
        for product_id, meta in products.items():
            formatted.append(
                {
                    "product_id": product_id,
                    "filename": meta.get("filename"),
                    "size": meta.get("size"),
                    "platform": meta.get("platformname"),
                    "acquisition_date": str(meta.get("beginposition")),
                    "title": meta.get("title"),
                }
            )

        return formatted
