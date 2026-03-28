"""
Sentinel Availability Sensor.

This sensor triggers the workflow when new Sentinel-1 imagery becomes available
for a defined region of interest (ROI) and time window. It handles polling logic
to avoid unnecessary API calls and ensure timely data processing.
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
import logging
import os

from sentinelsat import SentinelAPI

class SentinelAvailabilitySensor(BaseSensorOperator):
    """
    Sensor that checks for the availability of Sentinel-1 SAR data.
    
    Attributes:
        roi_bbox (list): Bounding box [min_lon, min_lat, max_lon, max_lat] to search.
        date_range (tuple): (start_date, end_date) for the accumulation window.
        platform_name (str): Satellite platform, defaults to 'Sentinel-1'.
    """

    template_fields = ("date_range",)

    @apply_defaults
    def __init__(
        self,
        roi_bbox: list,
        date_range: tuple,
        platform_name: str = "Sentinel-1",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.roi_bbox = roi_bbox
        self.date_range = date_range
        self.platform_name = platform_name

    def poke(self, context):
        """
        Check if data is available.
        
        This method is called repeatedly by Airflow until it returns True.
        """
        logging.info(f"Checking {self.platform_name} availability for ROI: {self.roi_bbox}")

        username = os.getenv("COPERNICUS_USER")
        password = os.getenv("COPERNICUS_PASSWORD")
        if not username or not password:
            raise ValueError(
                "Missing Copernicus credentials. Set COPERNICUS_USER and COPERNICUS_PASSWORD environment variables."
            )

        if not self.date_range or len(self.date_range) != 2:
            raise AirflowException("date_range must be a tuple/list with (start_date, end_date).")

        start_date, end_date = self.date_range
        if not start_date or not end_date:
            raise AirflowException("Both start_date and end_date must be populated for Sentinel polling.")

        area_wkt = self._bbox_to_wkt(self.roi_bbox)

        try:
            api = SentinelAPI(
                user=username,
                password=password,
                api_url="https://apihub.copernicus.eu/apihub",
            )

            products = api.query(
                area=area_wkt,
                date=(start_date, end_date),
                platformname=self.platform_name,
                producttype="GRD",
                sensoroperationalmode="IW",
            )
        except Exception as exc:
            logging.warning(f"Sentinel availability check failed; will retry on next poke: {exc}")
            return False

        data_found = len(products) > 0
        
        if data_found:
            logging.info("New Sentinel-1 data found.")
            return True
        else:
            logging.info("No data available yet.")
            return False

    def _bbox_to_wkt(self, bbox):
        """Convert [min_lon, min_lat, max_lon, max_lat] bbox to WKT polygon."""
        if not bbox or len(bbox) != 4:
            raise AirflowException("roi_bbox must be [min_lon, min_lat, max_lon, max_lat].")

        min_lon, min_lat, max_lon, max_lat = bbox
        return (
            f"POLYGON(({min_lon} {min_lat}, {min_lon} {max_lat}, "
            f"{max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
        )
