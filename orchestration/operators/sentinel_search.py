"""
Sentinel Search Operator.

This operator interacts with the Sentinel satellite catalog to find scenes
matching the specific spatial and temporal criteria derived from an AIS event.
It outputs metadata required for subsequent download and processing steps.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import json

class SentinelSearchOperator(BaseOperator):
    """
    Operator to search for Sentinel-1 SAR products.
    
    Pushes the list of found product IDs/metadata to XCom.
    """

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
        
        # Mock API Search
        found_products = self._mock_search_api()
        
        if not found_products:
            logging.warning("No products found matching criteria.")
            return []
            
        logging.info(f"Found {len(found_products)} products.")
        return found_products

    def _mock_search_api(self):
        """Returns mock product metadata."""
        return [
            {
                "product_id": "S1A_IW_GRDH_1SDV_20231025T050000_20231025T050025",
                "filename": "S1A_IW_GRDH_1SDV_20231025T050000_20231025T050025.zip",
                "size": "900 MB",
                "platform": "Sentinel-1A"
            }
        ]
