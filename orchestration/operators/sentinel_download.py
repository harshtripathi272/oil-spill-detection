"""
Sentinel Download Operator.

This operator handles the retrieval of Sentinel-1 SAR data from the provider
(e.g., Copernicus Open Access Hub or a cloud bucket mirroring the dataset).
It ensures the raw data is available locally or in an object store for inference.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import os

class SentinelDownloadOperator(BaseOperator):
    """
    Operator to download a specific Sentinel-1 product.
    
    Expects product metadata from XCom (upstream task).
    """

    @apply_defaults
    def __init__(
        self,
        download_dir: str = "/tmp/sentinel_data",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.download_dir = download_dir

    def execute(self, context):
        # Retrieve product list from XCom (assuming the upstream task ID is 'search_sentinel')
        ti = context['ti']
        products = ti.xcom_pull(task_ids='search_sentinel')
        
        if not products:
            logging.info("No products to download.")
            return []

        downloaded_paths = []
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

        for product in products:
            file_path = self._mock_download(product)
            downloaded_paths.append(file_path)
            
        return downloaded_paths

    def _mock_download(self, product):
        """Simulates a file download."""
        filename = product.get("filename", "unknown.zip")
        dest_path = os.path.join(self.download_dir, filename)
        logging.info(f"Downloading {filename} to {dest_path}...")
        
        # Create a dummy file
        with open(dest_path, "w") as f:
            f.write("DUMMY SAR DATA CONTENT")
            
        return dest_path
