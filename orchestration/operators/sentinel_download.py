"""
Sentinel Download Operator.

This operator handles the retrieval of Sentinel-1 SAR data from the provider
(e.g., Copernicus Open Access Hub or a cloud bucket mirroring the dataset).
It ensures the raw data is available locally or in an object store for inference.
"""

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
import logging
import os

from sentinelsat import SentinelAPI

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

        if isinstance(products, dict):
            products = [products]
        if products is not None and not isinstance(products, list):
            raise AirflowException("Expected search_sentinel XCom payload to be a list of product dicts.")
        
        if not products:
            logging.info("No products to download.")
            return []

        downloaded_paths = []
        os.makedirs(self.download_dir, exist_ok=True)

        username = os.getenv("COPERNICUS_USER")
        password = os.getenv("COPERNICUS_PASSWORD")

        if not username or not password:
            raise ValueError(
                "Missing Copernicus credentials. Set COPERNICUS_USER and COPERNICUS_PASSWORD environment variables."
            )

        try:
            api = SentinelAPI(
                user=username,
                password=password,
                api_url="https://apihub.copernicus.eu/apihub",
            )
        except Exception as exc:
            raise AirflowException(f"Failed to initialize Sentinel API client: {exc}") from exc

        for product in products:
            if not isinstance(product, dict):
                logging.warning(f"Skipping malformed product payload: {product}")
                continue

            try:
                file_path = self._download_product(api=api, product=product)
            except Exception as exc:
                product_id = product.get("product_id", "unknown")
                logging.warning(f"Download failed for product {product_id}: {exc}")
                continue

            if file_path:
                downloaded_paths.append(file_path)

        if products and not downloaded_paths:
            raise AirflowException("No Sentinel products were downloaded successfully.")
            
        return downloaded_paths

    def _download_product(self, api: SentinelAPI, product: dict):
        """Downloads a Sentinel-1 product from Copernicus hub and returns the local file path."""
        product_id = product.get("product_id")
        product_name = product.get("filename") or product.get("title") or "unknown_product"

        if not product_id:
            logging.warning(f"Skipping product without product_id: {product}")
            return None

        logging.info(f"Downloading Sentinel product {product_name} ({product_id}) to {self.download_dir}")
        download_result = api.download(product_id, directory_path=self.download_dir)

        local_path = download_result.get("path")
        if not local_path:
            logging.warning(f"Download completed but no local path returned for {product_id}")
            return None

        return local_path
