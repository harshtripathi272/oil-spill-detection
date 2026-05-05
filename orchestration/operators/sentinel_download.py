"""
Sentinel Download Operator.

This operator handles the retrieval of Sentinel-1 SAR data from the provider
(e.g., Copernicus Open Access Hub or a cloud bucket mirroring the dataset).
It ensures the raw data is available locally or in an object store for inference.
"""

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import logging
import os

import asf_search as asf

class SentinelDownloadOperator(BaseOperator):
    """
    Operator to download a specific Sentinel-1 product.
    
    Expects product metadata from XCom (upstream task).
    """

    def __init__(
        self,
        download_dir: str = "/tmp/sentinel_data",
        max_downloads: int = 1,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.download_dir = download_dir
        self.max_downloads = max_downloads

    def execute(self, context):
        # Retrieve product list from XCom (assuming the upstream task ID is 'search_sentinel')
        ti = context['ti']
        products = ti.xcom_pull(task_ids='search_sentinel')

        if isinstance(products, dict):
            products = [products]
        if products is not None and not isinstance(products, list):
            raise AirflowException("Expected search_sentinel XCom payload to be a list of product dicts.")
        
        if not products:
            logging.info("ℹ️ No products to download (search returned empty).")
            return []

        # Limit to max_downloads products
        original_count = len(products)
        products = products[:self.max_downloads]
        if len(products) < original_count:
            logging.info(f"⚠️ Limiting downloads to {self.max_downloads} of {original_count} available products")

        logging.info(f"📥 Starting download of {len(products)} Sentinel-1 products to {self.download_dir}")

        downloaded_paths = []
        os.makedirs(self.download_dir, exist_ok=True)

        username = os.getenv("EARTHDATA_USERNAME")
        password = os.getenv("EARTHDATA_PASSWORD")

        if not username or not password:
            error_msg = "❌ Missing Earthdata credentials. Set EARTHDATA_USERNAME and EARTHDATA_PASSWORD environment variables."
            logging.error(error_msg)
            raise ValueError(error_msg)

        try:
            logging.info("🔐 Authenticating with Earthdata...")
            session = asf.ASFSession().auth_with_creds(username, password)
            logging.info("✅ Earthdata authentication successful")
        except Exception as exc:
            error_msg = f"❌ Failed to initialize ASF authenticated session: {exc}"
            logging.error(error_msg)
            raise AirflowException(error_msg) from exc

        for product in products:
            if not isinstance(product, dict):
                logging.warning(f"Skipping malformed product payload: {product}")
                continue

            try:
                file_path = self._download_product(session=session, product=product)
            except Exception as exc:
                product_id = product.get("product_id", "unknown")
                logging.warning(f"Download failed for product {product_id}: {exc}")
                continue

            if file_path:
                downloaded_paths.append(file_path)

        if products and not downloaded_paths:
            raise AirflowException("No Sentinel products were downloaded successfully.")
            
        return downloaded_paths

    def _download_product(self, session, product: dict):
        """Download one product via ASF URL and return the local path."""
        product_name = product.get("fileName") or product.get("filename") or product.get("title") or "unknown_product"
        download_url = product.get("url")

        if not download_url:
            logging.warning("Skipping product without download URL: %s", product)
            return None

        logging.info("Downloading ASF product %s from %s to %s", product_name, download_url, self.download_dir)
        download_result = asf.download_url(
            url=download_url,
            path=self.download_dir,
            filename=product_name,
            session=session,
        )

        local_path = self._resolve_local_path(download_result, self.download_dir, product_name)
        if not local_path:
            logging.warning("Download completed but no local path returned for URL: %s", download_url)
            return None

        return local_path

    @staticmethod
    def _resolve_local_path(download_result, download_dir: str, fallback_name: str):
        """Normalize asf_search download return types into a local filesystem path."""
        if isinstance(download_result, str):
            return download_result

        if isinstance(download_result, dict):
            return download_result.get("path") or download_result.get("file")

        if isinstance(download_result, (list, tuple)) and download_result:
            first = download_result[0]
            if isinstance(first, str):
                return first
            if isinstance(first, dict):
                return first.get("path") or first.get("file")

        candidate = os.path.join(download_dir, fallback_name)
        if os.path.exists(candidate):
            return candidate

        return None
