"""
SAR Inference Operator.

This operator wraps the model inference logic. It takes the path to a downloaded
SAR image, pre-processes it, runs the oil spill detection model, and outputs
classification results and confidence scores.
"""

from airflow.models import BaseOperator
import json
import logging
import os
import subprocess
from pathlib import Path
import uuid
from datetime import datetime
import sys

import cv2
import numpy as np

from preprocessing.apply_sar_processing import preprocess_sar_png

# Ensure backend `app` package is importable when running inside Airflow.
# The backend lives at `<repo>/oil-spill-detection/backend/app`.
OIL_SPILL_ROOT = Path(__file__).resolve().parents[2]  # oil-spill-detection/
BACKEND_DIR = OIL_SPILL_ROOT / "backend"
if BACKEND_DIR.exists() and str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


class SARInferenceOperator(BaseOperator):
    """
    Operator to run oil spill detection inference on a SAR image.
    """

    def __init__(
        self,
        model_path: str = "/models/oil_spill_v1.pt",
        inference_command: str = None,
        work_dir: str = '/data/user13/oilspill_ugq/oil-spill-detection/sentinel_data/preprocessed',
        predictions_dir: str = '/data/user13/oilspill_ugq/oil-spill-detection/sentinel_data/predictions',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.model_path = model_path
        self.inference_command = inference_command
        self.work_dir = work_dir
        self.predictions_dir = predictions_dir

    def execute(self, context):
        ti = context['ti']
        # Pull downloaded file paths from the download task
        image_paths = ti.xcom_pull(task_ids='download_sentinel')
        
        if not image_paths:
            logging.info("No images to process.")
            return None

        os.makedirs(self.work_dir, exist_ok=True)
        os.makedirs(self.predictions_dir, exist_ok=True)
        results = []
        command_template = self.inference_command or os.getenv("SAR_INFERENCE_CMD")

        if not command_template:
            raise ValueError(
                "Missing inference command. Set SAR_INFERENCE_CMD env var or pass inference_command to SARInferenceOperator."
            )

        # Get database session - import here to avoid module loading issues
        try:
            from app.database import SessionLocal
            db = SessionLocal()
        except ImportError as e:
            logging.warning(f"Database modules not available, skipping database storage: {e}")
            db = None

        try:
            for image_path in image_paths:
                ready_input = self._prepare_input(image_path)
                logging.info(f"Running inference on prepared SAR image {ready_input} using model {self.model_path}")
                result = self._run_inference_command(
                    command_template=command_template,
                    image_path=ready_input,
                )
                
                # Save prediction to database and create prediction image (if database available)
                if db is not None:
                    prediction_record = self._save_prediction_to_db(db, result, context)
                    annotated_image_path = self._create_prediction_image(result, prediction_record.id)
                    if annotated_image_path:
                        prediction_record.prediction_image_path = annotated_image_path
                        db.commit()
                else:
                    logging.info("Database not available, skipping database storage")
                    self._create_prediction_image(result, f"prediction_{uuid.uuid4().hex[:8]}")
                
                results.append(result)
                
        finally:
            if db is not None:
                db.close()
            
        return results

    def _prepare_input(self, image_path: str) -> str:
        """Ensure the downloaded SAR product is prepared as a preprocessed PNG for inference."""
        path = Path(image_path)
        suffix = path.suffix.lower()

        # Check if this is a BURST image (not suitable for current preprocessing)
        filename = path.name.upper()
        if "BURST" in filename:
            raise RuntimeError(
                f"BURST images are not supported for inference. "
                f"Current preprocessing pipeline expects RTC (Radiometrically Terrain Corrected) images. "
                f"File: {path.name}"
            )

        if suffix in {".png", ".jpg", ".jpeg"}:
            converted_path = str(path)
        elif suffix in {".tif", ".tiff"}:
            converted_path = self._convert_tiff_to_png(path)
        elif suffix == ".h5":
            converted_path = self._convert_h5_to_png(path)
        else:
            raise RuntimeError(
                f"Unsupported SAR file type '{suffix}' for inference preparation: {image_path}"
            )

        preprocessed_path = Path(self.work_dir) / f"{path.stem}_preprocessed.png"
        if not preprocessed_path.exists():
            preprocess_sar_png(
                input_path=Path(converted_path),
                output_path=preprocessed_path,
            )
        return str(preprocessed_path)

    def _convert_tiff_to_png(self, path: Path) -> str:
        try:
            import tifffile as tiff
        except ImportError as exc:
            logging.warning(
                "tifffile is not installed; falling back to OpenCV for TIFF loading."
            )
            arr = cv2.imread(str(path), cv2.IMREAD_UNCHANGED)
            if arr is None:
                raise RuntimeError(
                    f"Unable to load TIFF file {path}. Install tifffile for better TIFF support."
                )
        else:
            arr = tiff.imread(str(path))

        if arr is None:
            raise RuntimeError(f"Unable to read TIFF file {path}")

        return self._save_array_as_png(arr, path, suffix=".png")

    def _convert_h5_to_png(self, path: Path) -> str:
        try:
            import h5py
        except ImportError as exc:
            raise RuntimeError(
                "h5py is required to convert .h5 SAR products to PNG. "
                "Install h5py in the inference environment."
            ) from exc

        with h5py.File(str(path), 'r') as h5_file:
            dataset = self._find_h5_dataset(h5_file)
            if dataset is None:
                raise RuntimeError(f"No suitable SAR dataset found inside {path}")
            arr = dataset[()]

        return self._save_array_as_png(arr, path, suffix=".png")

    def _find_h5_dataset(self, group):
        try:
            import h5py
        except ImportError:
            return None

        for key, item in group.items():
            if isinstance(item, h5py.Dataset) and item.ndim in {2, 3}:
                return item
            if isinstance(item, h5py.Group):
                candidate = self._find_h5_dataset(item)
                if candidate is not None:
                    return candidate
        return None

    def _save_array_as_png(self, arr: np.ndarray, source_path: Path, suffix: str) -> str:
        if arr.ndim == 2:
            arr = np.stack([arr] * 3, axis=-1)
        elif arr.ndim == 3:
            if arr.shape[0] in {1, 2, 3} and arr.shape[-1] not in {1, 2, 3}:
                arr = np.moveaxis(arr, 0, -1)
            if arr.shape[-1] == 1:
                arr = np.concatenate([arr] * 3, axis=-1)
            elif arr.shape[-1] == 2:
                arr = np.concatenate([arr, arr[..., :1]], axis=-1)
            elif arr.shape[-1] > 3:
                arr = arr[..., :3]
        else:
            raise RuntimeError(
                f"Unsupported SAR array shape {arr.shape} for {source_path}"
            )

        arr_uint8 = self._normalize_to_uint8(arr)
        output_path = Path(self.work_dir) / f"{source_path.stem}_raw{suffix}"
        cv2.imwrite(str(output_path), cv2.cvtColor(arr_uint8, cv2.COLOR_RGB2BGR))
        return str(output_path)

    def _normalize_to_uint8(self, arr: np.ndarray) -> np.ndarray:
        arr = arr.astype(np.float32)
        min_val = np.nanmin(arr)
        max_val = np.nanmax(arr)
        if max_val <= min_val:
            return np.zeros(arr.shape, dtype=np.uint8)

        normalized = (arr - min_val) / (max_val - min_val)
        normalized = np.clip(normalized * 255.0, 0, 255)
        return normalized.astype(np.uint8)

    def _run_inference_command(self, command_template: str, image_path: str):
        """
        Runs an external inference command and parses JSON result from stdout.

        Supported placeholders in command_template:
        - {input}: downloaded Sentinel file path
        - {model}: model path
        """
        command = command_template.format(input=image_path, model=self.model_path)
        completed = subprocess.run(
            command,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                f"Inference command failed for {image_path}. "
                f"Exit code: {completed.returncode}. stderr: {completed.stderr.strip()}"
            )

        stdout = completed.stdout.strip()
        if not stdout:
            raise RuntimeError(f"Inference command produced empty output for {image_path}")

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Inference command must output a JSON object on stdout. "
                f"Received: {stdout[:300]}"
            ) from exc

        prediction = payload.get("prediction")
        confidence = payload.get("confidence")
        mask_path = payload.get("mask_path")
        bbox_coordinates = payload.get("bbox_coordinates")

        if prediction is None or confidence is None:
            raise RuntimeError(
                "Inference JSON must contain at least 'prediction' and 'confidence' fields."
            )

        return {
            "image": image_path,
            "prediction": prediction,
            "confidence": confidence,
            "mask_path": mask_path,
            "bbox_coordinates": bbox_coordinates,
            "raw": payload,
        }

    def _save_prediction_to_db(self, db, result, context):
        """Save prediction result to database"""
        # Import here so it's always available in this method's scope.
        from app.models.predictions import Prediction

        # Extract incident_id from context or generate one
        incident_id = context.get('incident_id') or f"incident_{uuid.uuid4().hex[:8]}"
        dag_run_id = context.get('dag_run_id') or context['dag'].dag_id + "__" + context['run_id']
        
        # Create prediction record
        prediction_record = Prediction(
            incident_id=incident_id,
            dag_run_id=dag_run_id,
            image_path=result["image"],
            prediction=result["prediction"],
            confidence=result["confidence"],
            bbox_coordinates=result.get("bbox_coordinates"),
            mask_path=result.get("mask_path"),
            model_version=self.model_path.split('/')[-1] if self.model_path else None,
            processing_time=result.get("processing_time"),
            extra_metadata=result.get("raw", {})
        )
        
        db.add(prediction_record)
        db.commit()
        db.refresh(prediction_record)
        
        logging.info(f"Saved prediction to database: ID {prediction_record.id}, prediction: {result['prediction']}")
        return prediction_record

    def _create_prediction_image(self, result, prediction_id):
        """Create prediction image with bounding boxes overlaid"""
        try:
            # Load original image
            image = cv2.imread(result["image"])
            if image is None:
                logging.warning(f"Could not load image {result['image']} for prediction visualization")
                return
            
            # Create a copy for drawing
            prediction_image = image.copy()
            
            # Draw bounding boxes if available
            bbox = result.get("bbox_coordinates")
            if bbox:
                # Assuming bbox format: {"x1": x1, "y1": y1, "x2": x2, "y2": y2}
                x1, y1 = int(bbox.get("x1", 0)), int(bbox.get("y1", 0))
                x2, y2 = int(bbox.get("x2", 0)), int(bbox.get("y2", 0))
                
                # Draw rectangle
                cv2.rectangle(prediction_image, (x1, y1), (x2, y2), (0, 255, 0), 2)
                
                # Add prediction text
                text = f"{result['prediction']} ({result['confidence']:.2f})"
                cv2.putText(prediction_image, text, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
            
            # Save prediction image
            prediction_filename = f"prediction_{prediction_id}.jpg"
            prediction_path = os.path.join(self.predictions_dir, prediction_filename)
            cv2.imwrite(prediction_path, prediction_image)
            
            logging.info(f"Created prediction image: {prediction_path}")
            return prediction_path
            
        except Exception as e:
            logging.error(f"Error creating prediction image: {e}")
            return None
