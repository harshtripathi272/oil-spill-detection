"""
SAR Inference Operator.

This operator wraps the model inference logic. It takes the path to a downloaded
SAR image, pre-processes it, runs the oil spill detection model, and outputs
classification results and confidence scores.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import logging
import os
import subprocess

class SARInferenceOperator(BaseOperator):
    """
    Operator to run oil spill detection inference on a SAR image.
    """

    @apply_defaults
    def __init__(
        self,
        model_path: str = "/models/oil_spill_v1.pt",
        inference_command: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.model_path = model_path
        self.inference_command = inference_command

    def execute(self, context):
        ti = context['ti']
        # Pull downloaded file paths from the download task
        image_paths = ti.xcom_pull(task_ids='download_sentinel')
        
        if not image_paths:
            logging.info("No images to process.")
            return None

        results = []
        command_template = self.inference_command or os.getenv("SAR_INFERENCE_CMD")

        if not command_template:
            raise ValueError(
                "Missing inference command. Set SAR_INFERENCE_CMD env var or pass inference_command to SARInferenceOperator."
            )

        for image_path in image_paths:
            logging.info(f"Running inference on {image_path} using model {self.model_path}")
            result = self._run_inference_command(
                command_template=command_template,
                image_path=image_path,
            )
            results.append(result)
            
        return results

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

        if prediction is None or confidence is None:
            raise RuntimeError(
                "Inference JSON must contain at least 'prediction' and 'confidence' fields."
            )

        return {
            "image": image_path,
            "prediction": prediction,
            "confidence": confidence,
            "mask_path": mask_path,
            "raw": payload,
        }
