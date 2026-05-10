"""Placeholder responses when optional assets are missing on disk."""

from fastapi.responses import Response

# SVG placeholder for missing prediction / SAR-derived images (avoids broken <img> and 404 noise).
PREDICTION_IMAGE_PLACEHOLDER_SVG = b"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="320" height="200" viewBox="0 0 320 200">
  <rect fill="#1e293b" width="320" height="200"/>
  <text x="160" y="92" fill="#94a3b8" font-family="system-ui,sans-serif" font-size="13" text-anchor="middle">No image on disk</text>
  <text x="160" y="114" fill="#64748b" font-family="system-ui,sans-serif" font-size="11" text-anchor="middle">Add files under sentinel_data/predictions</text>
</svg>"""


def prediction_image_placeholder() -> Response:
    return Response(
        content=PREDICTION_IMAGE_PLACEHOLDER_SVG,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=120"},
    )
