"""
Geometry Utility Module.

This module provides geospatial helper functions for the oil spill detection system.
It includes functionality to create buffers around AIS points to define Regions of Interest (ROI),
validate coordinate integrity, and calculate bounding boxes for satellite imagery queries.
"""

import math

def validate_coordinates(lat: float, lon: float) -> bool:
    """
    Validates if the provided latitude and longitude are within standard bounds.
    
    Args:
        lat: Latitude (-90 to 90)
        lon: Longitude (-180 to 180)
        
    Returns:
        True if valid, False otherwise.
    """
    return -90 <= lat <= 90 and -180 <= lon <= 180

def create_buffer_bbox(lat: float, lon: float, radius_km: float = 20.0) -> list:
    """
    Creates a square bounding box around a center point given a radius in kilometers.
    This is a simplified estimation sufficient for querying satellite catalogs.
    
    Args:
        lat: Latitude of the center point.
        lon: Longitude of the center point.
        radius_km: Buffer radius in kilometers (default 20km).
        
    Returns:
        List corresponding to [min_lon, min_lat, max_lon, max_lat].
    """
    if not validate_coordinates(lat, lon):
        raise ValueError(f"Invalid coordinates: {lat}, {lon}")

    # Approximation: 1 degree latitude ~= 111 km
    # Longitude varies with latitude: 1 degree longitude ~= 111 * cos(lat) km
    
    lat_change = radius_km / 111.0
    lon_change = radius_km / (111.0 * math.cos(math.radians(lat)))
    
    min_lat = max(-90, lat - lat_change)
    max_lat = min(90, lat + lat_change)
    min_lon = max(-180, lon - lon_change)
    max_lon = min(180, lon + lon_change)
    
    return [min_lon, min_lat, max_lon, max_lat]

def wkt_from_bbox(bbox: list) -> str:
    """
    Converts a bounding box [min_lon, min_lat, max_lon, max_lat] to WKT Polygon format.
    
    Args:
        bbox: List of [min_lon, min_lat, max_lon, max_lat]
        
    Returns:
        Well-Known Text (WKT) string representation of the polygon.
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    return f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
