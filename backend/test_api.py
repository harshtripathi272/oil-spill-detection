#!/usr/bin/env python3
"""
Test script for Oil Spill Detection Backend APIs
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"

def test_health_check():
    """Test health check endpoint"""
    print("Testing health check...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            print("✅ Health check passed")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

def test_dashboard_stats():
    """Test dashboard stats endpoint"""
    print("Testing dashboard stats...")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/dashboard/stats")
        if response.status_code == 200:
            data = response.json()
            print("✅ Dashboard stats retrieved:"            print(json.dumps(data, indent=2))
            return True
        else:
            print(f"❌ Dashboard stats failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Dashboard stats error: {e}")
        return False

def test_system_health():
    """Test system health endpoint"""
    print("Testing system health...")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/system/health")
        if response.status_code == 200:
            data = response.json()
            print("✅ System health retrieved:"            print(f"  Overall status: {data['overall_status']}")
            print(f"  Components: {len(data['components'])}")
            return True
        else:
            print(f"❌ System health failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ System health error: {e}")
        return False

def test_incidents_list():
    """Test incidents list endpoint"""
    print("Testing incidents list...")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/incidents/?limit=5")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Incidents list retrieved: {len(data)} incidents")
            return True
        else:
            print(f"❌ Incidents list failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Incidents list error: {e}")
        return False

def test_metrics():
    """Test metrics endpoint"""
    print("Testing metrics...")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/metrics/categories")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Metrics categories retrieved: {data}")
            return True
        else:
            print(f"❌ Metrics failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Metrics error: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Testing Oil Spill Detection Backend APIs")
    print("=" * 50)

    # Wait a moment for server to start
    time.sleep(2)

    tests = [
        test_health_check,
        test_dashboard_stats,
        test_system_health,
        test_incidents_list,
        test_metrics
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()

    print("=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    exit(main())