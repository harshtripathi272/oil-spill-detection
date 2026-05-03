#!/bin/bash

# scripts/stop_local.sh
# Stop all VesselWatch Pipeline services gracefully

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Colors for logging
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}[INFO]${NC} Stopping VesselWatch Pipeline services..."
echo ""

# Stop services gracefully first
echo -e "${BLUE}[INFO]${NC} Stopping application services (ingestion, stream_processor, anomaly_detector, trigger_bridge)..."
pkill -f "run_ingestion.py" || echo -e "${YELLOW}[SKIP]${NC} Ingestion not running"
pkill -f "services.stream_processor.main" || echo -e "${YELLOW}[SKIP]${NC} Stream processor not running"
pkill -f "services.anomaly_detector.main" || echo -e "${YELLOW}[SKIP]${NC} Anomaly detector not running"
pkill -f "services.trigger_bridge.main" || echo -e "${YELLOW}[SKIP]${NC} Trigger bridge not running"
sleep 2

# Force kill any remaining Python processes from this project
echo -e "${BLUE}[INFO]${NC} Force-killing any remaining processes..."
pkill -9 -f "run_ingestion.py" 2>/dev/null || true
pkill -9 -f "stream_processor.main" 2>/dev/null || true
pkill -9 -f "anomaly_detector.main" 2>/dev/null || true
pkill -9 -f "trigger_bridge.main" 2>/dev/null || true

# Stop infrastructure
echo -e "${BLUE}[INFO]${NC} Stopping Kafka broker..."
"$PROJECT_ROOT/infra/kafka/bin/kafka-server-stop.sh" >/dev/null 2>&1 || pkill -9 -f 'kafka.Kafka' 2>/dev/null || true
sleep 1

echo -e "${BLUE}[INFO]${NC} Stopping ZooKeeper..."
"$PROJECT_ROOT/infra/kafka/bin/zookeeper-server-stop.sh" >/dev/null 2>&1 || pkill -9 -f 'QuorumPeerMain' 2>/dev/null || true
sleep 1

echo -e "${BLUE}[INFO]${NC} Stopping Redis..."
redis-cli shutdown >/dev/null 2>&1 || true
sleep 1

# Verify all stopped
echo ""
echo -e "${BLUE}[INFO]${NC} Verifying all processes stopped..."
remaining=$(ps aux | grep -E "run_ingestion|stream_processor|anomaly_detector|trigger_bridge|kafka|zookeeper|redis" | grep -v grep | wc -l)

if [ "$remaining" -eq 0 ]; then
    echo -e "${GREEN}[OK]${NC} All services stopped successfully."
else
    echo -e "${YELLOW}[WARN]${NC} $remaining processes still running (may be system services)"
    ps aux | grep -E "run_ingestion|stream_processor|anomaly_detector|trigger_bridge|kafka|zookeeper|redis" | grep -v grep || true
fi

# Optional: Clean up logs
if [ "$1" = "--clean-logs" ]; then
    echo -e "${BLUE}[INFO]${NC} Cleaning log files..."
    rm -f "$PROJECT_ROOT/logs"/*.log
    echo -e "${GREEN}[OK]${NC} Logs cleaned."
fi

echo ""
echo -e "${GREEN}===== Stop Complete =====${NC}"
