#!/bin/bash

# scripts/launch_local.sh
# One-command local execution for the VesselWatch Pipeline.

# Set project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Colors for logging
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 1. Load Environment Variables & Create Logs Dir
mkdir -p logs

if [ -f .env ]; then
    echo -e "${BLUE}[INFO]${NC} Loading environment from .env..."
    # Export vars, ignoring comments and empty lines
    export $(grep -v '^#' .env | xargs)
else
    echo -e "${BLUE}[INFO]${NC} No .env file found. Using system environment."
fi

# 2. Virtual Environment Setup
if [ -n "$VIRTUAL_ENV" ]; then
    echo -e "${BLUE}[INFO]${NC} Using active virtual environment: $VIRTUAL_ENV"
elif [ -n "$CONDA_PREFIX" ]; then
    echo -e "${BLUE}[INFO]${NC} Using active Conda environment: $CONDA_PREFIX"
elif [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    echo -e "${BLUE}[INFO]${NC} Activating virtual environment (venv)..."
    source "$PROJECT_ROOT/venv/bin/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    echo -e "${BLUE}[INFO]${NC} Activating virtual environment (.venv)..."
    source "$PROJECT_ROOT/.venv/bin/activate"
else
    echo -e "${BLUE}[INFO]${NC} No active or local venv found. Running with system python."
fi

# 3. Infrastructure Management
start_infra() {
    echo -e "${BLUE}[INFO]${NC} Checking infrastructure..."

    # Redis
    if ! (echo > /dev/tcp/localhost/6379) >/dev/null 2>&1 ; then
        echo -e "${BLUE}[INFO]${NC} Starting Redis..."
        redis-server --daemonize yes
        sleep 2
    else
        echo -e "${BLUE}[INFO]${NC} Redis is already running."
    fi

    # Zookeeper (Kafka requirement)
    if ! (echo > /dev/tcp/localhost/2181) >/dev/null 2>&1 ; then
        echo -e "${BLUE}[INFO]${NC} Starting Zookeeper..."
        "$PROJECT_ROOT/infra/kafka/bin/zookeeper-server-start.sh" -daemon "$PROJECT_ROOT/infra/kafka/config/zookeeper.properties"
        
        echo -e "${BLUE}[INFO]${NC} Waiting for Zookeeper (2181)..."
        for i in {1..30}; do
            if (echo > /dev/tcp/localhost/2181) >/dev/null 2>&1; then break; fi
            sleep 1
        done
    else
        echo -e "${BLUE}[INFO]${NC} Zookeeper is already running."
    fi

    # Kafka Broker
    if ! (echo > /dev/tcp/localhost/9092) >/dev/null 2>&1 ; then
        echo -e "${BLUE}[INFO]${NC} Starting Kafka Broker..."
        "$PROJECT_ROOT/infra/kafka/bin/kafka-server-start.sh" -daemon "$PROJECT_ROOT/infra/kafka/config/server.properties"
        
        echo -e "${BLUE}[INFO]${NC} Waiting for Kafka (9092)..."
        for i in {1..30}; do
            if (echo > /dev/tcp/localhost/9092) >/dev/null 2>&1; then break; fi
            sleep 1
        done
    else
        echo -e "${BLUE}[INFO]${NC} Kafka Broker is already running."
    fi
}

start_infra

# Ensure project root is in PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Function to stop all background processes
cleanup() {
    echo ""
    # Use the dedicated stop script for clean shutdown
    "$PROJECT_ROOT/scripts/stop_local.sh"
    exit 0
}

# Trap CTRL+C
trap cleanup SIGINT

echo -e "${GREEN}====================================================${NC}"
echo -e "${GREEN}   Starting VesselWatch Pipeline (Local Native)     ${NC}"
echo -e "${GREEN}====================================================${NC}"

# 3. Launch Services
python ingestion/ais_stream/run_ingestion.py > "$PROJECT_ROOT/logs/ingestion.log" 2>&1 &
echo -e "${BLUE}[RUNNING]${NC} Ingestion Service (PID: $!)"

python services/stream_processor/main.py > "$PROJECT_ROOT/logs/stream_processor.log" 2>&1 &
echo -e "${BLUE}[RUNNING]${NC} Stream Processor (PID: $!)"

python services/anomaly_detector/main.py > "$PROJECT_ROOT/logs/anomaly_detector.log" 2>&1 &
echo -e "${BLUE}[RUNNING]${NC} Anomaly Detector (PID: $!)"

python services/trigger_bridge/main.py > "$PROJECT_ROOT/logs/trigger_bridge.log" 2>&1 &
echo -e "${BLUE}[RUNNING]${NC} Trigger Bridge    (PID: $!)"

echo ""
echo -e "Logs are being written to ${BLUE}$PROJECT_ROOT/logs/*.log${NC}"
echo -e "Press ${GREEN}Ctrl+C${NC} to stop all services."

# Wait for all background processes
wait
