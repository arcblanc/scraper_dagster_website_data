#!/bin/bash
set -eu

# Import helper functions from project root
source ./helpers_sh/common.sh
source ./helpers_sh/connectivity.sh
source ./helpers_sh/setup.sh
source ./helpers_sh/validation.sh

# Initialize environment
log "Starting Dagster entrypoint for role: ${DAGSTER_ROLE:-$1}"
setup_environment
setup_paths_and_venv

# Get and validate role
DAGSTER_ROLE=$(echo "${DAGSTER_ROLE:-$1}" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
DAGSTER_PORT=${DAGSTER_WEBSERVER_PORT:-3000}
DAGSTER_HOST=${DAGSTER_WEBSERVER_HOST:-0.0.0.0}

# Ensure paths are set correctly for both uv and system binaries
export PATH="/root/.local/bin:/usr/local/bin:/app/.venv/bin:$PATH"
export DAGSTER_NAMESPACE="${DAGSTER_NAMESPACE:-dagster.internal}"
NAMESPACE="$DAGSTER_NAMESPACE"

log_environment_info

# Setup sequence
wait_for_efs_and_setup_config
test_service_connectivity
validate_dagster_setup
final_connectivity_check

# Start services based on role
case "$DAGSTER_ROLE" in
  webserver)
    log "Starting Dagster webserver on $DAGSTER_HOST:$DAGSTER_PORT..."
    exec dagster-webserver -h "$DAGSTER_HOST" -p "$DAGSTER_PORT"
    ;;

  daemon)
    log "Starting Dagster daemon..."
    start_daemon_with_monitoring
    ;;

  codeserver)
    log "Starting Dagster code server (gRPC) on port 4000..."
    exec dagster api grpc -m unified_definitions --host 0.0.0.0 -p 4000
    ;;

  *)
    log "Error: Unknown DAGSTER_ROLE '$DAGSTER_ROLE'. Use 'webserver', 'daemon' or 'codeserver'."
    show_available_env_vars
    exit 1
    ;;
esac
