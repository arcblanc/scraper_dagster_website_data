#!/bin/bash

# Common utility functions

# Logging function
log() { echo "[entrypoint] $(date '+%Y-%m-%d %H:%M:%S') $*"; }

# Setup basic environment
setup_environment() {
    export DAGSTER_NAMESPACE="${DAGSTER_NAMESPACE:-dagster.internal}"
    export PATH="/root/.local/bin:/usr/local/bin:/app/.venv/bin:$PATH"

    # Ensure DAGSTER_HOME exists and has correct permissions
    mkdir -p "$DAGSTER_HOME"
    chmod -R 755 "$DAGSTER_HOME"
}

# Setup paths and virtual environment
setup_paths_and_venv() {
    if [ -f /app/.venv/bin/activate ]; then
        log "Activating uv virtual environment..."
        source /app/.venv/bin/activate
    else
        log "No uv virtual environment found, using system Python"
    fi

    # Check if dagster commands are available
    which dagster-daemon &>/dev/null || log "WARNING: dagster-daemon not found in PATH"
    which dagster &>/dev/null || log "WARNING: dagster not found in PATH"
    which dagster-webserver &>/dev/null || log "WARNING: dagster-webserver not found in PATH"
}

# Log environment information
log_environment_info() {
    log "PATH=$PATH"
    log "Python location: $(which python)"
    log "DAGSTER_ROLE: $DAGSTER_ROLE"
    log "DAGSTER_HOST: $DAGSTER_HOST"
    log "DAGSTER_PORT: $DAGSTER_PORT"
    log "DAGSTER_HOME: $DAGSTER_HOME"
    log "PYTHONPATH: $PYTHONPATH"
}

# Show available environment variables
show_available_env_vars() {
    log "Available environment variables:"
    log "  DAGSTER_ROLE: $DAGSTER_ROLE"
    log "  DAGSTER_HOME: $DAGSTER_HOME"
    log "  DAGSTER_WEBSERVER_HOST: $DAGSTER_HOST"
    log "  DAGSTER_WEBSERVER_PORT: $DAGSTER_PORT"
    log "  PYTHONPATH: $PYTHONPATH"
}

# DNS wait helper
wait_for_dns_resolution() {
    local service_name="$1"
    local full_service_name="$service_name.${DAGSTER_NAMESPACE:-dagster.internal}"
    local max_attempts=50
    local attempt=1
    local delay=10

    log "⏳ Waiting for DNS resolution: $full_service_name (max ${max_attempts} attempts)"

    while [ $attempt -le $max_attempts ]; do
        if getent hosts "$full_service_name" >/dev/null 2>&1; then
            local resolved_ip=$(getent hosts "$full_service_name" | awk '{print $1}' | head -1)
            log "✅ DNS resolved $full_service_name -> $resolved_ip"
            return 0
        fi

        log "Attempt $attempt/$max_attempts: $full_service_name not yet resolvable, retrying in ${delay}s..."
        sleep $delay
        attempt=$((attempt + 1))
    done

    log "❌ Failed to resolve $full_service_name after $max_attempts attempts"
    return 1
}
