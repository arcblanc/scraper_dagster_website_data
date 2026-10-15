#!/bin/bash

# EFS and configuration setup functions

# Wait for EFS mount and setup configuration
wait_for_efs_and_setup_config() {
    local max_attempts=50
    local attempt=1

    log "Checking EFS mount and Dagster configuration..."

    while [ $attempt -le $max_attempts ]; do
        log "Attempt $attempt/$max_attempts: Checking EFS and config files..."

        # Test if we can write to DAGSTER_HOME (EFS mount test)
        if echo "test" > "$DAGSTER_HOME/.efs_test" 2>/dev/null; then
            rm -f "$DAGSTER_HOME/.efs_test"
            log "EFS mount is writable"

            if check_existing_config_files; then
                return 0
            fi

            if copy_config_files_from_staging; then
                return 0
            fi
        else
            log "EFS mount not ready yet (attempt $attempt/$max_attempts)"
        fi

        sleep 10
        attempt=$((attempt + 1))
    done

    log "ERROR: Failed to setup EFS and config files after $max_attempts attempts"
    exit 1
}

# Check if existing config files are valid
check_existing_config_files() {
    if [ -f "$DAGSTER_HOME/workspace.yaml" ] && [ -f "$DAGSTER_HOME/dagster.yaml" ]; then
        log "Found existing Dagster config files in $DAGSTER_HOME"

        if [ -s "$DAGSTER_HOME/dagster.yaml" ] && [ -s "$DAGSTER_HOME/workspace.yaml" ]; then
            log "Config files are valid and non-empty"
            return 0
        else
            log "Config files exist but are empty, will recreate..."
        fi
    fi
    return 1
}

# Copy configuration files from staging area
copy_config_files_from_staging() {
    if [ -f "/opt/dagster-config/workspace.yaml" ] && [ -f "/opt/dagster-config/dagster.yaml" ]; then
        log "Copying Dagster config from /opt/dagster-config/ to $DAGSTER_HOME..."

        if cp /opt/dagster-config/workspace.yaml "$DAGSTER_HOME/workspace.yaml" && \
           cp /opt/dagster-config/dagster.yaml "$DAGSTER_HOME/dagster.yaml"; then

            if [ -s "$DAGSTER_HOME/dagster.yaml" ] && [ -s "$DAGSTER_HOME/workspace.yaml" ]; then
                log "Successfully copied and verified Dagster configuration files"
                chmod 644 "$DAGSTER_HOME/dagster.yaml" "$DAGSTER_HOME/workspace.yaml"
                return 0
            else
                log "ERROR: Copied files are empty or corrupted"
            fi
        else
            log "ERROR: Failed to copy config files"
        fi
    else
        log "ERROR: Source config files not found in /opt/dagster-config/"
        log "Expected files:"
        log "  /opt/dagster-config/workspace.yaml"
        log "  /opt/dagster-config/dagster.yaml"
        exit 1
    fi
    return 1
}


# Start daemon with monitoring
start_daemon_with_monitoring() {
    sleep 5  # Optional delay for safety

    # Start continuous connectivity monitoring in background
    log "Starting continuous connectivity monitoring..."
    (
        while true; do
            if nc -z codeserver.dagster.internal 4000; then
                log "Code server port 4000 reachable"
            else
                log "⚠️  Code server port 4000 NOT reachable"
            fi
            sleep 30
        done
    ) &
    MONITOR_PID=$!
    log "Connectivity monitoring started with PID: $MONITOR_PID"

    # Start daemon in background and capture PID
    dagster-daemon run &
    DAEMON_PID=$!

    sleep 2
    log "Current Dagster-related processes:"

    # Wait for heartbeats to register, then check status
    sleep 20
    log "Checking daemon heartbeat status..."
    python -c "
from dagster import DagsterInstance
statuses = DagsterInstance.get().get_daemon_statuses()
for k, v in statuses.items():
    print(f'{k}: healthy={v.healthy}, last_heartbeat={v.last_heartbeat}')
"

    # Wait for daemon process to exit (or be killed externally)
    wait $DAEMON_PID
}
