#!/bin/bash

# Service connectivity testing functions

# Test DNS resolution
test_dns_resolution() {
    local service_name="$1"
    local full_service_name="$service_name.${DAGSTER_NAMESPACE:-dagster.internal}"

    log "Testing DNS resolution for: $full_service_name"

    if command -v nslookup >/dev/null 2>&1; then
        if nslookup "$full_service_name" >/dev/null 2>&1; then
            local resolved_ip=$(nslookup "$full_service_name" | grep -A1 "Name:" | tail -1 | awk '{print $2}' | head -1)
            log "✅ DNS resolved $full_service_name -> $resolved_ip"
            return 0
        else
            log "❌ DNS resolution failed for $full_service_name"
            return 1
        fi
    elif command -v dig >/dev/null 2>&1; then
        if dig +short "$full_service_name" >/dev/null 2>&1; then
            local resolved_ip=$(dig +short "$full_service_name" | head -1)
            log "✅ DNS resolved $full_service_name -> $resolved_ip"
            return 0
        else
            log "❌ DNS resolution failed for $full_service_name"
            return 1
        fi
    else
        log "⚠️  No DNS tools (nslookup/dig) available, skipping DNS test for $full_service_name"
        return 0
    fi
}

# Test port connectivity
test_port_connectivity() {
    local service_name="$1"
    local port="$2"
    local timeout=10
    local retries=50
    local delay=10
    local full_service_name="$service_name.${DAGSTER_NAMESPACE:-dagster.internal}"

    log "⏳ Checking connectivity to $full_service_name:$port (timeout: ${timeout}s, retries: $retries)"

    local attempt=1
    while [ $attempt -le $retries ]; do
        if command -v nc >/dev/null 2>&1; then
            if timeout "$timeout" nc -z "$full_service_name" "$port" 2>/dev/null; then
                log "✅ Port $port is reachable on $full_service_name"
                return 0
            else
                log "Attempt $attempt/$retries: Port $port not reachable on $full_service_name, retrying in ${delay}s..."
            fi
        else
            log "⚠️  No 'nc' command found, skipping connectivity check"
            return 0
        fi

        sleep $delay
        attempt=$((attempt + 1))
    done

    log "❌ Port $port is NOT reachable on $full_service_name after $retries attempts"
    return 1
}

# Main service connectivity test
test_service_connectivity() {
    log "=== Testing Service Discovery and Connectivity ==="

    # CloudMap service names
    local WEBSERVER_SERVICE="${DAGSTER_WEBSERVER_SERVICE:-webserver}"
    local DAEMON_SERVICE="${DAGSTER_DAEMON_SERVICE:-daemon}"
    local CODE_SERVER_SERVICE="${DAGSTER_CODE_SERVER_SERVICE:-codeserver}"
    local NAMESPACE="${DAGSTER_NAMESPACE:-dagster.internal}"

    log "CloudMap Configuration:"
    log "  Namespace: $NAMESPACE"
    log "  Webserver Service: $WEBSERVER_SERVICE"
    log "  Daemon Service: $DAEMON_SERVICE"
    log "  Code Server Service: $CODE_SERVER_SERVICE"

    # Test based on current role
    case "$DAGSTER_ROLE" in
        webserver)
            log "--- Webserver connectivity tests ---"
            wait_for_dns_resolution "$DAEMON_SERVICE"
            wait_for_dns_resolution "$CODE_SERVER_SERVICE"
            test_dns_resolution "$DAEMON_SERVICE"
            test_dns_resolution "$CODE_SERVER_SERVICE"
            test_port_connectivity "$CODE_SERVER_SERVICE" "4000"
            ;;

        daemon)
            log "--- Daemon connectivity tests ---"
            wait_for_dns_resolution "$CODE_SERVER_SERVICE"
            wait_for_dns_resolution "$WEBSERVER_SERVICE"
            test_dns_resolution "$CODE_SERVER_SERVICE"
            test_dns_resolution "$WEBSERVER_SERVICE"
            test_port_connectivity "$CODE_SERVER_SERVICE" "4000"
            ;;

        codeserver)
            log "--- Code Server connectivity tests ---"
            wait_for_dns_resolution "$WEBSERVER_SERVICE"
            wait_for_dns_resolution "$DAEMON_SERVICE"
            test_dns_resolution "$WEBSERVER_SERVICE"
            test_dns_resolution "$DAEMON_SERVICE"
            ;;
    esac

    # Test database if configured
    test_database_connectivity

    # Show network info
    show_network_info

    log "=== Service Discovery Tests Complete ==="
}

# Test database connectivity
test_database_connectivity() {
    if [ -n "${DATABASE_HOST:-}" ]; then
        log "--- Database connectivity test ---"
        local db_port="${DATABASE_PORT:-5432}"
        log "Testing database connectivity: $DATABASE_HOST:$db_port"

        if command -v nc >/dev/null 2>&1; then
            if timeout 10 nc -z "$DATABASE_HOST" "$db_port" 2>/dev/null; then
                log "✅ Database is reachable at $DATABASE_HOST:$db_port"
            else
                log "❌ Database is NOT reachable at $DATABASE_HOST:$db_port"
            fi
        fi
    fi
}

# Show network information
show_network_info() {
    log "--- Current container network info ---"
    log "Hostname: $(hostname)"
    log "Container IP: $(hostname -i 2>/dev/null || echo 'unknown')"

    if command -v ip >/dev/null 2>&1; then
        log "Network interfaces:"
        ip addr show | grep -E "inet " | grep -v "127.0.0.1" | awk '{print "  " $2}' || true
    fi
}

# Final connectivity check before service start
final_connectivity_check() {
    log "=== Final connectivity check before starting $DAGSTER_ROLE ==="

    case "$DAGSTER_ROLE" in
        webserver)
            if ! test_port_connectivity "${DAGSTER_CODE_SERVER_SERVICE:-dagster-codeserver}" "4000" ; then
                log "WARNING: Code server may not be available yet. Webserver will retry during execution."
            fi
            ;;
        daemon)
            if ! test_port_connectivity "${DAGSTER_CODE_SERVER_SERVICE:-dagster-codeserver}" "4000" ; then
                log "WARNING: Code server may not be available yet. Daemon will retry during execution."
            fi
            ;;
        codeserver)
            log "Code server starting - other services will connect to this service"
            ;;
    esac
}
