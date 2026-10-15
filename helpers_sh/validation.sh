#!/bin/bash

# Dagster setup validation functions

# Validate Dagster setup
validate_dagster_setup() {
    log "Validating Dagster setup for role: $DAGSTER_ROLE"

    # Test basic Dagster instance creation
    python -c "
import sys
try:
    from dagster import DagsterInstance
    instance = DagsterInstance.get()
    print('✅ Dagster instance created successfully')

    # For daemon, test database connectivity specifically
    if '$DAGSTER_ROLE' == 'daemon':
        # Test database operations
        run_storage = instance.run_storage
        print('✅ Database connection verified')

        # Test if we can check daemon status (this will also test DB)
        daemon_status = instance.get_daemon_statuses()
        print('✅ Daemon status check successful')

except ImportError as e:
    print(f'❌ Import error: {e}')
    sys.exit(1)
except Exception as e:
    print(f'❌ Dagster setup validation failed: {e}')
    sys.exit(1)
" || {
        log "ERROR: Dagster setup validation failed"
        exit 1
    }

    log "✅ Dagster setup validation passed"
}
