#! /bin/bash

################################################################################
# code-server SLURM job submission template
################################################################################

################################################################################
# Functions
################################################################################

# Trap function to end the process from scancel/timeout
cancel() {
    # Pass SIGTERM to allow graceful shutdown
    if [[ -n "${VSCODE_PID}" ]]; then
        kill -SIGTERM ${VSCODE_PID}
    fi
    sleep 10
}

# Trap function to handle job teardown
cleanup() {
    exit 0
}

# Retrieve an unused port from the OS, restricted to a given range
freeport() {
    comm -23 \
        <(seq 44000 44099) \
        <(ss -Htan | awk '{gsub(/.*:/, "", $4); print $4}' | sort -u) \
    | shuf \
    | head -1
}

################################################################################
# Setup the execution environment
################################################################################

module load code-server

# Acquire an available port from the OS
IP=$(hostname -i)
PORT=$(freeport)
echo "${IP}:${PORT}"

################################################################################
# Launch code-server
################################################################################

# Wrapped with the signal and exit traps ...
trap cancel SIGTERM
trap cleanup EXIT
# ... start code-server
code-server \
    --bind-addr "${IP}:${PORT}" \
    --auth none \
    --disable-telemetry \
    --disable-workspace-trust \
    --disable-update-check \
> /dev/null &

VSCODE_PID=$!
wait ${VSCODE_PID}
