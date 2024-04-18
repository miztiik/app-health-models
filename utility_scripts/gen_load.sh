#!/bin/bash
# set -x
# set -e
set -o pipefail

# Define colors for console output
GREEN="\e[32m"
RED="\e[31m"
BLUE="\e[34m"
CYAN="\e[36m"
YELLOW="\e[33m"
RESET="\e[0m"
# echo -e "${GREEN}This text is green.${RESET}"

# LOG_FILE="/var/log/miztiik-$(date +'%Y-%m-%d').json"
LOG_FILE="miztiik-$(date +'%Y-%m-%d').json"
COMPUTER_NAME=$(hostname)
SLEEP_AT_WORK_SECS=0
LOAD_COUNT=2

# Set the URL
URL="https://latency-store-front-us-apim-005.azure-api.net/api/store-events-producer-fn"

# Set the number of iterations (N)
N=5  # You can replace this with your desired number of iterations

function log_entry() {
    local timestamp
    timestamp=$(date +'%Y-%m-%dT%H:%M:%S.%3NZ')
    echo "{\"timestamp\":\"$timestamp\",\"computer_name\":\"$COMPUTER_NAME\",\"count\":\"$1\",\"req_no\":\"$2\",\"tot_req\":\"$3\"   }" >> "$LOG_FILE"
}

function generate_load() {
    # Loop N times
    for ((i=1; i<=${LOAD_COUNT}; i++)); do
        # Generate a random value for COUNT (between 1 and 10)
        COUNT=$((1 + RANDOM % 10))

        # Make the curl request with the random count
        curl "$URL?count=$COUNT" &

        # Log the request
        log_entry "$COUNT" "$i" "$LOAD_COUNT"

        # Echo the request with color
        echo -e "${CYAN}Request: (${i}/${LOAD_COUNT}) -------------${RESET}"

        # Optional: Add a sleep between requests if needed
        sleep 1  # Adjust the sleep duration as needed
    done
}

generate_load
