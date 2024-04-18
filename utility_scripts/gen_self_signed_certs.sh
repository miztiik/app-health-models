#!/bin/bash
# set -x
set -e
set -o pipefail

# Define colors for console output
GREEN="\e[32m"
RED="\e[31m"
BLUE="\e[34m"
CYAN="\e[36m"
YELLOW="\e[33m"
RESET="\e[0m"

# Define variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CERT_DIR="${SCRIPT_DIR}/certs"
LOG_FILE="${CERT_DIR}/self_signed_cert_creation.log"

ROOT_CA_KEY="MIZTIIK.KEY"
ROOT_CA_CSR="MIZTIIK.CSR"
ROOT_CA_CERT="MIZTIIK.CRT"
ROOT_CA_CER="MIZTIIK.CER"
ROOT_CA_PFX="MIZTIIK.PFX"

CLIENT_KEY="miztiikClient.KEY"
CLIENT_CSR="miztiikClient.CSR"
CLIENT_CERT="miztiikClient.CRT"
CLIENT_CER="miztiikClient.CER"
CLIENT_PFX="miztiikClient.PFX"

# Create Certs directory if it doesn't exist
mkdir -p "$CERT_DIR"

# Define log function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Clean out the directory content
log "Deleting old certs in directory: '$CERT_DIR'"
rm -rf "$CERT_DIR"/*

pushd "$CERT_DIR" > /dev/null || exit 1

# Define the validity period in days
VALIDITY=365

# Define the subject values
ROOT_SUBJ="//emailAddress=C=MI/ST=City State of MIZTIIK/L=Nagaram/O=Miztiik/OU=MiztiikCorp/CN=miztPvtCA"
CLIENT_SUBJ="//emailAddress=C=MI/ST=Client State of MIZTIIK/L=grammam/O=Miztiik/OU=MiztiikCorp/CN=miztClient"

# Function to create a certificate
create_certificate() {
    KEY=$1
    CSR=$2
    CERT=$3
    PFX=$4
    SUBJ=$5
    CA_CERT=$6
    CA_KEY=$7

    # Get the current date in YYYYMMDD format
    SERIAL=$(date +%Y%m%d%H%M%S)

    openssl genrsa -out "$KEY" 2048 > /dev/null 2>&1
    openssl req -new -key "$KEY" -out "$CSR" -subj "$SUBJ" > /dev/null 2>&1
    if [ -z "$CA_CERT" ] || [ -z "$CA_KEY" ]; then
        openssl x509 -req -days "$VALIDITY" -in "$CSR" -signkey "$KEY" -set_serial "$SERIAL" -out "$CERT" > /dev/null 2>&1
    else
        openssl x509 -req -days "$VALIDITY" -in "$CSR" -CA "$CA_CERT" -CAkey "$CA_KEY" -set_serial "$SERIAL" -out "$CERT" > /dev/null 2>&1
    fi
    # Create a .pfx file for the certificate
    openssl pkcs12 -export -out "$PFX" -inkey "$KEY" -in "$CERT" -passout pass: > /dev/null 2>&1
}

# Create a root CA certificate
create_certificate "$ROOT_CA_KEY" "$ROOT_CA_CSR" "$ROOT_CA_CERT" "$ROOT_CA_PFX" "$ROOT_SUBJ"

# Create a client certificate
create_certificate "$CLIENT_KEY" "$CLIENT_CSR" "$CLIENT_CERT" "$CLIENT_PFX" "$CLIENT_SUBJ" "$ROOT_CA_CERT" "$ROOT_CA_KEY"

# Rename the root CA certificate to .cer format & also client certificate to .cer format
cp "$ROOT_CA_CERT" "$ROOT_CA_CER"
cp "$CLIENT_CERT" "$CLIENT_CER"

# Verify the client certificate
# openssl verify -CAfile ca_certificate.pem client_certificate.pem
log "Verifying the client certificate."
openssl verify -CAfile "${ROOT_CA_CERT}" "${CLIENT_CERT}"

log "Certificate creation completed. Check the log file for details: $LOG_FILE"

echo "---------"
echo -e " Root Certificate: ${GREEN}${ROOT_CA_CER} ${RESET}"
echo -e " Client Certificate: ${GREEN}${CLIENT_CER} ${RESET}"
echo "---------"

popd > /dev/null || exit 1
