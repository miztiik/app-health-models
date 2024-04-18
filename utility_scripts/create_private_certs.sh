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
# echo -e "${GREEN}This text is green.${RESET}"

# Reference: https://gist.github.com/mtigas/952344
# Reference: https://www.digicert.com/kb/ssl-support/openssl-quick-reference-guide.htm
# https://superuser.com/questions/226192/avoid-password-prompt-for-keys-and-prompts-for-dn-information/226229#226229

# https://github.dev/google/nogotofail/blob/7037dcb23f1fc370de784c36dbb24ae93cd5a58d/nogotofail/mitm/util/ca.py

# Set variables
CERT_DIR="n-certs"
CA_KEY="RootCA.key"
CA_CERT="RootCA.cer"
ROOT_PEM="RootCA.pem" # This is the bundle of RootCA.key and RootCA.cer in PEM format
ROOT_PFX="RootCA.pfx" # This is the bundle of RootCA.key and RootCA.cer in PFX format
CLIENT_KEY="my_client.key"
CLIENT_CSR="my_client.csr"
CLIENT_CERT="my_client.cer"
CLIENT_PEM="my_client.pem" # This is the bundle of my_client.key and my_client.cer in PEM format
CLIENT_PFX="my_client.pfx" # This is the bundle of my_client.key and my_client.cer in PFX format
DAYS_VALID=356

# Check if CA directory exists
if [ ! -d "$CERT_DIR" ]; then
  echo "The Certs directory '$CERT_DIR' does not exist. Creating it now..."
  mkdir -p "$CERT_DIR"
else
  # Clean out the directory content
  echo -e "${RED}Removing old Certs directory '$CERT_DIR' ${RESET}"
  rm -rf "$CERT_DIR"/*
fi

# Create the private certificate authority (CA) key
openssl genrsa -out "${CERT_DIR}/${CA_KEY}" 4096 > /dev/null 2>&1

# Create CA Root Certificate
openssl req \
  -new \
  -x509 \
  -days "${DAYS_VALID}" \
  -key "${CERT_DIR}/${CA_KEY}" \
  -out "${CERT_DIR}/${CA_CERT}" \
  -subj "//C=MI/ST=MIZTIIK/L=grammam/O=Miztiik/OU=MiztiikCorp/CN=miztPvtCA/emailAddress=" > /dev/null 2>&1


# Create client certificate private key
openssl genrsa -out "${CERT_DIR}/${CLIENT_KEY}" 4096 > /dev/null 2>&1

# Create client certificate signing request (CSR)
openssl req \
  -new \
  -key "${CERT_DIR}/${CLIENT_KEY}" \
  -out "${CERT_DIR}/${CLIENT_CSR}" \
  -subj "//C=MI/ST=MIZTIIK/L=grammam/O=Miztiik/OU=MiztiikCorp/CN=miztClient/emailAddress=" > /dev/null 2>&1

# Sign the newly created client cert using your certificate authority
openssl x509 \
  -req \
  -in "${CERT_DIR}/${CLIENT_CSR}" \
  -CA "${CERT_DIR}/${CA_CERT}" \
  -CAkey "${CERT_DIR}/${CA_KEY}" \
  -set_serial 01 \
  -out "${CERT_DIR}/${CLIENT_CERT}" \
  -days "${DAYS_VALID}" \
  -sha256

echo -e "${GREEN}Client certificate signed and generated: ${CERT_DIR}/${CLIENT_CERT} ${RESET}"
# Bundle the client key and certificate
cat "${CERT_DIR}/${CLIENT_CERT}" "${CERT_DIR}/${CLIENT_KEY}" > "${CERT_DIR}/${CLIENT_PEM}"
cat "${CERT_DIR}/${CA_CERT}" "${CERT_DIR}/${CA_KEY}" > "${CERT_DIR}/${ROOT_PEM}"
# cat ${CLIENT_ID}.key ${CLIENT_ID}.pem ca.pem > ${CLIENT_ID}.full.pem


# Verify the client certificate
# openssl verify -CAfile ca_certificate.pem client_certificate.pem
echo -e "${CYAN}   Verifying the client certificate."
openssl verify -CAfile "${CERT_DIR}/${CA_CERT}" "${CERT_DIR}/${CLIENT_CERT}"
echo -e ${RESET}


# Bundle the root key and certificate into a PFX file without a password
openssl pkcs12 -export \
  -out "${CERT_DIR}/${ROOT_PFX}" \
  -inkey "${CERT_DIR}/${CA_KEY}" \
  -in "${CERT_DIR}/${CA_CERT}" \
  -passout pass:

# Bundle the client key and certificate into a PFX file
openssl pkcs12 -export \
  -out "${CERT_DIR}/${CLIENT_PFX}" \
  -inkey "${CERT_DIR}/${CLIENT_KEY}" \
  -in "${CERT_DIR}/${CLIENT_CERT}" \
  -certfile "${CERT_DIR}/${CA_CERT}" \
  -passout pass:



echo "---------"
echo -e " Root Cerficiate Bundle: ${GREEN}${ROOT_PFX} ${RESET}"
echo -e " Client Cerficiate Bundle: ${GREEN}${CLIENT_PFX} ${RESET}"
echo "---------"


