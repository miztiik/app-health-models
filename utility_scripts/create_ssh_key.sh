#!/bin/bash
set -ex

# Set the key file path
SSH_KEY_FILE="./../dist/ssh_keys/miztiik_ssh_key"

# Remove the key file if it exists
if [ -f "$SSH_KEY_FILE" ]; then
  rm -f "$SSH_KEY_FILE"
fi

# Generate the SSH key without a passphrase
# ssh-keygen -t rsa -b 4096 -N "" -f "$SSH_KEY_FILE" 
ssh-keygen -t rsa -b 4096 -N "" -f "$SSH_KEY_FILE" -C "miztiik@git"
