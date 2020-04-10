#!/bin/bash

# Load config values
SCRIPT_DIR=$(dirname $0)
if [ -f ${SCRIPT_DIR}/property.config ]; then source ${SCRIPT_DIR}/property.config; fi

# Copy SSH keys to cluster user folder
sudo mv -v ~/cluster-ssh-keys/* /home/${DEFAULT_USERNAME}/.ssh/

# Create cluster data dir, which will be used for MapReduce code
sudo chown -R ${DEFAULT_USERNAME}:${DEFAULT_USERNAME} /home/${DEFAULT_USERNAME}/

# Create cluster data folder
sudo mkdir /mnt/ssd0
sudo chown -R ${DEFAULT_USERNAME}:${DEFAULT_USERNAME} /mnt/ssd0

