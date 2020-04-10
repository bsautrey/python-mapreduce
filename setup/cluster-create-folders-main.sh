#!/bin/bash

# Load config values
SCRIPT_DIR=$(dirname $0)
if [ -f ${SCRIPT_DIR}/property.config ]; then source ${SCRIPT_DIR}/property.config; fi

sudo mkdir /home/${DEFAULT_USERNAME}/working_dir
sudo mkdir /home/${DEFAULT_USERNAME}/code
sudo mkdir /home/${DEFAULT_USERNAME}/file_transfer
sudo mkdir /home/${DEFAULT_USERNAME}/projects
sudo mkdir /home/${DEFAULT_USERNAME}/projects/tests
sudo mkdir /home/${DEFAULT_USERNAME}/miscellaneous

sudo chmod -R 755 /home/${DEFAULT_USERNAME}/working_dir
sudo chmod -R 755 /home/${DEFAULT_USERNAME}/code
sudo chmod -R 755 /home/${DEFAULT_USERNAME}/file_transfer
sudo chmod -R 755 /home/${DEFAULT_USERNAME}/projects
sudo chmod -R 755 /home/${DEFAULT_USERNAME}/projects/tests
sudo chmod -R 755 /home/${DEFAULT_USERNAME}/miscellaneous

# Copy SSH keys to cluster user folder
sudo mv -v ~/cluster-ssh-keys/* /home/${DEFAULT_USERNAME}/.ssh/

# Create cluster data dir, which will be used for MapReduce code
sudo chown -R ${DEFAULT_USERNAME}:${DEFAULT_USERNAME} /home/${DEFAULT_USERNAME}/

# Create cluster data folder
sudo mkdir /mnt/sandbox
sudo mkdir /mnt/sandbox/uploads
sudo chown -R ${DEFAULT_USERNAME}:${DEFAULT_USERNAME} /mnt/sandbox

