#!/bin/bash

# Exit on errors:
set -e

# Include common script
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/common.sh
source ${SCRIPT_DIR}/property.config


# Create SSH key and home folder on master machine
checkForClusterSSHKey


if [ $# -eq 0 ]
  then
    printWarn "Please add main machine IP as parameter for this script"
  else
    ip=$1
    printInfo "Install software on Master machine: $ip"

    # Install needed software
    ssh -i ${INIT_SSH_ACCESS_KEY} ${INIT_SSH_USER}@${ip} 'bash -s' < <(cat ${SCRIPT_DIR}/property.config cluster-install-software.sh)

    printInfo "Copy SSH keys"

    # Copy cluster key to worker and allow access for it
    cat ssh-keys/cluster_id_rsa.pub | ssh -i ${INIT_SSH_ACCESS_KEY} ${INIT_SSH_USER}@${ip} "sudo bash -c 'cat >> /home/${DEFAULT_USERNAME}/.ssh/authorized_keys'"
    rsync -e "ssh -i ${INIT_SSH_ACCESS_KEY}" --rsync-path="mkdir -p /home/${INIT_SSH_USER}/cluster-ssh-keys/ && rsync" -r ssh-keys/ ${INIT_SSH_USER}@${ip}:~/cluster-ssh-keys/

    printInfo "Create folders structure"

    ssh -i ${INIT_SSH_ACCESS_KEY} ${INIT_SSH_USER}@${ip} 'bash -s' < <(cat ${SCRIPT_DIR}/property.config cluster-create-folders-main.sh)

    printInfo "Copy MR code"

    scp -i ssh-keys/cluster_id_rsa ${DEFAULT_PROJECT_ROOT}/python_mapreduce/* ${DEFAULT_USERNAME}@${ip}:/home/${DEFAULT_USERNAME}/code
    scp -i ssh-keys/cluster_id_rsa ${DEFAULT_PROJECT_ROOT}/projects/tests/* ${DEFAULT_USERNAME}@${ip}:/home/${DEFAULT_USERNAME}/projects/tests
    scp -i ssh-keys/cluster_id_rsa ${DEFAULT_PROJECT_ROOT}/file_transfer/* ${DEFAULT_USERNAME}@${ip}:/home/${DEFAULT_USERNAME}/file_transfer
    scp -i ssh-keys/cluster_id_rsa ${DEFAULT_PROJECT_ROOT}/miscellaneous/* ${DEFAULT_USERNAME}@${ip}:/home/${DEFAULT_USERNAME}/miscellaneous

    printInfo "Done, bye!"
fi