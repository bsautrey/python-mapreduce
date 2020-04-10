#!/bin/bash

# Exit on errors:
set -e

# Include common script
SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/common.sh
source ${SCRIPT_DIR}/property.config

# Create SSH key and home folder on master machine
checkForClusterSSHKey

printInfo "Write command here to execute on all servers, followed by [ENTER]:"
read command

while IFS='' read -r line || [[ -n "$line" ]]; do
    ip=${line}
    printInfo "Login to server: $line";
    printInfo "Execute command: $command";
    ssh -n -i ${INIT_SSH_ACCESS_KEY} ${INIT_SSH_USER}@${ip} "$command || echo" < /dev/null
done < "cluster-instances.txt"

printInfo "Done, bye!"
