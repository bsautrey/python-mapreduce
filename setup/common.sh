#!/bin/bash

SCRIPT_DIR=$(dirname $0)
source ${SCRIPT_DIR}/property.config

# Messages:
##############################
RETURN=   # Global variable to hold oversize return value of function.

printInfo () {
    echo -e "\033[0;33m + \033[1;32m$1 \033[0m"
}

printWarn () {
    echo -e "\033[0;33m + \033[1;33m$1 \033[0m"
}

printError () {
    echo -e "\033[0;33m + \033[1;31mError: $1 \033[0m"
    exit 1
}

promptMsg () {
    [ `echo $FLAGS | grep y` ] && return 0

    echo -e "\033[0;33m + \033[1;32m$1 \033[0m"
    read -p "[y/N]: " USER_RESPONSE

    if [[ $USER_RESPONSE =~ ^[Yy]$ ]]; then
        return 0
    else
        return 1
    fi
}

readMsg() {
    echo -en "\e[0;33m + \e[1;32m$1 \e[0m"
    read USER_RESPONSE
    RETURN=$USER_RESPONSE
}

checkForSSHKey() {
    worker_ssh_key_path=".ssh/id_rsa"
    if [ ! -f ${worker_ssh_key_path} ]; then
        echo "Generate Worker Key file..."
        ssh-keygen -t rsa -f ${worker_ssh_key_path} -N ""

        # Copy new ssh key for master machine user
        mkdir -p /home/${DEFAULT_USERNAME}/.ssh
        cat ${worker_ssh_key_path}.pub >> /home/${DEFAULT_USERNAME}/.ssh/authorized_keys
        cp ${worker_ssh_key_path}* /home/${DEFAULT_USERNAME}/.ssh/
        chown -R ${DEFAULT_USERNAME}:${DEFAULT_USERNAME} /home/${DEFAULT_USERNAME}/.ssh
    fi
}

checkForClusterSSHKey() {
    worker_ssh_key_path="ssh-keys/${DEFAULT_CLUSTER_KEY_NAME}"

    if [ ! -f ${worker_ssh_key_path} ]; then
        printInfo "Generate Cluster Key file..."
        ssh-keygen -t rsa -f ${worker_ssh_key_path} -N ""
    fi
}
