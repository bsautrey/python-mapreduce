#!/bin/bash

# Load config values
SCRIPT_DIR=$(dirname $0)
if [ -f ${SCRIPT_DIR}/property.config ]; then source ${SCRIPT_DIR}/property.config; fi


# Update server software and install needed packages
sudo apt-get update
sudo apt-get install -y screen mc git python python-dev python2.7-dev nload build-essential autoconf libtool

# Setup python stuff
sudo wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py
sudo rm get-pip.py

sudo pip install simplejson rpyc ujson pytz shapely numpy scipy sklearn requests psutil

sudo pip install -U scikit-learn==0.18

# Update default servers timezone
sudo timedatectl set-timezone America/New_York

# Increase default OS settings
sudo /bin/su -c "echo 'fs.file-max = 1000000' >> /etc/sysctl.conf"
sudo /bin/su -c "echo 'vm.overcommit_memory=1' >> /etc/sysctl.conf"
sudo /bin/su -c "echo '* - nofile 100000' >> /etc/security/limits.conf"
sudo sysctl -p

# Increase number of SSH connections
sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.old
sudo bash -c "cat >>/etc/ssh/sshd_config <<EOL

MaxStartups 500
MaxSessions 100
PasswordAuthentication no
EOL"
sudo service sshd restart


# Create user on cluster machines
sudo useradd -d /home/${DEFAULT_USERNAME} -m  -s /bin/bash ${DEFAULT_USERNAME}
sudo usermod -aG sudo ${DEFAULT_USERNAME}
sudo bash -c "sudo cat >>/etc/sudoers <<EOL

${DEFAULT_USERNAME}  ALL=NOPASSWD:ALL
EOL"

# Enable multiscreen support for new cluster user
sudo bash -c "cat >/home/${DEFAULT_USERNAME}/.screenrc <<EOL
multiuser on
acladd root
EOL"

# Create SSH config file
sudo mkdir /home/${DEFAULT_USERNAME}/.ssh
sudo bash -c "cat >/home/${DEFAULT_USERNAME}/.ssh/config <<EOL
Host *
    StrictHostKeyChecking no
EOL"

# Prepare screen command stuff
sudo chmod +s /usr/bin/screen
sudo chmod 2755 /usr/bin/screen
sudo chmod 775 /var/run/screen
sudo chmod 777 /run/screen

# Alias for multiscreen
sudo ln -s /usr/bin/screen /usr/bin/multiscreen

# Create cluster data dir, which will be used for MapReduce code
sudo chown -R ${DEFAULT_USERNAME}:${DEFAULT_USERNAME} /home/${DEFAULT_USERNAME}/

exit 0
