# configs.py contains configs for the cluster.
# NOTE: Change USER and KEY_FILE in file_transfer.py

import os
from ConfigParser import ConfigParser

config = ConfigParser()

home_dir = '/home/streamline'
compute_dir = '/mnt/ssd0'

config.add_section('CLUSTER')
config.set('CLUSTER', 'working_dir', home_dir + '/working_dir')
config.set('CLUSTER', 'temp_dir', home_dir + '/temp')
config.set('CLUSTER', 'auxiliary_dir', compute_dir + '/auxiliary')
config.set('CLUSTER', 'cluster_dir', compute_dir + '/cluster')
config.set('CLUSTER', 'locations', '192.168.0.1,192.168.0.2,...,192.168.0.100')
config.set('CLUSTER', 'management_port', '45678')
config.set('CLUSTER', 'max_attempts', '10')
config.set('CLUSTER', 'USER', 'streamline')
config.set('CLUSTER', 'KEY_FILE', home_dir + '/.ssh/cluster_id_rsa')

if os.path.exists(compute_dir):
    config_dir = compute_dir + '/configs'
    config_file_name = config_dir + '/file_transfer_config.cfg'
else:
    config_dir = home_dir + '/configs'
    config_file_name = config_dir + '/file_transfer_config.cfg'

if not os.path.exists(config_dir):
    os.mkdir(config_dir)

with open(config_file_name, 'wb') as file_name:
    config.write(file_name)
