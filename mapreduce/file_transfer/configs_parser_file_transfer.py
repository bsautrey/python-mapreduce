# configs_parser_file_transfer.py parses and returns configs from configs.py
import os
from ConfigParser import ConfigParser

compute_dir = '/mnt/ssd0'
home_dir = os.path.expanduser('~')

if os.path.exists(compute_dir):
    config_dir = compute_dir + '/configs'
    config_file_name = config_dir + '/file_transfer_config.cfg'
else:
    config_dir = home_dir + '/configs'
    config_file_name = config_dir + '/file_transfer_config.cfg'

config = ConfigParser()
config.read(config_file_name)


def get_configs(module_name):
    if module_name == 'file_transfer':
        configs = _get_file_transfer_configs()

    return configs


def _get_file_transfer_configs():
    configs = {}

    # get
    working_dir = config.get('CLUSTER', 'working_dir')
    temp_dir = config.get('CLUSTER', 'temp_dir')
    auxiliary_dir = config.get('CLUSTER', 'auxiliary_dir')
    cluster_dir = config.get('CLUSTER', 'cluster_dir')
    locations = config.get('CLUSTER', 'locations').split(',')
    management_port = int(config.get('CLUSTER', 'management_port'))
    max_attempts = int(config.get('CLUSTER', 'max_attempts'))
    USER = config.get('CLUSTER', 'USER')
    KEY_FILE = config.get('CLUSTER', 'KEY_FILE')

    # set
    configs['working_dir'] = working_dir
    configs['temp_dir'] = temp_dir
    configs['auxiliary_dir'] = auxiliary_dir
    configs['cluster_dir'] = cluster_dir
    configs['locations'] = locations
    configs['management_port'] = management_port
    configs['max_attempts'] = max_attempts
    configs['temp_dir'] = temp_dir
    configs['USER'] = USER
    configs['KEY_FILE'] = KEY_FILE

    return configs
