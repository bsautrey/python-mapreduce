# configs_parser.py parses and returns configs from configs.py
# IF YOU ARE GETTING AN UNINFORMATIVE CONFIG PARSER ERROR, CHECK TO MAKE SURE YOUR CONFIG PARSER IMPORT STATEMENTS ARE CORRECT IN YOUR MODULE

import os
from ConfigParser import ConfigParser

compute_dir = '/mnt/ssd0'
home_dir = os.path.expanduser('~')

if os.path.exists(compute_dir):
    config_dir = compute_dir + '/configs'
    config_file_name = config_dir + '/cluster_config.cfg'
else:
    config_dir = home_dir + '/configs'
    config_file_name = config_dir + '/cluster_config.cfg'

config = ConfigParser()
config.read(config_file_name)


def get_configs(module_name):
    if module_name == 'manage_cluster':
        configs = _get_manage_cluster_configs()
    elif module_name == 'mapper':
        configs = _get_mapper_configs()
    elif module_name == 'shuffler':
        configs = _get_shuffler_configs()
    elif module_name == 'reducer':
        configs = _get_reducer_configs()
    elif module_name == 'simple_client':
        configs = _get_simple_client_configs()
    elif module_name == 'simple_server':
        configs = _get_simple_server_configs()
    elif module_name == 'disk_list':
        configs = _get_disk_list_configs()
    elif module_name == 'disk_sort':
        configs = _get_disk_sort_configs()
    elif module_name == 'scheduler':
        configs = _get_scheduler_configs()
    elif module_name == 'process':
        configs = _get_process_configs()
    elif module_name == 'slack_integration':
        configs = _get_slack_integration_configs()
    elif module_name == 'profiler':
        configs = _get_profiler_configs()

    return configs


def _get_manage_cluster_configs():
    configs = {}

    # get
    cluster_name = config.get('CLUSTER', 'cluster_name')
    locations = config.get('CLUSTER', 'locations').split(',')
    base_port = int(config.get('CLUSTER', 'base_port'))
    management_port = int(config.get('CLUSTER', 'management_port'))
    number_of_servers_per_location = int(config.get('CLUSTER', 'number_of_servers_per_location'))
    start_command = config.get('CLUSTER', 'start_command')
    kill_command = config.get('CLUSTER', 'kill_command')
    redis_restart_command = config.get('CLUSTER', 'redis_restart_command')
    management_start_command = config.get('CLUSTER', 'management_start_command')
    push_code_command = config.get('CLUSTER', 'push_code_command')
    create_configs_command = config.get('CLUSTER', 'create_configs_command')
    local_projects_dir = config.get('CLUSTER', 'local_projects_dir')
    local_code_dir = config.get('CLUSTER', 'local_code_dir')
    local_working_dir = config.get('CLUSTER', 'local_working_dir')
    code_dir = config.get('CLUSTER', 'code_dir')
    base_dir = config.get('CLUSTER', 'base_dir')
    working_dir = config.get('CLUSTER', 'working_dir')
    base_projects_dir = config.get('CLUSTER', 'base_projects_dir')
    logs_dir = config.get('CLUSTER', 'logs_dir')
    auxiliary_dir = config.get('CLUSTER', 'auxiliary_dir')
    poll_every = int(config.get('CLUSTER', 'poll_every'))

    # set
    configs['cluster_name'] = cluster_name
    configs['locations'] = locations
    configs['base_port'] = base_port
    configs['management_port'] = management_port
    configs['number_of_servers_per_location'] = number_of_servers_per_location
    configs['start_command'] = start_command
    configs['kill_command'] = kill_command
    configs['redis_restart_command'] = redis_restart_command
    configs['management_start_command'] = management_start_command
    configs['push_code_command'] = push_code_command
    configs['create_configs_command'] = create_configs_command
    configs['local_projects_dir'] = local_projects_dir
    configs['local_code_dir'] = local_code_dir
    configs['local_working_dir'] = local_working_dir
    configs['code_dir'] = code_dir
    configs['base_dir'] = base_dir
    configs['working_dir'] = working_dir
    configs['base_projects_dir'] = base_projects_dir
    configs['logs_dir'] = logs_dir
    configs['auxiliary_dir'] = auxiliary_dir
    configs['poll_every'] = poll_every

    return configs


def _get_mapper_configs():
    configs = {}

    # get
    base_dir = config.get('CLUSTER', 'base_dir')
    base_projects_dir = config.get('CLUSTER', 'base_projects_dir')
    auxiliary_dir = config.get('CLUSTER', 'auxiliary_dir')
    working_dir = config.get('CLUSTER', 'working_dir')
    number_of_servers_per_location = int(config.get('CLUSTER', 'number_of_servers_per_location'))

    # set
    configs['base_dir'] = base_dir
    configs['base_projects_dir'] = base_projects_dir
    configs['auxiliary_dir'] = auxiliary_dir
    configs['working_dir'] = working_dir
    configs['number_of_servers_per_location'] = number_of_servers_per_location

    return configs


def _get_shuffler_configs():
    configs = {}

    # get
    base_dir = config.get('CLUSTER', 'base_dir')
    code_dir = config.get('CLUSTER', 'code_dir')
    working_dir = config.get('CLUSTER', 'working_dir')
    max_hash_cache_size = int(config.get('CLUSTER', 'max_hash_cache_size'))

    # set
    configs['base_dir'] = base_dir
    configs['code_dir'] = code_dir
    configs['working_dir'] = working_dir
    configs['max_hash_cache_size'] = max_hash_cache_size

    return configs


def _get_reducer_configs():
    configs = {}

    # get
    base_dir = config.get('CLUSTER', 'base_dir')
    base_projects_dir = config.get('CLUSTER', 'base_projects_dir')
    auxiliary_dir = config.get('CLUSTER', 'auxiliary_dir')
    working_dir = config.get('CLUSTER', 'working_dir')
    number_of_servers_per_location = int(config.get('CLUSTER', 'number_of_servers_per_location'))

    # set
    configs['base_dir'] = base_dir
    configs['base_projects_dir'] = base_projects_dir
    configs['auxiliary_dir'] = auxiliary_dir
    configs['working_dir'] = working_dir
    configs['number_of_servers_per_location'] = number_of_servers_per_location

    return configs


def _get_disk_sort_configs():
    configs = {}

    # get
    working_dir = config.get('CLUSTER', 'working_dir')
    max_number_of_bytes_in_memory = int(config.get('CLUSTER', 'max_number_of_bytes_in_memory'))
    number_of_servers_per_location = int(config.get('CLUSTER', 'number_of_servers_per_location'))

    # set
    configs['working_dir'] = working_dir
    configs['max_number_of_bytes_in_memory'] = max_number_of_bytes_in_memory
    configs['number_of_servers_per_location'] = number_of_servers_per_location

    return configs


def _get_disk_list_configs():
    configs = {}

    # get
    working_dir = config.get('CLUSTER', 'working_dir')

    # set
    configs['working_dir'] = working_dir

    return configs


def _get_scheduler_configs():
    configs = {}

    # get
    local_working_dir = config.get('CLUSTER', 'local_working_dir')

    # set
    configs['local_working_dir'] = local_working_dir

    return configs


def _get_process_configs():
    configs = {}

    # get
    working_dir = config.get('CLUSTER', 'working_dir')

    # set
    configs['working_dir'] = working_dir

    return configs


def _get_slack_integration_configs():
    configs = {}

    # get
    slack_webhook = config.get('CLUSTER', 'slack_webhook')

    # set
    configs['slack_webhook'] = slack_webhook

    return configs


def _get_profiler_configs():
    configs = {}

    # get
    ram_usage_threshold = int(config.get('CLUSTER', 'ram_usage_threshold'))

    # set
    configs['ram_usage_threshold'] = int(ram_usage_threshold)

    return configs
