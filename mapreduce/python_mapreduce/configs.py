# configs.py contains configs for the cluster.
USER = 'streamline'
KEY_FILE = '/home/streamline/.ssh/cluster_id_rsa'

start_command = '''ssh -i KEY_FILE USER@LOCATION "screen -d -m -S 'cluster_SERVER_ID' rpyc_classic.py -m threaded -p PORT --host=0.0.0.0"'''
kill_command = '''ssh -i KEY_FILE USER@LOCATION "pkill -f cluster_"'''
redis_restart_command = 'ssh -i KEY_FILE USER@LOCATION sudo service redis-server restart'
management_start_command = '''ssh -i KEY_FILE USER@LOCATION "screen -d -m -S 'cluster_management' rpyc_classic.py -m threaded -p PORT --host=0.0.0.0"'''
push_code_command = 'scp -i KEY_FILE LOCAL_DIR/* USER@LOCATION:CLUSTER_DIR'

start_command = start_command.replace('USER', USER).replace('KEY_FILE', KEY_FILE)
kill_command = kill_command.replace('USER', USER).replace('KEY_FILE', KEY_FILE)
redis_restart_command = redis_restart_command.replace('USER', USER).replace('KEY_FILE', KEY_FILE)
management_start_command = management_start_command.replace('USER', USER).replace('KEY_FILE', KEY_FILE)
push_code_command = push_code_command.replace('USER', USER).replace('KEY_FILE', KEY_FILE)

import os
from ConfigParser import ConfigParser

config = ConfigParser()

home_dir = os.path.expanduser('~')
compute_dir = '/mnt/ssd0'  # must already exist

config.add_section('CLUSTER')
config.set('CLUSTER', 'cluster_name', USER)
config.set('CLUSTER', 'compute_dir', compute_dir)
config.set('CLUSTER', 'base_dir', compute_dir + '/cluster')
config.set('CLUSTER', 'working_dir', compute_dir + '/working_dir')
config.set('CLUSTER', 'counts_dir', compute_dir + '/counts')
config.set('CLUSTER', 'local_working_dir', home_dir + '/working_dir')
config.set('CLUSTER', 'local_projects_dir', home_dir + '/projects')
config.set('CLUSTER', 'local_code_dir', home_dir + '/code')
config.set('CLUSTER', 'logs_dir', compute_dir + '/logs')
config.set('CLUSTER', 'auxiliary_dir', compute_dir + '/auxiliary')
config.set('CLUSTER', 'code_dir', compute_dir + '/code')
config.set('CLUSTER', 'base_projects_dir', compute_dir + '/projects')
config.set('CLUSTER', 'locations', '192.168.0.1,192.168.0.2,...,192.168.0.100')
config.set('CLUSTER', 'base_port', '23456')
config.set('CLUSTER', 'management_port', '45678')
config.set('CLUSTER', 'number_of_servers_per_location', '23')
config.set('CLUSTER', 'max_number_of_bytes_in_memory', '1500000000')
config.set('CLUSTER', 'start_command', start_command)
config.set('CLUSTER', 'kill_command', kill_command)
config.set('CLUSTER', 'redis_restart_command', redis_restart_command)
config.set('CLUSTER', 'management_start_command', management_start_command)
config.set('CLUSTER', 'push_code_command', push_code_command)
config.set('CLUSTER', 'create_configs_command', 'python CLUSTER_DIR/configs.py')
config.set('CLUSTER', 'poll_every', '7')
config.set('CLUSTER', 'max_hash_cache_size', '1000000')
config.set('CLUSTER', 'ram_usage_threshold', '95')
config.set('CLUSTER', 'slack_webhook', 'https://hooks.slack.com/services/*************************************')

if os.path.exists(compute_dir):
    config_dir = compute_dir + '/configs'
    config_file_name = config_dir + '/cluster_config.cfg'
else:
    config_dir = home_dir + '/configs'
    config_file_name = config_dir + '/cluster_config.cfg'

if not os.path.exists(config_dir):
    os.mkdir(config_dir)

with open(config_file_name, 'wb') as file_name:
    config.write(file_name)
