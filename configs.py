# configs.py contains configs for the cluster.

import os
from ConfigParser import ConfigParser

config = ConfigParser()

home_dir = 'e.g. /HOME/ME'
compute_dir = '/mnt/ssd0' # must already exist

config.add_section('CLUSTER')
config.set('CLUSTER','cluster_name','cluster')
config.set('CLUSTER','compute_dir',compute_dir)
config.set('CLUSTER','base_dir',compute_dir +'/cluster')
config.set('CLUSTER','working_dir',compute_dir +'/working_dir')
config.set('CLUSTER','local_working_dir','/services/archive/working_dir')
config.set('CLUSTER','logs_dir',compute_dir+'/logs')
config.set('CLUSTER','auxiliary_dir',compute_dir+'/auxiliary')
config.set('CLUSTER','code_dir',compute_dir +'/code')
config.set('CLUSTER','base_projects_dir',compute_dir +'/projects')
config.set('CLUSTER','locations','COMMA SEPARATED IP ADDRESSES')
config.set('CLUSTER','base_port','23456')
config.set('CLUSTER','management_port','45678')
config.set('CLUSTER','number_of_servers_per_location','25')
config.set('CLUSTER','max_number_of_bytes_in_memory','80000000000')
config.set('CLUSTER','start_command','''ssh LOCATION "screen -d -m -S 'cluster_SERVER_ID' rpyc_classic.py -m threaded -p PORT"''')
config.set('CLUSTER','kill_command','''ssh LOCATION "screen -X -S cluster_SERVER_ID kill"''')
config.set('CLUSTER','redis_restart_command','ssh LOCATION sudo service redis-server restart')
config.set('CLUSTER','management_start_command','''ssh LOCATION "screen -d -m -S 'cluster_management' rpyc_classic.py -m threaded -p PORT"''')
config.set('CLUSTER','push_code_command','scp LOCAL_DIR/* LOCATION:CLUSTER_DIR')
config.set('CLUSTER','create_configs_command','python CLUSTER_DIR/configs.py')
config.set('CLUSTER','poll_every','7')
config.set('CLUSTER','max_hash_cache_size','2500000')
config.set('CLUSTER','redis_auth','REDIS KEY')


if os.path.exists(compute_dir):
    config_dir = compute_dir +'/configs'
    config_file_name = config_dir +'/cluster_config.cfg'
else:
    config_dir = home_dir +'/configs'
    config_file_name = config_dir +'/cluster_config.cfg'
    
if not os.path.exists(config_dir):
    os.mkdir(config_dir)

with open(config_file_name,'wb') as file_name:
    config.write(file_name)