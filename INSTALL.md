## Minimum requirements
 
To setup cluster you need at last 2 machines:

    * Master with Ubuntu 16.04 or newer
    * Workers (1 or more) with Ubuntu 16.04 or newer


## Installation steps


### 1. Prepare setup scripts

Checkout git repo with project and cluster init scripts:

```
git clone https://github.com/bsautrey/python-mapreduce && cd python-mapreduce
```

Go to folder with cluster setup scripts and update needed configs:

```
cd setup
```

#### 1.1 "property.config"

This file contains variables for setup initial (root) access to cluster machines. Normally this is
username and SSH key. For example:

```
INIT_SSH_USER='root'
INIT_SSH_ACCESS_KEY='./ssh-keys/root_cluster_ssh_key'
```

And cluster user name:

```
DEFAULT_USERNAME="streamline"
DEFAULT_PROJECT_ROOT="../mapreduce"
```

Note: Some providers can't create machines with SSH key access, in other words they can give only login/password access.
In this case you need to generate SSH key and copy it to each machine manually.

Generate cluster SSH key:

```
ssh-keygen -t rsa -f ./ssh-keys/root_cluster_ssh_key -N ""
```

Copy SSH key to all machines manually:
```
ssh-copy-id -i ./ssh-keys/root_cluster_ssh_key {login}@{MASTER_IP}
ssh-copy-id -i ./ssh-keys/root_cluster_ssh_key {login}@{WORKER_IP_1}
...
ssh-copy-id -i ./ssh-keys/root_cluster_ssh_key {login}@{WORKER_IP_N}
```

#### 1.2 "cluster-instances.txt" 

This file contains list of cluster workers public IP's. Each IP on new line, for example:

```
142.93.66.98
142.93.66.108
.....
142.93.66.129
```


### 2. Master initial configuration

In order to start installation process you need to setup and configure Master instance. Later it will be used for
 communicate with Workers.

Run script:

```
./cluster-setup-main.sh {MASTER_IP}
```

If all fine with SSH access to master machine (step #1.1), it will be configured automatically. This will take some time, about 2-3 minutes.


### 3. Workers configuration


Workers configuration can be started with:

```
./cluster-setup-instances.sh
```

This will take some time, about 2-3 minutes per each Worker. This step require valid configuration in steps #1.1 and #1.2.


### 4. Workers Helper Script

In case if some command need to be executed on each Worker, you can use this script: 

```
./cluster-command-instances.sh
```

It will prompt for command, after [ENTER] this command will be executed on each worker.


### 5. Run cluster

#### 5.1 Cluster configs

Now you can login to Master machine as cluster user:

```
ssh -i .ssh-keys/cluster_id_rsa {DEFAULT_USERNAME}@{MASTER_IP}
```

Note: Please do not forget to update workers private IP's in next cluster configs:

```
/home/{DEFAULT_USERNAME}/code/configs.py
/home/{DEFAULT_USERNAME}/file_transfer/configs.py
```
and execute:
```
python file_transfer/configs.py
python code/configs.py

```

Note: For machines with SSD disks, please do not forget to remove "discard" mount option in /etc/fstab.

/etc/fstab example:
```
/dev/disk/by-id/scsi-0DO_Volume_volume-nyc3-01 /mnt/sandbox ext4 defaults,nofail 0 0
```

In case if "discard" option was enable by default, you can disable it with this command:
```
sudo mount -o remount,nodiscard /mnt/ssd0
sudo mcedit /etc/fstab    - do not forget to remove "discard" option
```

Useful commands:
```
findmnt -O discard        - list all mount points with "discard" option
```


#### 5.2 Run Cluster

Execute these commands in "python" console:

```
import sys
sys.path.append('/home/streamline/code')
sys.path.append('/home/streamline/miscellaneous')

from scheduler import Runner
r = Runner()
r.run()
```

Example output:

```
...
INITIALIZING CLUSTER PROCESS: S_18
INITIALIZING CLUSTER PROCESS: S_15
INITIALIZING CLUSTER PROCESS: S_14
INITIALIZING CLUSTER PROCESS: S_17
INITIALIZING CLUSTER PROCESS: S_16
INITIALIZING CLUSTER PROCESS: S_11
INITIALIZING CLUSTER PROCESS: S_10
INITIALIZING CLUSTER PROCESS: S_13
INITIALIZING CLUSTER PROCESS: S_12
>>> r.run()

```

#### 5.3 Test Cluster

Open in "python" console and execute these commands:

Generate test data:
```
for i in range(10000):
    f = open('/mnt/sandbox/uploads/TEST_'+str(i)+'.data','w')
    f.write('0')
    f.close()
```

Upload test data on cluster:

```
import sys
sys.path.append('/home/streamline/file_transfer')
from file_transfer import FileTransfer
ft = FileTransfer()
input_dir = '/mnt/sandbox/uploads'
output_dir = '/mnt/ssd0/cluster/test_input'
ft.upload_bulk(input_dir,output_dir)
```

Run test MapReduce job:

```
import sys
sys.path.append('/home/streamline/code')
sys.path.append('/home/streamline/miscellaneous')

job = {}

# job/project
job['job_submitter'] = 'user_name'
job['job_type'] = 'mapreduce'
job['force_run'] = False
job['start_time'] = None
job['end_time'] = None
job['project_name'] = 'tests'
job['job_name'] = 'tests'
job['group_name'] = 'tests'
job['job_priority'] = 1
job['input_dirs'] = ['/mnt/ssd0/cluster/test_input']
job['delete_job_data'] = True
job['run_once'] = True
job['exception'] = None
job['current_phase'] = None

# mapper
job['map_function_name'] = 'generate_keys_and_payloads'
job['auxiliary_data_name_mapper'] = None
job['hold_state'] = False
job['downsample'] = 1.0

# shuffler
job['max_number_dumped_items_shuffler'] = 1000000
job['max_cache_size'] = 5000
job['number_simultaneous_tranfers'] = 12

# reducer
job['reduce_function_name'] = 'sum_function'
job['auxiliary_data_name_reducer'] = None
job['max_number_dumped_items_reducer'] = 1000000
job['disk_based_input'] = False
job['disk_based_output'] = False
job['compress'] = True

import sys
sys.path.append('/home/streamline/code')
from scheduler import Scheduler
s = Scheduler()
s.submit_job(job)
```

Download results:

```
import sys
sys.path.append('/home/streamline/file_transfer')
from file_transfer import FileTransfer
ft = FileTransfer()
input_dir = '/mnt/ssd0/cluster/tests/reduce'
output_dir = '/mnt/sandbox/downloads'
ft.download(input_dir,output_dir)
```

Congratulations, cluster is ready for new jobs!