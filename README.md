WARNING: THIS SYSTEM MAKES USE OF RPyC. RPyC RUNS ON PORTS 45678 AND 23456. IF YOU DO NOT BLOCK ACCESS TO THOSE PORTS FROM THE PUBLIC INTERNET, YOUR MACHINES CAN BE ACCESSED AND ARBITRARY CODE CAN BE EXECUTED.

This system has been open sourced by Contextly (http://contextly.com/). This system was designed and built by Ben Autrey - CTO and technical cofounder of Contextly. All initialization scripts were written by Andrew Nikolaienko - Senior Infrastructure Engineer at Contextly. Feel free to email us:

ben@contextly.com
andrew@contextly.com

------------------------------------------------------------------------

## A Few Notes About the Architecture

This is a mapreduce system built in pure Python.

There are five important ways this system differs from Hadoop:

* There is no DFS. This system was intended to support small to medium sized clusters, where the probability of a hardware failure is low and can be patched quickly. A DFS can be added if need be.

* Incrementation is built into the system. For example, for datasets A and B and mapreduce function f(), if f(A + B) = f(A) + f(B), then it is possible to process dataset A at a different time than dataset B and then combine the result, f(A) + f(B), to get f(A + B). An example would be counting tokens in text. A counterexample would be finding the probability of tokens in text.

* This system is designed to run three types of jobs natively - mapreduce jobs, scripts, and file transfers. This is to accommodate a common programming pattern in mapreduce programming, e.g. 1) mapreduce job generates an output, 2) the output is downloaded to a local machine, 3) the downloaded output is processed by a script, 4) script results are uploaded to the cluster, 5) mapreduce jobs continue running, incorporating the script results.

* At the time of this writing, this code is only 2850 lines of code. This makes it manageable to learn in its entirety. As a result, a single programmer can have complete control over customizations and additions to the system.

* Downsampling is built into the system for testing purposes. There will often be times when you would like to test some mapreduce jobs, but do not wish to process all of the input data. In this case the user can set downsample = 0.0001, and a 1/10000th random sample of the input data will be generated automatically.

There are three important packages that do much to simplify the design and implementation of this system:

* RPyC (https://rpyc.readthedocs.io/en/latest/) - RPyC implements RPCs in Python. It is used for issuing commands to each machine in the cluster. For example, it is used to start and stop "cluster servers" on each machine, check for dirs, create dirs, remove dirs, etc. All of this can be done across all cluster machines in a manner that is very similar to programming in Python on a single machine, thanks to RPyC.

* Linux Screen (https://www.gnu.org/software/screen/manual/screen.html) - Screen is a window management utility. It is used here to house each cluster server and permit interactivity. For example, if one uses print statements in their mapreduce functions for debugging, you can attach to a screen and see the print statements execute for that cluster server and then detach when you are done. An extention of screen is used (called "multiscreen") that allows any user with access to that machine to attach to the screen session.

* <strike>iii) Redis (https://redis.io/) - Redis is a memory-based, key-value store. Each cluster machine has an instance of Redis running. The shuffle phase is built on top of Redis, i.e. Redis is used to transfer files across machines. It is also used to hold state information that can be easiliy accessed by cluster servers.</strike>

These packages are dependencies of the system. They must be installed.

As an example of a cluster that runs this code in production, our production system is 12 machines, 384 cores, 128GB RAM/machine, and 12TB of SSD across all machines.

------------------------------------------------------------------------

## List of Modules and Their Functionality

--- mapper.py ---

`mapper.py` implements both the map and shuffle phase. The shuffle phase is implemented indirectly by importing the Shuffle class from shuffle.py. The main function is `map()`.

`map()` - Read input data and process those inputs according to the specified map function.

--- shuffler.py ---

`shuffler.py` implements the shuffle phase. The main functions are `append()` and `shuffle()`.

`append()` - After the inputs have been processed by the map function, they are appended to a file. The file is marked for transfer to another machine.

`shuffle()` - Move all files to their destination machine using Redis.

--- reducer.py ---

`reducer.py` implements both the sort and reduce phase. The sort phase is implemented indirectly by importing the DiskSort class from `disk_sort.py`. The main functions are `sort()` and `reduce()`.

`sort()` - Sort the data in preparation for the reduce. This is often a disk-based sort.

`reduce()` - Read each grouping in the sorted data and process them according to the specified reduce function.

--- disk_sort.py ---

`disk_sort.py` implements the sort phase. The main functions are `append()` and `next_group()`.

`append()` - Add an item to the heap for sorting.

`next_group()` - Return a grouping of data from the sorted data.

--- disk_list.py ---

`disk_list.py` is an on-disk Python list for datasets that will not fit in memory, used as a helper in disk_sort.py. The main functions are `next()`, `next_group()`, and `append()`.

`append()` - Add an item to the disk-based list.

`next()` - Return the next item.

`next_group()` - Return the next grouping.

--- process.py ---

`process.py` is what each cluster server runs. The main function is `process()`.

`process()` - Implements the map, sort, and reduce phase in each cluster server.

--- manage_cluster.py ---

`manage_cluster.py` has a number of initialization and teardown functions as well as a function for coordinating all cluster servers across all machines. The main functions are `start_cluster()`, `stop_cluster()`, `kill_cluster()`, and `run()`.

`start_cluster()` - Implement a number of initialization functions.

`stop_cluster()` - Attempts to stop the cluster gracefully.

`kill_cluster()` - When `stop_cluster()` fails or when dismantling the cluster. This function kills the cluster by killing all screen sessions on the cluster machines, i.e. killing all the terminals that house the cluster servers.

`run()` - Implements the main loop and can be considered as playing the role of "master". It coordinates each mapreduce phase across all cluster servers, polling them to see when they have all finished a phase, then instructing them to move to the next phase.

--- scheduler.py ---

`scheduler.py` has two classes, Scheduler and Runner. Scheduler is used to implement and manage the job queue. Runner is used to run the jobs in the job queue. The main functions are `run()`, `submit_job()`, `delete_job()`, `get_mapreduce_job_template()`, `t_script_template()`, `get_file_transfer_template()`.

`run()` - Select a job in the job queue and run it.

`submit_job()` - Submit a job to the job queue.

`delete_job()` - Delete a job from the job queue.

`get_mapreduce_job_template()` - Return a mapreduce job, implemented as a Python dictionary, with all default values. The default values should be modified to specify a job, then submitted to the job queue using `submit_job()`.

`get_script_template()` - Return a script job, implemented as a Python dictionary, with all default values. The default values should be modified to specify a job, then submitted to the job queue using `submit_job()`.

`get_file_transfer_template()` - Return a file transfer job, implemented as a Python dictionary, with all default values. The default values should be modified to specify a job, then submitted to the job queue using `submit_job()`.

------------------------------------------------------------------------

## How Do the Components Interact?
```
    _______________
    |              |
    |  Job Queue   |
    |______________|

           ^
           |
           |
           |
           v

_________________________         _____________
|                        |        |            |
|        Runner          |  ----> | Process_1  |
|   ________________     |        |____________|
|   |               |    |
|   |   Scheduler   |    |   .
|   |_______________|    |   .
|                        |   .
|   ________________     |   .
|   |               |    |   .
|   | ManageCluster |    |        _____________
|   |_______________|    |        |            |
|                        |  ----> | Process_k  |
|________________________|        |____________|

...where each Process is like this:

_________________________ 
|                        |
|        Process         |
|   ________________     |
|   |               |    |
|   |      Map      |    |
|   |_______________|    |
|                        |
|   ________________     |
|   |               |    |
|   |     Reduce    |    |
|   |_______________|    |
|                        |
|________________________|

...and Map and Reduce are like this:

_________________________ 
|                        |
|          Map           |
|   ________________     |
|   |               |    |
|   |    Shuffle    |    |
|   |_______________|    |
|                        |
|________________________|

_________________________ 
|                        |
|        Reduce          |
|   ________________     |
|   |               |    |
|   |   DiskSort    |    |
|   |_______________|    |
|                        |
|________________________|
```

## What Does a MapReduce Job Look Like?

Both file transfer jobs and script jobs are similar to this mapreduce job (template):
```
job = {}

# job/project
job['job_submitter'] = self.job_submitter
job['job_type'] = 'mapreduce'
job['force_run'] = False
job['start_time'] = None
job['end_time'] = None
job['project_name'] = None
job['job_name'] = None
job['group_name'] = None
job['job_priority'] = None
job['input_dirs'] = None
job['delete_job_data'] = True
job['run_once'] = False
job['exception'] = None
job['current_phase'] = None

# mapper
job['map_function_name'] = None
job['auxiliary_data_name_mapper'] = None
job['hold_state'] = False
job['downsample'] = 1.0

# shuffler
job['max_number_dumped_items_shuffler'] = None
job['simultaneous_files_in_redis'] = None

# reducer
job['reduce_function_name'] = None
job['auxiliary_data_name_reducer'] = None
job['max_number_dumped_items_reducer'] = None
job['disk_based_input'] = False
job['disk_based_output'] = False
job['compress'] = False
```

## What About Map/Reduce Functions?

Both Map and Reduce functions are stored in map_functions.py and reduce_functions.py module in a project dir under the main ./projects dir, like so:
```
./projects/PROJECT_NAME/map_functions.py
```
The following is a template of required items for a map function:

```
# map_functions.py contains functions used by mapper.py.

import ujson


class MapFunctions():
    
    def __init__(self,map_function_name):
        if map_function_name == 'my_map_function':
            self.map_function = my_map_function
		elif map_function_name == 'my_map_function_2':
            self.map_function = my_map_function_2
			
    def get_map_function(self):
        return self.map_function
        
def my_map_function(line,auxiliary_data):
    items = []

    ...
	
    item = (key,value)
    items.append(item)
    return items
	
def my_map_function_2(line,auxiliary_data):
    items = []

    ...
	
    item = (key,value)
    items.append(item)
    return items

	
```
The following is a template of required items for a reduce function:
```
# reduce_functions.py contains functions used by reduce.py

class ReduceFunctions():
    
    def __init__(self,reduce_function_name):
        if reduce_function_name == 'my_reduce_function':
            self.reduce_function = my_reduce_function
        
    def get_reduce_function(self):
        return self.reduce_function
        
def my_reduce_function(group,auxiliary_data):
    items = group
	
    ...
	
    return items
```
Notice the indentation in both cases. The function is not part of the class, but is called by the class. You can put as many functions in each file as you wish. Each function is called by name.
	

## What About File Transfers?

File transfers are handled by a separate collections of tools, not included in these core cluster modules.

## How to setup MR cluster?

All instructions with examples can be found in[INSTALL.md](INSTALL.md)file.