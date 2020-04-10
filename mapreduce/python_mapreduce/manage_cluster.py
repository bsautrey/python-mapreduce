# manage_cluster.py for starting and stopping the cluster.

from __future__ import division

import os, sys, subprocess, socket, json, errno
from time import sleep, ctime
from uuid import uuid4
from threading import Thread

import rpyc, time

from configs_parser import get_configs
from slack_integration import SlackIntegration


class ManageCluster:

    def __init__(self):
        # configs
        configs = get_configs(self.__module__)
        self.cluster_name = configs['cluster_name']
        self.locations = configs['locations']
        self.base_port = configs['base_port']
        self.management_port = configs['management_port']
        self.number_of_servers_per_location = configs['number_of_servers_per_location']
        self.start_command = configs['start_command']
        self.kill_command = configs['kill_command']
        self.management_start_command = configs['management_start_command']
        self.push_code_command = configs['push_code_command']
        self.create_configs_command = configs['create_configs_command']
        self.local_projects_dir = configs['local_projects_dir']
        self.local_code_dir = configs['local_code_dir']
        self.local_working_dir = configs['local_working_dir']
        self.code_dir = configs['code_dir']
        self.base_dir = configs['base_dir']
        self.working_dir = configs['working_dir']
        self.base_projects_dir = configs['base_projects_dir']
        self.logs_dir = configs['logs_dir']
        self.auxiliary_dir = configs['auxiliary_dir']
        self.poll_every = configs['poll_every']

        # project and job
        self.project_dir = None
        self.project_name = None
        self.job_name = None
        self.delete_job_data = None
        self.current_phase = None

        # other
        self.server_names = None
        self.connections = None
        self.management_connections = None
        self.processes = None
        self.start_cluster_processes = {}
        self.exceptions = None
        self.slack_integration = SlackIntegration()

    def start_cluster(self):
        start_time = time.time()

        print 'CREATE LOCAL DIRS...'
        self._create_local_dirs()

        print 'INIT CLUSTER WORKERS...'
        self._init_workers()

        print 'CREATING SERVER REFERENCES AND CONNECTING TO CLUSTER...'
        self._start_cluster()

        print 'SETUP WORKERS CONNECTIONS...'
        self._init_workers_connections()

        print 'INITIALIZING CLUSTER STATE...'
        self._init_workers_states()

        print 'STARTING ALL PROCESSES ON THE CLUSTER...'
        self._start_workers_processes()

        end_time = time.time()
        print 'START CLUSTER TIME: %.2f sec' % (end_time - start_time)

    def _init_worker_management(self, location):
        attempts = 0
        management_start_command = self._create_management_start_command(location, self.management_port)
        os.system(management_start_command)
        sleep(1.0)

        while True:

            # sometimes the sockets die and need to be restarted with an attempted connection.
            try:
                connection = rpyc.classic.connect(location, self.management_port, keepalive=True)
                break

            except socket.error as error:
                errorcode = error[0]

                if errorcode == errno.ECONNREFUSED:
                    print 'ATTEMPT FAILED. RETRYING:', location
                    attempts = attempts + 1

                else:
                    # raise Exception('Problem with socket...')
                    raise
                    sys.exit(0)

            if attempts > 2:
                print 'CONNECTION FAILED. EXITING:', location
                sys.exit(0)

        self.management_connections[location] = connection
        print 'STARTED:', management_start_command

    def _init_worker_directories(self, location):
        connection = self.management_connections[location]
        code_dir_exists = connection.modules.os.path.exists(self.code_dir)
        base_dir_exists = connection.modules.os.path.exists(self.base_dir)
        working_dir_exists = connection.modules.os.path.exists(self.working_dir)
        base_projects_dir_exists = connection.modules.os.path.exists(self.base_projects_dir)
        logs_dir_exists = connection.modules.os.path.exists(self.logs_dir)
        auxiliary_dir_exists = connection.modules.os.path.exists(self.auxiliary_dir)

        if not code_dir_exists:
            connection.modules.os.mkdir(self.code_dir)
            print 'MADE:', location, self.code_dir
        else:
            print 'EXISTS:', location, self.code_dir

        if not base_dir_exists:
            connection.modules.os.mkdir(self.base_dir)
            print 'MADE:', location, self.base_dir
        else:
            print 'EXISTS:', location, self.base_dir

        # need to reset the state of the cluster, e.g. remove KILL.data, etc.
        if not working_dir_exists:
            connection.modules.os.mkdir(self.working_dir)
            print 'MADE:', location, self.working_dir
        else:
            connection.modules.shutil.rmtree(self.working_dir, ignore_errors=True)
            connection.modules.os.mkdir(self.working_dir)
            print 'DELETED AND REMADE:', location, self.working_dir

        if not base_projects_dir_exists:
            connection.modules.os.mkdir(self.base_projects_dir)
            print 'MADE:', location, self.base_projects_dir
        else:
            print 'EXISTS:', location, self.base_projects_dir

        # need to reset the state of the cluster.
        if not logs_dir_exists:
            connection.modules.os.mkdir(self.logs_dir)
            print 'MADE:', location, self.logs_dir
        else:
            connection.modules.shutil.rmtree(self.logs_dir, ignore_errors=True)
            connection.modules.os.mkdir(self.logs_dir)
            print 'DELETED AND REMADE:', location, self.logs_dir

        if not auxiliary_dir_exists:
            connection.modules.os.mkdir(self.auxiliary_dir)
            print 'MADE:', location, self.auxiliary_dir
        else:
            print 'EXISTS:', location, self.auxiliary_dir

    def _push_worker_code(self, location):
        connection = self.management_connections[location]
        push_code_command = self.push_code_command.replace('LOCATION', location)
        push_code_command = push_code_command.replace('LOCAL_DIR', self.local_code_dir)
        push_code_command = push_code_command.replace('CLUSTER_DIR', self.code_dir)
        os.system(push_code_command)
        create_configs_command = self.create_configs_command.replace('CLUSTER_DIR', self.code_dir)
        connection.modules.os.system(create_configs_command)
        print 'PUSHED CODE:', push_code_command
        print 'CONFIGURED:', create_configs_command

    def _init_worker(self, location):
        self._init_worker_management(location)
        self._init_worker_directories(location)
        self._push_worker_code(location)

    def _init_workers(self):
        self.management_connections = {}
        self.processes = {}

        workers = []

        for location in self.locations:
            process = Thread(target=self._init_worker, args=[location])

            print 'INIT WORKER', location
            process.start()
            workers.append(process)

        for process in workers:
            process.join()

    def _init_worker_connection(self, server_name):
        location = self.server_names[server_name]['LOCATION']
        port = self.server_names[server_name]['PORT']
        connection = rpyc.classic.connect(location, port, keepalive=True)
        connection.modules.sys.path.append(self.code_dir)
        self.connections[server_name] = connection
        print 'CONNECTED:', server_name

    def _init_workers_connections(self):
        self.connections = {}

        workers = []

        for server_name in self.server_names:
            process = Thread(target=self._init_worker_connection, args=[server_name])

            process.start()
            workers.append(process)

        for process in workers:
            process.join()

    def _init_workers_states(self):
        workers = []

        for location in self.management_connections:
            process = Thread(target=self._init_worker_state, args=[location])

            process.start()
            workers.append(process)

        for process in workers:
            process.join()

    def _init_worker_state(self, location):
        self._initialize_worker_job_state(location)
        self._initialize_worker_process_state(location)
        self._initialize_worker_exception_state(location)

    def _initialize_worker_job_state(self, location):
        connection = self.management_connections[location]
        fn = self.working_dir + '/JOB_STATE.data'
        f = connection.modules.__builtin__.open(fn, 'w')
        s = json.dumps('')
        f.write(s)
        f.close()

    def _initialize_worker_process_state(self, target_location):
        process_state = {}
        for server_name in self.server_names:
            location = self.server_names[server_name]['LOCATION']

            if location == target_location:
                s = json.dumps('')
                process_state['PROCESS|' + server_name] = s

        connection = self.management_connections[target_location]
        fn = self.working_dir + '/PROCESS_STATE.data'
        f = connection.modules.__builtin__.open(fn, 'w')
        s = json.dumps(process_state)
        f.write(s)
        f.close()

    def _initialize_worker_exception_state(self, target_location):
        exception_state = {}
        for server_name in self.server_names:
            location = self.server_names[server_name]['LOCATION']

            if location == target_location:
                s = json.dumps('')
                exception_state['EXCEPTION|' + server_name] = s

        connection = self.management_connections[target_location]
        fn = self.working_dir + '/EXCEPTION_STATE.data'
        f = connection.modules.__builtin__.open(fn, 'w')
        s = json.dumps(exception_state)
        f.write(s)
        f.close()

    def _start_workers_processes(self):
        self.processes = {}

        workers = []

        for server_name in self.server_names:
            process = Thread(target=self._start_worker_processes, args=[server_name])

            process.start()
            workers.append(process)

        for process in workers:
            process.join()

    def _start_worker_processes(self, server_name):
        print 'INITIALIZING CLUSTER PROCESS:', server_name
        server_id = self.server_names[server_name]['SERVER_ID']
        location = self.server_names[server_name]['LOCATION']
        connection = self.connections[server_name]
        process = connection.modules.process.Process(server_name, server_id, location, self.server_names)
        process_async = rpyc.async_(process.process)
        self.processes[server_name] = process_async()

    def _create_local_dirs(self):

        if not os.path.exists(self.local_projects_dir):
            os.mkdir(self.local_projects_dir)
            print 'CREATED LOCAL PROJECTS DIR:', self.local_projects_dir

        if not os.path.exists(self.local_working_dir):
            os.mkdir(self.local_working_dir)
            print 'CREATED LOCAL WORKING DIR:', self.local_working_dir

    def _wait_for(self, processes_pool):
        print 'WAIT FOR CONNECTIONS'

        # wait and check for all connections to be established
        while True:
            sleep(5)
            non_ready_processes = 0

            for server_id in processes_pool:
                p = processes_pool[server_id]
                if p.poll() == None:
                    non_ready_processes += 1

            if non_ready_processes > 0:
                total = len(processes_pool.keys())
                done = ((total - non_ready_processes) / total) * 100
                print 'DONE %.1f%%' % done

            else:
                print 'DONE 100%'
                break

        sleep(2)

    def _start_cluster_management(self):
        connections_pool = {}

        for location in self.locations:
            management_start_command = self._create_management_start_command(location, self.management_port)

            connections_pool[location] = subprocess.Popen(management_start_command, shell=True)
            print 'STARTED:', management_start_command

        self._wait_for(connections_pool)

        self.management_connections = {}
        for location in connections_pool:
            self.management_connections[location] = rpyc.classic.connect(location, self.management_port, keepalive=True)

    # from config file: "ssh LOCATION "screen -d -m -S 'cluster_management' rpyc_classic.py -m threaded -p PORT""
    def _create_management_start_command(self, location, port):
        management_start_command = self.management_start_command.replace('LOCATION', location)
        management_start_command = management_start_command.replace('PORT', str(port))
        return management_start_command

    def _start_cluster(self):
        self.start_cluster_processes = {}
        self.server_names = {}
        server_id = 0

        # start cluster processes
        for location in self.locations:
            for i in range(self.number_of_servers_per_location):
                port = self.base_port + server_id
                start_command = self._create_start_command(location, port, server_id)

                self.start_cluster_processes[server_id] = subprocess.Popen(start_command, shell=True)
                self._add_server_name(server_id, location, port)
                server_id = server_id + 1
                print 'STARTED:', start_command

        self._wait_for(self.start_cluster_processes)

    # from config file: "ssh LOCATION "screen -d -m -S 'cluster_SERVER_ID' rpyc_classic.py -m threaded -p PORT""
    def _create_start_command(self, location, port, server_id):
        start_command = self.start_command.replace('LOCATION', location)
        start_command = start_command.replace('PORT', str(port))
        start_command = start_command.replace('SERVER_ID', str(server_id))
        return start_command

    def _add_server_name(self, server_id, location, port):
        server_name = 'S_' + str(server_id)
        self.server_names[server_name] = {}
        self.server_names[server_name]['SERVER_ID'] = server_id
        self.server_names[server_name]['LOCATION'] = location
        self.server_names[server_name]['PORT'] = port

    def _push_project_to_worker(self, location, local_dir):
        connection = self.management_connections[location]
        cluster_dir = self.base_projects_dir + '/' + self.project_name

        if not connection.modules.os.path.exists(cluster_dir):
            print 'CREATE LOCATION DIR:', location, cluster_dir
            connection.modules.os.mkdir(cluster_dir)

        push_code_command = self.push_code_command.replace('LOCATION', location)
        push_code_command = push_code_command.replace('LOCAL_DIR', local_dir)
        push_code_command = push_code_command.replace('CLUSTER_DIR', cluster_dir)

        os.system(push_code_command)
        print 'PUSHED PROJECT:', push_code_command

    def _push_project_to_cluster(self):
        local_dir = self.local_projects_dir + '/' + self.project_name

        if not os.path.exists(local_dir):
            raise Exception('MISSING PROJECT: ' + local_dir)

        workers = []
        for location in self.locations:
            process = Thread(target=self._push_project_to_worker, args=[location, local_dir])
            process.start()
            workers.append(process)

        for process in workers:
            process.join()

    def kill_cluster(self):
        connections_pool = {}

        for location in self.locations:
            kill_command = self.kill_command.replace('LOCATION', location)
            connections_pool[location] = subprocess.Popen(kill_command, shell=True)
            print 'KILLED:', kill_command

        self._wait_for(connections_pool)

    def run(self, job):
        # set project, push code, and initialize cluster state.
        self.job_name = job['job_name']
        self.project_name = job['project_name']
        self.delete_job_data = job['delete_job_data']
        self.number_simultaneous_transfers = job['number_simultaneous_tranfers']
        self._set_number_simultaneous_transfers()
        self._set_project_name()
        self._delete_job_data()
        self._set_job(job)

        timestamp = ctime()
        data = (self.job_name, timestamp)
        self.slack_integration.notify_slack(data, 'ATTEMPTING_JOB')

        print '-----'
        print 'MAP PHASE...'
        self.current_phase = 'MAP'
        self._start_phase()
        sleep(3.0)  # let all mappers start
        while not self._poll():
            sleep(self.poll_every)

        print '-----'
        print 'SORT PHASE...'
        self.current_phase = 'SORT'
        self._start_phase()
        while not self._poll():
            sleep(self.poll_every)

        print '-----'
        print 'REDUCE PHASE...'
        self.current_phase = 'REDUCE'
        self._start_phase()
        while not self._poll():
            sleep(self.poll_every)

        self._cleanup()

    # poll needs to i) monitor states and wait for completion of phase, ii) check for exceptions and raise if found.
    def _poll(self):
        print '\nPOLLING:', len(self.server_names), self.current_phase, ctime()
        polls = []
        errors = []
        self.exceptions = []

        # get PROCESS_STATE.data
        count_states = {}
        count_states['DONE'] = 0
        for location in self.management_connections:
            connection = self.management_connections[location]
            fn = self.working_dir + '/PROCESS_STATE.data'

            read_attempts = 0
            while True:
                f = connection.modules.__builtin__.open(fn, 'r')
                my_lock = self._lock_remote_file(f, connection)

                if my_lock:
                    try:
                        s = f.read()

                    except:
                        print 'TIMEOUT ERROR:', location
                        read_attempts = read_attempts + 1

                        if read_attempts < 3:
                            sleep(3.0)
                            continue

                        else:
                            raise

                    try:
                        process_state = json.loads(s)

                    except ValueError:
                        self._unlock_remote_file(f, connection)
                        f.close()
                        print 'JSON CORRUPT. RETRYING...'
                        sleep(0.2)
                        continue

                    self._unlock_remote_file(f, connection)
                    f.close()
                    for server_name in process_state:

                        s = process_state[server_name]
                        state = json.loads(s)

                        if state == 'DONE':
                            poll = True
                            polls.append(poll)
                            count_states['DONE'] = count_states['DONE'] + 1
                            # print '\t' + server_name + ' FINISHED'
                        else:
                            poll = False
                            polls.append(poll)
                            # print '\t' + server_name + ' NOT FINISHED'

                    break

                else:
                    f.close()
                    sleep(0.2)

        # report cluster state
        print 'DONE:', count_states['DONE']

        # get EXCEPTION_STATE.data
        for location in self.management_connections:
            connection = self.management_connections[location]
            fn = self.working_dir + '/EXCEPTION_STATE.data'

            while True:
                f = connection.modules.__builtin__.open(fn, 'r')
                my_lock = self._lock_remote_file(f, connection)

                if my_lock:
                    s = f.read()

                    try:
                        exception_state = json.loads(s)

                    except ValueError:
                        self._unlock_remote_file(f, connection)
                        f.close()
                        print 'JSON CORRUPT. RETRYING...'
                        sleep(0.2)
                        continue

                    self._unlock_remote_file(f, connection)
                    f.close()

                    for server_name in exception_state:

                        # all states throw an exception. just include true exceptions, not exceptions used for exiting.
                        s = exception_state[server_name]
                        state = json.loads(s)
                        if not state or 'EXITING...' in str(state):
                            error = False
                            errors.append(error)

                        else:
                            error = True
                            errors.append(error)
                            self.exceptions.append(state)

                    break

                else:
                    f.close()
                    sleep(0.2)

        if False not in polls and True not in errors:
            print 'FINISHED:', ctime()
            return True
        elif True in errors:
            print 'FOUND EXCEPTION:', ctime()
            self._check_exceptions()
        else:
            print 'CONTINUE:', ctime()
            return False

    def _start_phase(self):
        for target_location in self.management_connections:
            process_state = {}
            for server_name in self.server_names:
                location = self.server_names[server_name]['LOCATION']

                if location == target_location:
                    s = json.dumps('START')
                    process_state['PROCESS|' + server_name] = s

            while True:
                fn = self.working_dir + '/i_own_process_state.data'
                connection = self.management_connections[target_location]
                flock = connection.modules.__builtin__.open(fn, 'w')
                my_lock = self._lock_remote_file(flock, connection)

                if my_lock:
                    fn = self.working_dir + '/PROCESS_STATE.data'
                    f = connection.modules.__builtin__.open(fn, 'w')
                    s = json.dumps(process_state)
                    f.write(s)
                    f.close()
                    self._unlock_remote_file(flock, connection)
                    flock.close()
                    break

                else:
                    sleep(0.1)

    def _set_project_name(self):
        project_dir = os.path.expanduser('~') + '/projects/' + self.project_name

        if os.path.exists(project_dir):
            self.project_dir = project_dir
            sys.path.insert(0, self.project_dir)

            self._push_project_to_cluster()
            print 'PROJECT SET:', self.project_dir

        else:
            print 'PROJECT NOT FOUND:', project_dir
            Exception('PROJECT NOT FOUND: ' + project_dir)

    def _set_job(self, job):
        for location in self.management_connections:
            connection = self.management_connections[location]
            fn = self.working_dir + '/i_own_job_state.data'
            flock = connection.modules.__builtin__.open(fn, 'w')

            while True:
                my_lock = self._lock_remote_file(flock, connection)

                if my_lock:
                    fn = self.working_dir + '/JOB_STATE.data'
                    f = connection.modules.__builtin__.open(fn, 'w')
                    s = json.dumps(job)
                    f.write(s)
                    f.close()
                    self._unlock_remote_file(flock, connection)
                    flock.close()
                    print 'WROTE JOB STATE AT LOCATION:', location
                    break

                else:
                    sleep(0.1)

    def _set_number_simultaneous_transfers(self):
        for location in self.management_connections:
            tokens = {}
            for i in range(self.number_simultaneous_transfers):
                token = str(uuid4())
                tokens[token] = True

            connection = self.management_connections[location]
            fn = self.working_dir + '/TRANSFER_TOKENS.data'
            f = connection.modules.__builtin__.open(fn, 'w')
            while True:
                my_lock = self._lock_remote_file(f, connection)

                if my_lock:
                    s = json.dumps(tokens)
                    f.write(s)
                    self._unlock_remote_file(f, connection)
                    f.close()
                    print 'SET NUMBER SIMULTANEOUS TRANSFERS:', location
                    break

                else:
                    print 'ATTEMPTING TO LOCK NUMBER SIMULTANEOUS TRANSFERS FOR LOCATION:', location
                    sleep(0.1)

    def _check_exceptions(self):
        # needs to be fixed at some point.
        try:
            exception = self.exceptions[0].strip() + '\nPHASE: ' + self.exceptions[1]

        except IndexError:
            exception = self.exceptions

        print 'EXCEPTION:', exception
        sleep(10.0)
        self.exceptions = []
        self._cleanup()
        raise Exception(exception)

    def _delete_job_data(self):
        # delete job dir
        if self.delete_job_data:

            job_dir = self.base_dir + '/' + self.job_name
            for location in self.management_connections:
                connection = self.management_connections[location]
                if connection.modules.os.path.exists(job_dir):
                    connection.modules.shutil.rmtree(job_dir, ignore_errors=True)
                    print 'DELETED:', location, job_dir
                else:
                    print 'NOT FOUND:', location, job_dir

        # cleanup ./map before starting job
        map_dir = self.base_dir + '/' + self.job_name + '/map'
        for location in self.management_connections:
            connection = self.management_connections[location]
            if connection.modules.os.path.exists(map_dir):
                connection.modules.shutil.rmtree(map_dir, ignore_errors=True)
                print 'DELETED:', location, map_dir
            else:
                print 'NOT FOUND:', location, map_dir

    def _cleanup(self):
        # write kill file so processes know to exit. do this first to give processes time to observe the file.
        for location in self.management_connections:
            connection = self.management_connections[location]

            # write kill file so processes know to exit
            fn = self.working_dir + '/KILL.data'
            f = connection.modules.__builtin__.open(fn, 'w')
            f.close()

            print 'WROTE KILL FILE:', location

        # confirm all processes are killed and dont continue altering the underlying state of the cluster, i.e. no race conditions.
        fn = self.working_dir + '/KILL_REPORT.data'
        while True:
            kill_report = set([])
            checked_all_locations = True
            for location in self.management_connections:
                connection = self.management_connections[location]

                if not connection.modules.os.path.exists(fn):
                    checked_all_locations = False
                    print 'FILE DOES NOT YET EXIST:', location, fn
                    break

                f = connection.modules.__builtin__.open(fn, 'r')
                s = f.read()
                f.close()
                number_killed_processes = len(set(s.strip().split('\n')))
                kill_report.add(number_killed_processes)

                print 'CHECKING KILL REPORT:', location, number_killed_processes

            if checked_all_locations and len(kill_report) == 1 and self.number_of_servers_per_location in kill_report:
                print 'PROCESSES ON ALL MACHINES HAVE BEEN KILLED...'
                break

            else:
                print 'TRYING TO KILL ALL PROCESSES. KILL REPORT:', kill_report
                sleep(1.0)

        # cleanup workers
        self._cleanup_workers()

        # reset cluster to initial state
        self._init_workers_states()
        self._start_workers_processes()

    def _cleanup_workers(self):
        workers = []

        for location in self.management_connections:
            process = Thread(target=self._cleanup_worker, args=[location])

            process.start()
            workers.append(process)

        for process in workers:
            process.join()

    def _cleanup_worker(self, location):
        connection = self.management_connections[location]

        # cleanup map folder
        map_dir = self.base_dir + '/' + self.job_name + '/map'
        if connection.modules.os.path.exists(map_dir):
            connection.modules.shutil.rmtree(map_dir, ignore_errors=True)
            print 'DELETED:', location, map_dir

        else:
            print 'NOT FOUND:', location, map_dir

        # cleanup working_dir and kill processes
        attempts = 0
        sleep_time = 2

        print 'CLEANING UP WORKING DIR:', location

        while True:

            try:
                connection.modules.shutil.rmtree(self.working_dir, ignore_errors=True)
                connection.modules.os.mkdir(self.working_dir)
                print 'DELETED AND REMADE:', location, self.working_dir
                delete_status = True

            except:
                print 'FAILED TO DELETE AND REMAKE:', location, self.working_dir
                delete_status = False

            if delete_status:
                print 'KILLED ALL PROCESSES SUCCESSFULLY FOR:', location
                break

            elif not delete_status and attempts < 4:
                print 'STILL TRYING TO KILL PROCESSES FOR:', location, ' ATTEMPT:', attempts
                sleep(sleep_time ** attempts)

            else:
                print 'KILL PROCESSES FAILED FOR:', location, ' CLUSTER IS IN A HANGING STATE.'
                break

            attempts = attempts + 1

    def _lock_remote_file(self, remote_file, connection):
        try:
            # attempt lock
            connection.modules.fcntl.flock(remote_file,
                                           connection.modules.fcntl.LOCK_EX | connection.modules.fcntl.LOCK_NB)
            return True
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False

    def _unlock_remote_file(self, remote_file, connection):
        connection.modules.fcntl.flock(remote_file, connection.modules.fcntl.LOCK_UN)
