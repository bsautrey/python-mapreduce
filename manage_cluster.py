# manage_cluster.py for starting and stopping the cluster.

import os,sys,subprocess,random
from time import sleep,ctime

import ujson,rpyc,redis

from configs_parser import get_configs


class ManageCluster():
    
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
        self.redis_restart_command = configs['redis_restart_command']
        self.management_start_command = configs['management_start_command']
        self.push_code_command = configs['push_code_command']
        self.create_configs_command = configs['create_configs_command']
        self.code_dir = configs['code_dir']
        self.base_dir = configs['base_dir']
        self.working_dir = configs['working_dir']
        self.base_projects_dir = configs['base_projects_dir']
        self.logs_dir = configs['logs_dir']
        self.auxiliary_dir = configs['auxiliary_dir']
        self.poll_every = configs['poll_every']
        self.redis_auth = configs['redis_auth']
        
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
        self.redis = None
        self.management_redis = None
        self.exceptions = None
        
    def start_cluster(self):
        print 'STARTING CLUSTER MANAGEMENT...'
        self._start_cluster_management()
        
        print 'SETTING CLUSTER DIRECTORIES...'
        self._set_cluster_directories()
        
        print 'PUSH CODE...'
        self._push_code_to_cluster()
        
        print 'CREATING SERVER REFERENCES AND CONNECTING TO CLUSTER...'
        self._start_cluster()
        
        print 'RESTARTING REDIS...'
        self._restart_redis()
        
        print 'STARTING ALL PROCESSES ON THE CLUSTER...'
        self._initialize_processes()
                
    def _start_cluster_management(self):
        self.management_connections = {}
        for location in self.locations:
            management_start_command = self._create_management_start_command(location,self.management_port)
            os.system(management_start_command)
            sleep(1.0)
            connection = rpyc.classic.connect(location,self.management_port)
            self.management_connections[location] = connection
            print 'STARTED:',management_start_command
        
    # from config file: "ssh LOCATION "screen -d -m -S 'cluster_management' rpyc_classic.py -m threaded -p PORT""
    def _create_management_start_command(self,location,port):
        management_start_command = self.management_start_command.replace('LOCATION',location)
        management_start_command = management_start_command.replace('PORT',str(port))
        return management_start_command
        
    def _start_cluster(self):
        self.server_names = {}
        server_id = 0
        for location in self.locations:
            for i in xrange(self.number_of_servers_per_location):
                port = self.base_port + server_id
                start_command = self._create_start_command(location,port,server_id)
                os.system(start_command)
                self._add_server_name(server_id,location,port)
                server_id = server_id + 1
                print 'STARTED:',start_command
            
        sleep(1.0)
        self.connections = {}
        for server_name in self.server_names:
            location = self.server_names[server_name]['LOCATION']
            port = self.server_names[server_name]['PORT']
            connection = rpyc.classic.connect(location,port)
            connection.modules.sys.path.append(self.code_dir)
            self.connections[server_name] = connection
            print 'CONNECTED:',server_name
            
    # from config file: "ssh LOCATION "screen -d -m -S 'cluster_SERVER_ID' rpyc_classic.py -m threaded -p PORT""
    def _create_start_command(self,location,port,server_id):
        start_command = self.start_command.replace('LOCATION',location)
        start_command = start_command.replace('PORT',str(port))
        start_command = start_command.replace('SERVER_ID',str(server_id))
        return start_command
        
    def _add_server_name(self,server_id,location,port):
        server_name = 'S_'+str(server_id)
        self.server_names[server_name] = {}
        self.server_names[server_name]['SERVER_ID'] = server_id
        self.server_names[server_name]['LOCATION'] = location
        self.server_names[server_name]['PORT'] = port
        
    # added sleep statements as a hack to this error:
    # redis.exceptions.BusyLoadingError: Redis is loading the dataset in memory
    # https://github.com/redis/redis-rb/issues/350
    # it seems to be an issue with redis needing some time to startup.
    # EDIT: added a retry loop to handle this case better.
    # from config file: "ssh LOCATION sudo service redis-server restart"
    def _restart_redis(self):
        for location in self.locations:
            redis_restart_command = self.redis_restart_command.replace('LOCATION',location)
            os.system(redis_restart_command)
            print 'RESTARTED REDIS:',redis_restart_command
            
        location2server_names = {}
        for server_name in self.server_names:
            location = self.server_names[server_name]['LOCATION']
            if location not in location2server_names:
                location2server_names[location] = [server_name]
            else:
                location2server_names[location].append(server_name)
        
        self.redis = {}
        self.management_redis = {}
        for location in location2server_names:
            redis_management_set = False
            print 'STARTING STATE SERVER:',location
            r = redis.Redis(location,password=self.redis_auth)
            server_names = location2server_names[location]
            for server_name in server_names:
                self.redis[server_name] = r
                if not redis_management_set:
                    self.management_redis[location] = r
                    redis_management_set = True
                
        self._initialize_redis()
                
    def _initialize_redis(self):
        
        attempts = 0
        wait_time = 2.0
        while attempts < 4:
            
            try:
                self._flush_redis()
                for server_name in self.redis:
                    r = self.redis[server_name]
                    s = ujson.dumps('')
                    val = r.set('JOB',s)
                    val = r.set('PROCESS|'+server_name,s)
                    val = r.set('EXCEPTION|'+server_name,s)
                return
                
            except redis.exceptions.BusyLoadingError:
                if attempts < 4:
                    print 'REDIS NOT AVAILABLE/ATTEMPT:',attempts
                    sleep(wait_time)
                    wait_time = wait_time**2
                    attempts = attempts + 1
                else:
                    raise
        
    def _set_cluster_directories(self):
        for location in self.management_connections:
            connection = self.management_connections[location]
            code_dir_exists = connection.modules.os.path.exists(self.code_dir)
            base_dir_exists = connection.modules.os.path.exists(self.base_dir)
            working_dir_exists = connection.modules.os.path.exists(self.working_dir)
            base_projects_dir_exists = connection.modules.os.path.exists(self.base_projects_dir)
            logs_dir_exists = connection.modules.os.path.exists(self.logs_dir)
            auxiliary_dir_exists = connection.modules.os.path.exists(self.auxiliary_dir)
            
            if not code_dir_exists:
                connection.modules.os.mkdir(self.code_dir)
                print 'MADE:',location,self.code_dir
            else:
                print 'EXISTS:',location,self.code_dir
                
            if not base_dir_exists:
                connection.modules.os.mkdir(self.base_dir)
                print 'MADE:',location,self.base_dir
            else:
                print 'EXISTS:',location,self.base_dir
                
            if not working_dir_exists:
                connection.modules.os.mkdir(self.working_dir)
                print 'MADE:',location,self.working_dir
            else:
                print 'EXISTS:',location,self.working_dir
                
            if not base_projects_dir_exists:
                connection.modules.os.mkdir(self.base_projects_dir)
                print 'MADE:',location,self.base_projects_dir
            else:
                print 'EXISTS:',location,self.base_projects_dir
                
            if not logs_dir_exists:
                connection.modules.os.mkdir(self.logs_dir)
                print 'MADE:',location,self.logs_dir
            else:
                print 'EXISTS:',location,self.logs_dir
                
            if not auxiliary_dir_exists:
                connection.modules.os.mkdir(self.auxiliary_dir)
                print 'MADE:',location,self.auxiliary_dir
            else:
                print 'EXISTS:',location,self.auxiliary_dir
                
    def _push_code_to_cluster(self):
        local_dir = os.path.expanduser('~') +'/code'
        for location in self.management_connections:
            connection = self.management_connections[location]
            push_code_command = self.push_code_command.replace('LOCATION',location)
            push_code_command = push_code_command.replace('LOCAL_DIR',local_dir)
            push_code_command = push_code_command.replace('CLUSTER_DIR',self.code_dir)
            os.system(push_code_command)
            create_configs_command = self.create_configs_command.replace('CLUSTER_DIR',self.code_dir)
            connection.modules.os.system(create_configs_command)
            print 'PUSHED CODE:',push_code_command
            print 'CONFIGURED:',create_configs_command
            
    def _push_project_to_cluster(self):
        # push
        local_dir = os.path.expanduser('~') +'/projects/'+self.project_name
        for location in self.management_connections:
            connection = self.management_connections[location]
            cluster_dir = self.base_projects_dir +'/'+ self.project_name
            if not connection.modules.os.path.exists(cluster_dir):
                connection.modules.os.mkdir(cluster_dir)
            push_code_command = self.push_code_command.replace('LOCATION',location)
            push_code_command = push_code_command.replace('LOCAL_DIR',local_dir)
            push_code_command = push_code_command.replace('CLUSTER_DIR',cluster_dir)
            os.system(push_code_command)
            print 'PUSHED PROJECT:',push_code_command
            
    def _initialize_processes(self):
        self.processes = {}
        for server_name in self.server_names:
            print 'INITIALIZING CLUSTER PROCESS:',server_name
            server_id = self.server_names[server_name]['SERVER_ID']
            location = self.server_names[server_name]['LOCATION']
            connection = self.connections[server_name]
            process = connection.modules.process.Process(server_name,server_id,location,self.server_names)
            process_async = rpyc.async(process.process)
            self.processes[server_name] = process_async()
                
    def _set_project_name(self):
        project_dir = os.path.expanduser('~') +'/projects/'+ self.project_name
        if os.path.exists(project_dir):
            self.project_dir = project_dir
            sys.path.insert(0,self.project_dir)
            self._push_project_to_cluster()
            print 'PROJECT SET:',self.project_dir
        else:
            print 'PROJECT NOT FOUND:',project_dir
            Exception('PROJECT NOT FOUND: '+project_dir)       
            
    def _start_phase(self):
        for server_name in self.redis:
            s = ujson.dumps('START')
            r = self.redis[server_name]
            val = r.set('PROCESS|'+server_name,s)
            print 'STARTED:',server_name
            
    def stop_cluster(self):
        print 'STOP CLUSTER MANAGEMENT...'
        for location in self.management_connections:
            connection = self.management_connections[location]
            connection.modules.thread.interrupt_main()
            connection.close()
            print 'STOPPED MANAGEMENT:',location
        
        print 'STOP CLUSTER SERVERS...'
        for server_name in self.connections:
            connection = self.connections[server_name]
            connection.modules.thread.interrupt_main()
            connection.close()
            print 'STOPPED:',server_name
            
    def kill_cluster(self):
        for location in self.locations:
            kill_command = '''ssh LOCATION "pkill screen"'''.replace('LOCATION',location)
            os.system(kill_command)
            print 'KILLED:',kill_command
        
    def run(self,job):
        # set project and push code
        self.job_name = job['job_name']
        self.project_name = job['project_name']
        self.delete_job_data = job['delete_job_data']
        self._set_project_name()
        self._delete_job_data()
        
        # set job in redis
        s = ujson.dumps(job)
        for server_name in self.redis:
            r = self.redis[server_name]
            val = r.set('JOB',s)
        
        print '-----'
        print 'MAP PHASE...'
        self.current_phase = 'MAP'
        self._start_phase()
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

    # poll needs to i) monitor redis and wait for completion of phase, ii) check for exceptions and raise if found.
    def _poll(self):
        print '\nPOLLING:',len(self.redis),ctime()
        polls = []
        errors = []
        self.exceptions = []
        for server_name in self.redis:
            r = self.redis[server_name]
            s = r.get('PROCESS|'+server_name)
            val = ujson.loads(s)
            if val == 'DONE':
                poll = True
                polls.append(poll)
            else:
                poll = False
                polls.append(poll)
                
            s = r.get('EXCEPTION|'+server_name)
            val = ujson.loads(s)
            if not val:
                error = False
                errors.append(error)
            else:
                error = True
                errors.append(error)
                self.exceptions.append(val)
                
            print '\t',poll,error,server_name
        
        if False not in polls and True not in errors:
            return True
        elif True in errors:
            self._check_exceptions()
        else:
            return False
            
    def _check_exceptions(self):
        if self.exceptions:
            exception = self.exceptions[0]
            print 'EXCEPTION:',exception
            self.exceptions = []
            self._cleanup()
            raise Exception(exception)
            #self._flush_redis()
            #sys.exit()
            
    def _flush_redis(self):
        for location in self.management_redis:
            print 'FLUSHING:',location
            r = self.management_redis[location]
            r.flushall()
            
    def _delete_job_data(self):
        # delete job dir
        if self.delete_job_data:
            
            job_dir = self.base_dir +'/'+ self.job_name
            for location in self.management_connections:
                connection = self.management_connections[location]
                if connection.modules.os.path.exists(job_dir):
                    connection.modules.shutil.rmtree(job_dir,ignore_errors=True)
                    print 'DELETED:',location,job_dir
                else:
                    print 'NOT FOUND:',location,job_dir
                    
        # cleanup ./map before starting job
        map_dir = self.base_dir +'/'+ self.job_name +'/map'
        for location in self.management_connections:
            connection = self.management_connections[location]
            if connection.modules.os.path.exists(map_dir):
                connection.modules.shutil.rmtree(map_dir,ignore_errors=True)
                print 'DELETED:',location,map_dir
            else:
                print 'NOT FOUND:',location,map_dir
             
    def _cleanup(self):
        
        # cleanup working_dir
        for location in self.management_connections:
            connection = self.management_connections[location]

            try:
                connection.modules.shutil.rmtree(self.working_dir,ignore_errors=True)
                connection.modules.os.mkdir(self.working_dir)
                print 'CLEANUP LOCATION:',location
            except:
                print 'CLEANUP ERRORS:',location
                 
        # cleanup ./map
        map_dir = self.base_dir +'/'+ self.job_name +'/map'
        for location in self.management_connections:
            connection = self.management_connections[location]
            if connection.modules.os.path.exists(map_dir):
                connection.modules.shutil.rmtree(map_dir,ignore_errors=True)
                print 'DELETED:',location,map_dir
            else:
                print 'NOT FOUND:',location,map_dir
                
        # reset redis
        self._initialize_redis()
          