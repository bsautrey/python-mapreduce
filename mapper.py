# mapper.py is the map step of mapreduce.

import sys,random,uuid,os,fcntl,gzip,errno
from time import time
from glob import glob
from math import fmod

import ujson

from shuffler import Shuffle
from configs_parser import get_configs

class Map():
    
    def __init__(self,map_function_name,project_name,input_dirs,server_id,job_name,server_names,hold_state=False,downsample=1.0,auxiliary_data_name=None,max_number_dumped_items_shuffler=500000,simultaneous_files_in_redis=10):
        # configs
        configs = get_configs(self.__module__)
        self.base_dir = configs['base_dir']
        self.base_projects_dir = configs['base_projects_dir']
        self.auxiliary_dir = configs['auxiliary_dir']
        self.number_of_servers_per_location = configs['number_of_servers_per_location']
        
        # variables
        self.map_function_name = map_function_name
        self.project_name = project_name
        self.input_dirs = input_dirs
        self.server_id = server_id
        self.job_name = job_name
        self.hold_state = hold_state
        self.downsample = downsample
        self.max_number_dumped_items_shuffler = max_number_dumped_items_shuffler
        self.simultaneous_files_in_redis = simultaneous_files_in_redis
        
        # shared references
        self.map_function = None
        self.shuffler = Shuffle(job_name,server_names,server_id,max_number_dumped_items_shuffler,simultaneous_files_in_redis)
        self.state = self._read_state()
        self.file_names = self._get_file_names()
        self.auxiliary_data = self._get_auxiliary_data(auxiliary_data_name)
        
    def map(self):
        print 'MAPPING...'
        
        project_path = self.base_projects_dir +'/'+ self.project_name
        if project_path in sys.path:
            sys.path.remove(project_path)
        
        sys.path.insert(0,project_path)
            
        import map_functions
        reload(map_functions)
        from map_functions import MapFunctions
        self.map_function = MapFunctions(self.map_function_name).get_map_function()
        
        for file_name in self.file_names:
            print 'READING:',file_name
            file = self._get_file_reference(file_name)
            for line in file:
                items = self.map_function(line,self.auxiliary_data)
                for item in items:
                    key = item[0]
                    self.shuffler.append(key,item)
            file.close()
            
            if self.hold_state:
                time_stamp = time()
                self.state[file_name] = time_stamp
                
        self._write_state()
            
        # shuffle
        print 'SHUFFLING...'
        self.shuffler.shuffle()

    def _get_file_names(self):
        all_file_names = []
        for input_dir in self.input_dirs:
            file_names = glob(input_dir+'/*')
            for file_name in file_names:
                if self._file_is_mine(file_name) and self._check_file(file_name):
                    all_file_names.append(file_name)
                elif self._have_seen(file_name):
                    print 'HAVE SEEN/REJECTED FILE:',file_name
        print 'ALL FILES:',all_file_names
        return all_file_names
        
    def _get_file_reference(self,file_name):
        file_extension = file_name[-3:]
        if file_extension == '.gz':
            file = gzip.open(file_name)
        else:
            file = open(file_name)
            
        return file

    def _file_is_mine(self,file_name):
        bucket_id = fmod(self.server_id,self.number_of_servers_per_location)
        h = self._hash(file_name)
        if fmod(h,self.number_of_servers_per_location) == bucket_id and self._downsample() and not self._have_seen(file_name):
            print 'FILE IS MINE:',file_name
            return True
            
    def _downsample(self):
        rand = random.uniform(0,1)
        if rand <= self.downsample:
            return True
        else:
            return False
            
    def _have_seen(self,file_name):
        if file_name in self.state:
            return True
        else:
            return False
        
    def _read_state(self):
        global_state = {}
        token = '/STATE_'+ self.job_name +'_SERVER_ID_'+str(self.server_id)+'_'
        state_file_names = filter(lambda state_file_name: token in state_file_name,glob(self.auxiliary_dir+'/*'))
        for state_file_name in state_file_names:
            if self.hold_state:
                print 'READING STATE:',state_file_name
                f = open(state_file_name)
                s = f.read()
                f.close()
                state = ujson.loads(s)
                for file_name in state:
                    print '\tINCLUDING FILE:',file_name
                    time_stamp = state[file_name]
                    global_state[file_name] = time_stamp
            
            print 'DELETING STATE:',state_file_name
            os.remove(state_file_name)
        print 'GLOBAL STATE:',global_state
        return global_state
    
    def _write_state(self):
        output_file_name = self.auxiliary_dir +'/STATE_'+ self.job_name +'_SERVER_ID_'+str(self.server_id)+'_'+ str(uuid.uuid4()) +'.data'
        f = open(output_file_name,'w')
        s = ujson.dumps(self.state)
        f.write(s)
        f.close()
        print 'WROTE STATE:',output_file_name    
            
    def _hash(self,file_name):
        random.seed(file_name)
        h = int(random.uniform(0,1)*1000000)
        return h
        
    def _get_auxiliary_data(self,auxiliary_data_name):
        if auxiliary_data_name:
            fn = self.auxiliary_dir +'/'+ auxiliary_data_name + '.data'
            f = open(fn)
            s = f.read()
            f.close()
            auxiliary_data = ujson.loads(s)
            return auxiliary_data
            
    def _check_file(self,file_name):
        file = open(file_name)
        if self._lock_file(file):
            self._unlock_file(file)
            file.close()
            return True
        else:
            file.close()
            return False
            
    def _lock_file(self,file):
        try:
            # attempt lock
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False
                
    def _unlock_file(self,file):
        fcntl.flock(file, fcntl.LOCK_UN)
    
        
        