# process.py is a dumb processor. It receives job settings, then uses them to configure and run a mapreduce process.

import sys,traceback
from time import sleep
from copy import copy

import redis,ujson

from mapper import Map
from reducer import Reduce
from configs_parser import get_configs


class Process():
    
    def __init__(self,server_name,server_id,location,server_names):
        # who am i and where am i?
        self.server_name = server_name
        self.server_id = server_id
        self.location = location
        self.server_names = copy(server_names)
        
        # configs
        configs = get_configs(self.__module__)    
        redis_auth = configs['redis_auth']
        self.redis = redis.Redis(self.location,password=redis_auth)
        
        # job/project
        self.project_name = None
        self.job_name = None
        self.input_dirs = None
        self.delete_job_data = None
        
        # mapper
        self.map_function_name = None
        self.auxiliary_data_name_mapper = None
        self.hold_state = None
        self.downsample = None
        
        # shuffler
        self.max_number_dumped_items_shuffler = None
        self.simultaneous_files_in_redis = None
        
        # reduce
        self.reduce_function_name = None
        self.auxiliary_data_name_reducer = None
        self.max_number_dumped_items_reducer = None
        self.disk_based_input = None
        self.disk_based_output = None
        self.compress = None
        
        # mapreduce
        self.mapper = None
        self.reducer = None
        
        # shared objects
        self.job = None
        
    def process(self):
        
        while True:
            
            try:
        
                if self._set_job():
                    
                    self._state('START')
                    self.mapper = Map(self.map_function_name,self.project_name,self.input_dirs,self.server_id,self.job_name,self.server_names,self.hold_state,self.downsample,self.auxiliary_data_name_mapper,self.max_number_dumped_items_shuffler,self.simultaneous_files_in_redis)
                    self.mapper.map()
                    self._state('END')
                    
                    self._state('START')
                    self.reducer = Reduce(self.reduce_function_name,self.project_name,self.job_name,self.server_id,self.max_number_dumped_items_reducer,self.disk_based_input,self.disk_based_output,self.auxiliary_data_name_reducer,self.compress)
                    self.reducer.sort()
                    self._state('END')
                
                    self._state('START')
                    self.reducer.reduce()
                    self._state('END')
                    
                    self._cleanup()
                
                else:
                    sleep(0.5)
                    
            except:
                exception = traceback.format_exc()
                state = ['EXCEPTION',exception]
                self._state(state)
            
            
    def _set_job(self):
        
        s = self.redis.get('JOB')
        if s:
            
            self.job = ujson.loads(s)
            if self.job and self.job != 'DONE':
        
                # job/project
                self.project_name = self.job['project_name']
                self.job_name = self.job['job_name']
                self.input_dirs = self.job['input_dirs']
                self.delete_job_data = self.job['delete_job_data']
        
                # mapper
                self.map_function_name = self.job['map_function_name']
                self.auxiliary_data_name_mapper = self.job['auxiliary_data_name_mapper']
                self.hold_state = self.job['hold_state']
                self.downsample = self.job['downsample']
        
                # shuffler
                self.max_number_dumped_items_shuffler = self.job['max_number_dumped_items_shuffler']
                self.simultaneous_files_in_redis = self.job['simultaneous_files_in_redis']
        
                # reduce
                self.reduce_function_name = self.job['reduce_function_name']
                self.auxiliary_data_name_reducer = self.job['auxiliary_data_name_reducer']
                self.max_number_dumped_items_reducer = self.job['max_number_dumped_items_reducer']
                self.disk_based_input = self.job['disk_based_input']
                self.disk_based_output = self.job['disk_based_output']
                self.compress = self.job['compress']
                
                return True
            
    def _state(self,state):
        
        if state == 'START':
            while True:
                s = self.redis.get('PROCESS|'+self.server_name)
                redis_state = ujson.loads(s)
                if redis_state != state:
                    sleep(0.5)
                else:
                    break
            return
            
        elif state == 'END':
            s = ujson.dumps('DONE')
            val = self.redis.set('PROCESS|'+self.server_name,s)
            
        elif 'EXCEPTION' in state:
            exception = state[1]
            s = ujson.dumps(exception)
            val = self.redis.set('EXCEPTION|'+self.server_name,s)
            
            # clear job to stop process loop
            s = ujson.dumps('')
            val = self.redis.set('JOB',s)
            
    def _cleanup(self):
        self.job = 'DONE'
        s = ujson.dumps(self.job)
        val = self.redis.set('JOB',s)
        val = self.redis.set('PROCESS|'+self.server_name,s)
            
