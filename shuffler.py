# shuffler.py takes output from the mapper and writes the data to a file that will be transferred/shuffled to a specific machine, for a specific process/server.
# the two main functions are append() and shuffle().

import uuid,os,random,fcntl,errno,sys
from math import fmod
from copy import copy
from time import sleep

import ujson,redis

from configs_parser import get_configs


class Shuffle():
    
    def __init__(self,job_name,server_names,server_id,max_number_dumped_items_shuffler=500000,simultaneous_files_in_redis=10):
        # configs
        configs = get_configs(self.__module__)
        self.base_dir = configs['base_dir']
        self.code_dir = configs['code_dir']
        self.working_dir = configs['working_dir']
        self.max_hash_cache_size = configs['max_hash_cache_size']
        self.redis_auth = configs['redis_auth']
        
        # variables
        self.job_name = job_name
        self.server_names = server_names
        self.my_server_id = server_id
        self.number_of_servers = len(self.server_names)
        self.max_number_dumped_items_shuffler = max_number_dumped_items_shuffler
        self.simultaneous_files_in_redis = simultaneous_files_in_redis
        
        # shared references
        self.redis_token = None
        self.pending_file_names = set([])
        self.pending_file_name2server_name = {}
        self.processed_file_names = {}
        self.processed_server_names = set([])
        self.hash_cache = {}
        self.files = {}
        self.file_names = {}
        self.have_seen_file_names = set([])
        self.number_dumped_items = {}
        self.server_id2server_name = {}
        self.location2connection = {}
        for server_name in self.server_names:
            server_id = self.server_names[server_name]['SERVER_ID']
            file_name,file = self._get_new_file_reference(server_id)
            self.files[server_id] = file
            self.file_names[server_id] = [file_name]
            self.number_dumped_items[server_id] = 0
            self.server_id2server_name[server_id] = server_name
            location = server_names[server_name]['LOCATION']
            if location not in self.location2connection:
                connection = redis.Redis(location,password=self.redis_auth)
                self.location2connection[location] = connection
            
        self.my_server_name = self.server_id2server_name[self.my_server_id]
        self.my_location = self.server_names[self.my_server_name]['LOCATION']
        
        self.connections = {}
        for server_name in self.server_names:
            location = self.server_names[server_name]['LOCATION']
            connection = self.location2connection[location]
            self.connections[server_name] = connection
            if server_name == self.my_server_name:
                self.my_connection = connection
                
        self._make_dir()
            
    def append(self,key,item):
        h = self._hash(key)
        server_id = fmod(h,self.number_of_servers)
        file = self.files[server_id]
        s = ujson.dumps(item)
        file.write(s+'\n')
        self._check_file(server_id)
        
    def _check_file(self,server_id):
        self.number_dumped_items[server_id] = self.number_dumped_items[server_id] + 1
        if fmod(self.number_dumped_items[server_id],self.max_number_dumped_items_shuffler) == 0:
            file = self.files[server_id]
            file.close()
            file_name,file = self._get_new_file_reference(server_id)
            self.files[server_id] = file
            self.file_names[server_id].append(file_name)
        
    def _get_new_file_reference(self,server_id):
        file_name = self.working_dir +'/SHUFFLE_'+ str(uuid.uuid4()) + '.data'
        file  = open(file_name,'w')
        return file_name,file
        
    def shuffle(self):
        print 'FLUSH...'
        self._flush()
        
        print 'INITIALIZE REDIS...'
        self._initialize_redis()
        
        while True:
            
            finished_writing = False
            finished_reading = False
            
            if self.pending_file_names:
                self._write()
            else:
                key = 'READY:'+self.my_server_name
                s = ujson.dumps('IS_FINISHED')
                val = self.my_connection.set(key,s)
                finished_writing = True
                
            server_names = set(self.server_names.keys())
            if self.processed_server_names != server_names:
                self._read()
            else:
                finished_reading = True
                
            if finished_writing and finished_reading:
                break
            else:
                sleep(0.5)
                
        self.hash_cache = {}
                   
    # read files from redis that have been submitted by other processes in the cluster.
    def _read(self):
        for server_name in self.server_names:
            
            if server_name not in self.processed_server_names:
                status = self._get_connection_status(server_name)
                if status == 'IS_READY' or status == 'IS_FINISHED':
                    pending_file_names = self._get_pending_file_names(server_name)
                    for pending_file_name in pending_file_names:
                        connection = self.connections[server_name]
                        s = connection.get(pending_file_name)
                        output_file_name = self._get_output_file_name()
                        f = open(output_file_name,'w')
                        f.write(s)
                        f.close()
                        
                        val = connection.delete(pending_file_name)
                        self.have_seen_file_names.add(pending_file_name)
                        self._deincrement_files_in_redis(server_name)
                        print 'READ:',pending_file_name
                    
                    if status == 'IS_FINISHED':
                        self.processed_server_names.add(server_name)
                
                elif status == 'NOT_READY':
                    print status,server_name
                  
    # write files to redis for pickup by other processes in the cluster.                  
    def _write(self):
        remove_files = set([])
        for pending_file_name in self.pending_file_names:
            server_name = self.pending_file_name2server_name[pending_file_name]
            status = self._get_connection_status(server_name)
            if status == 'IS_READY' or status == 'IS_FINISHED':
                print 'WRITE READY...'
                files_in_redis = self._get_number_files_in_redis()
                if files_in_redis < self.simultaneous_files_in_redis:
                    self._add_file_for_pickup(server_name,pending_file_name)
                    remove_files.add(pending_file_name)
                    self._increment_files_in_redis()
                    files_in_redis = self._get_number_files_in_redis()
                    if files_in_redis >= self.simultaneous_files_in_redis:
                        break
                        
        for pending_file_name in remove_files:
            self.pending_file_names.remove(pending_file_name)
            print 'WROTE:',pending_file_name
            
    def _add_file_for_pickup(self,server_name,pending_file_name):
        attempts = 0
        while True:
            
            try:
                f = open(pending_file_name,'r')
                s = f.read()
                f.close()
                val = self.my_connection.set(pending_file_name,s)
                os.remove(pending_file_name)
        
                key = self.my_server_name +':FROM:TO:'+ server_name
                val = self.my_connection.get(key)
                pending_file_names = ujson.loads(val)
                pending_file_names.append(pending_file_name)
                s = ujson.dumps(pending_file_names)
                val = self.my_connection.set(key,s)
                print 'ADDED:',key,pending_file_names
                return
                
            except:
                print 'UNABLE TO WRITE:',pending_file_name
                if attempts < 5:
                    attempts = attempts + 1
                    sleep(1.5)
                else:
                    raise
            
    def _get_connection_status(self,server_name):
        
        key = 'READY:'+server_name
        connection = self.connections[server_name]
        val = connection.get(key)
        
        if val:
            status = ujson.loads(val)
            return status
        else:
            return 'NOT_READY'
            
    def _get_pending_file_names(self,server_name):
        pending_file_names = []
        key = 'READY:'+server_name
        connection = self.connections[server_name]
        val = connection.get(key)
        if val:
            key = server_name +':FROM:TO:'+ self.my_server_name
            val = connection.get(key)
            all_pending_file_names = ujson.loads(val)
            for pending_file_name in all_pending_file_names:
                if pending_file_name not in self.have_seen_file_names:
                    pending_file_names.append(pending_file_name)
        
        return pending_file_names
        
    def _get_number_files_in_redis(self):
        val = self.my_connection.get('FILES_IN_REDIS')
        files_in_redis = ujson.loads(val)
        for server_name in self.server_names:
            key = 'FILES_IN_REDIS:'+server_name
            val = self.my_connection.get(key)
            if val:
                deincrement = ujson.loads(val)
                files_in_redis = files_in_redis + deincrement
                
        return files_in_redis
        
    def _increment_files_in_redis(self):
        key = 'FILES_IN_REDIS'
        while True:
            if self._lock_redis():
                val = self.my_connection.get(key)
                if val:
                    files_in_redis = ujson.loads(val)
                    files_in_redis = files_in_redis + 1
                    s = ujson.dumps(files_in_redis)
                    self.my_connection.set(key,s)
                else:
                    s = ujson.dumps(0)
                    self.my_connection.set(key,s)
                self._unlock_redis()
                break
            else:
                sleep(0.1)
        
    def _deincrement_files_in_redis(self,server_name):
        key = 'FILES_IN_REDIS:'+self.my_server_name
        connection = self.connections[server_name]
        val = connection.get(key)
        if val:
            files_in_redis = ujson.loads(val)
            files_in_redis = files_in_redis - 1
        else:
            files_in_redis = -1
        s = ujson.dumps(files_in_redis)
        connection.set(key,s)
                
    def _initialize_redis(self):
        for server_name in self.server_names:
            self.processed_file_names[server_name] = set()
            server_id = self.server_names[server_name]['SERVER_ID']
            pending_file_names = self.file_names[server_id]
            for pending_file_name in pending_file_names:                        
                self.pending_file_names.add(pending_file_name)
                self.pending_file_name2server_name[pending_file_name] = server_name
                
            key = self.my_server_name +':FROM:TO:'+ server_name
            s = ujson.dumps([])
            val = self.my_connection.set(key,s)
            
            val = self.my_connection.get('FILES_IN_REDIS')
            if not val:
                self._increment_files_in_redis()
                
        key = 'READY:'+self.my_server_name
        s = ujson.dumps('IS_READY')
        val = self.my_connection.set(key,s)
            
    def _remove_empty_files(self):
        for server_name in self.server_names:
            server_id = self.server_names[server_name]['SERVER_ID']
            pending_file_names = self.file_names[server_id]
            for pending_file_name in pending_file_names:
                pending_file_size = os.path.getsize(pending_file_name)
                if pending_file_size == 0.0:
                    os.remove(pending_file_name)
                    self.file_names[server_id].remove(pending_file_name)
                            
    def _flush(self):
        for server_id in self.files:
            self.files[server_id].close()
                
    def _get_output_file_name(self):
        output_file_name = self.base_dir +'/'+ self.job_name +'/map/'+ str(self.my_server_id) +'/'+ str(uuid.uuid4()) +'.data'
        return output_file_name
        
    def _make_dir(self):
        dir_1 = self.base_dir +'/'+ self.job_name
        try:
            os.mkdir(dir_1)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise
            
        dir_2 = self.base_dir +'/'+ self.job_name +'/map'
        try:
            os.mkdir(dir_2)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise
            
        dir_3 = self.base_dir +'/'+ self.job_name +'/map/'+ str(self.my_server_id)
        try:
            os.mkdir(dir_3)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise
                
    def _lock_redis(self):
        token_name = self.working_dir+'/i_own_redis.flag'
        self.redis_token = open(token_name,'w')
        try:
            fcntl.flock(self.redis_token, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False
                
    def _unlock_redis(self):
        fcntl.flock(self.redis_token, fcntl.LOCK_UN)
        self.redis_token.close()
            
    def _hash(self,key):
        hash_cache_size = len(self.hash_cache)
        if key in self.hash_cache:
            h = self.hash_cache[key]
        elif hash_cache_size < self.max_hash_cache_size:
            random.seed(key)
            h = int(random.uniform(0,1)*10000000)
            self.hash_cache[key] = h
        else:
            random.seed(key)
            h = int(random.uniform(0,1)*10000000)
        return h
                

                