# reducer.py is the reduce step of mapreduce.

import sys,uuid,os,gzip
from glob import glob
from math import fmod,ceil

import ujson

from disk_sort import DiskSort
from disk_list import DiskList
from configs_parser import get_configs

class Reduce():
    
    def __init__(self,reduce_function_name,project_name,job_name,server_id,max_number_dumped_items,disk_based_input=False,disk_based_output=False,auxiliary_data_name=None,compress=False):
        # configs
        configs = get_configs(self.__module__)
        self.base_dir = configs['base_dir']
        self.base_projects_dir = configs['base_projects_dir']
        self.auxiliary_dir = configs['auxiliary_dir']
        self.number_of_servers_per_location = configs['number_of_servers_per_location']
        
        # variables
        project_path = self.base_projects_dir +'/'+ project_name
        if project_path in sys.path:
            sys.path.remove(project_path)
        
        sys.path.insert(0,project_path)
            
        import reduce_functions
        reload(reduce_functions)
        from reduce_functions import ReduceFunctions
        self.reduce_function = ReduceFunctions(reduce_function_name).get_reduce_function()
        self.job_name = job_name
        self.server_id = server_id
        self.max_number_dumped_items = max_number_dumped_items
        self.disk_based_input = disk_based_input
        self.disk_based_output = disk_based_output
        self.compress = compress
        
        
        # shared references
        self.sorted_file = None
        self.output_file_name = None
        self.number_dumped_items = 0
        self.output_file = self._get_output_file()
        self.input_file_names = self._get_input_file_names()
        self.auxiliary_data = self._get_auxiliary_data(auxiliary_data_name)
         
    def sort(self):
        self.sorted_file = DiskSort()
        
        total = 0.0
        for file_name in self.input_file_names:
            file = open(file_name)
            for line in file:
                
                try:
                    item = ujson.loads(line)
                except ValueError:
                    f = open('/mnt/ssd0/logs/LINE_ERROR_'+str(self.server_id)+'.data','a')
                    f.write(line)
                    f.close()
                    raise
                    
                self.sorted_file.append(item)
            
                total = total + 1
                if fmod(total,500000) == 1:
                    print total
            file.close()
                    
    def reduce(self):
        while True:
            try:
                group = self.sorted_file.next_group(self.disk_based_input)
            except StopIteration:
                self.output_file.close()
                break
                
            # bit of a hack to permit the output to be a disk-based iterator, i.e. not in memory.
            if self.disk_based_output:
                disk_based_items = DiskList()
                auxiliary_data = (disk_based_items,self.auxiliary_data)
                disk_based_items = self.reduce_function(group,auxiliary_data)
                items = disk_based_items
            else:
                items = self.reduce_function(group,self.auxiliary_data)
                
            for item in items:
                s = ujson.dumps(item)
                self.output_file.write(s+'\n')
                self._check_file()
              
    def _check_file(self):
        self.number_dumped_items = self.number_dumped_items + 1
        if fmod(self.number_dumped_items,self.max_number_dumped_items) == 0:
            self.output_file.close()
            self.output_file = self._get_output_file()
                    
    def _get_input_file_names(self):
        input_dir = self.base_dir +'/'+ self.job_name +'/map/'+ str(self.server_id)
        input_file_names = glob(input_dir +'/*')
        return input_file_names
        
    def _get_output_file(self):
        self._get_output_file_name()
        if self.compress:
            output_file = gzip.open(self.output_file_name,'w')
        else:
            output_file = open(self.output_file_name,'w')
            
        return output_file
        
    def _get_output_file_name(self):
        dir_1 = self.base_dir +'/'+ self.job_name
        try:
            os.mkdir(dir_1)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise
            
        dir_2 = self.base_dir +'/'+ self.job_name +'/reduce'
        try:
            os.mkdir(dir_2)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise
        
        if self.compress:
            file_extension = '.gz'
        else:
            file_extension = '.data'
        
        self.output_file_name = self.base_dir +'/'+ self.job_name +'/reduce/'+ str(uuid.uuid4()) + file_extension
        
    def _get_auxiliary_data(self,auxiliary_data_name):
        if auxiliary_data_name:
            fn = self.auxiliary_dir +'/'+ auxiliary_data_name + '.data'
            f = open(fn)
            s = f.read()
            f.close()
            auxiliary_data = ujson.loads(s)
            return auxiliary_data