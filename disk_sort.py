# disk_sort.py is a disk-based merge sort for large datasets.

import heapq,uuid,os,cPickle,sys
from math import fmod

import ujson

from disk_list import DiskList
from configs_parser import get_configs


class DiskSort():
    
    def __init__(self):
        # configs
        configs = get_configs(self.__module__)
        self.working_dir = configs['working_dir']
        self.max_number_of_bytes_in_memory = configs['max_number_of_bytes_in_memory']
        self.number_of_servers_per_location = configs['number_of_servers_per_location']
        self.max_number_items = None
        
        # shared references
        self.number_items = 0
        self.current_list = []
        self.file_names = []
        self.item_sizes = []
        self.current_item = None
        self.write_mode = True
        self.finished = False
        self.file = None
        
    def append(self,item):
        heapq.heappush(self.current_list,item)
        self._get_max_number_items(item)
        self.number_items = self.number_items + 1
        if fmod(self.number_items,self.max_number_items) == 0:
            self._reset()
        
    def next_group(self,disk_based=False):
        group = []
        
        if disk_based:
            group = DiskList()
        
        if self.write_mode:
            self._reset()
            
            # initialize files
            files = []
            for fn in self.file_names:
                df = DeserializedFile(fn)
                files.append(df)
                
            # initialize merge
            self.file = heapq.merge(*files)
            
            # initialize first item
            self.current_item = self.file.next()
            
            self.write_mode = False
            
        elif self.finished:
            raise StopIteration
        
        item = self.current_item
        while item[0] == self.current_item[0]:
            group.append(item)
            try:
                item = self.file.next()
            except StopIteration:
                self.finished = True
                break
  
        self.current_item = item
        return group
            
    def _reset(self):
        self._write()
        self.current_list = []
        
    def _get_file_reference(self):
        file_name = self.working_dir +'/SORT_'+str(uuid.uuid4())+'.data'
        file = open(file_name,'w')
        return file_name,file
        
    def _write(self):
        file_name,file = self._get_file_reference()
        while True:
            try:
                item = heapq.heappop(self.current_list)
                s = ujson.dumps(item)
                file.write(s+'\n')
            except IndexError:
                break
                
        self.file_names.append(file_name)
        file.close()
    
    def _get_max_number_items(self,item):
        flag = False
        
        l = len(self.item_sizes)
        if l < 250:
            flag = True
            
        elif fmod(self.number_items,250000) == 0:
            del(self.item_sizes[0])
            flag = True
            
        if flag:
            s = cPickle.dumps(item)
            size = sys.getsizeof(s)
            self.item_sizes.append(size)
            self.item_sizes.sort(reverse=True)
            largest_items = self.item_sizes[0:250]
            s = sum(largest_items)
            l  = float(len(largest_items))
            mean_size = s/l
            self.max_number_items = int(self.max_number_of_bytes_in_memory/(mean_size*self.number_of_servers_per_location))


# create an iterable object, from a file, that returns deserialized items.      
class DeserializedFile():
    
    def __init__(self,file_name):
        self.file_name = file_name
        self.file = open(file_name)
        
    def __iter__(self):
        return self
        
    def next(self):  
        try:
            s = self.file.next()
            item = ujson.loads(s)
            return item
        except StopIteration:
            self._delete()
            raise StopIteration
            
    def _delete(self):
        self.file.close()
        os.remove(self.file_name)
        