# disk_list.py offers ephemeral list-like access off of disk, one item at a time. It can be used in cases where holding large lists in memory is not feasible.

import uuid,os,cPickle
from configs_parser import get_configs

class DiskList():
    
    def __init__(self):
        self.config_data = get_configs(self.__module__)
        working_dir = self.config_data['working_dir']
        self.working_file = working_dir +'/LIST_'+str(uuid.uuid4())+'.data'
        self.file = open(self.working_file,'w')
        self.mode = 'PUSH'
        self.len = 0
        self.current_item = None
        
    def __iter__(self):
        return self
        
    def __len__(self):
        return self.len
        
    def next(self):
        if self.mode != 'POP':
            self._reset()
            self.mode = 'POP'  
        try:
            item = cPickle.load(self.file)
            return item
        except EOFError:
            self._delete()
            raise StopIteration
            
    def next_group(self):
        group = []
        if self.current_item == None:
            item = self.next()
            self.current_item = item
        elif self.current_item == 'STOP':
            raise StopIteration
            
        item = self.current_item
        while item[0] == self.current_item[0]:
            try:
                group.append(item)
                item = self.next()
            except StopIteration:
                self.current_item = 'STOP'
                return group
                
        self.current_item = item
        return group

    def append(self,item):
        cPickle.dump(item,self.file)
        self.len = self.len + 1
        
    def _reset(self):
        self.file.close()
        self.file = open(self.working_file,'r')
        
    def _delete(self):
        self.file.close()
        os.remove(self.working_file)