# scheduler.py is used to submit, schedule, run jobs on the cluster.

import os,sys,fcntl,subprocess,random,traceback,errno
from time import sleep,time,ctime

import ujson

sys.path.append('/home/ben/code')
sys.path.append('/home/ben/file_transfer')
from manage_cluster import ManageCluster
from file_transfer import FileTransfer
from configs_parser import get_configs


# for running jobs submitted through the Scheduler
class Runner():
    
    def __init__(self):
        # configs
        configs = get_configs(self.__module__)
        self.local_working_dir = configs['local_working_dir']
        
        # shared references
        self.manage_cluster = ManageCluster()
        self.manage_cluster.start_cluster()
        self.file_transfer = FileTransfer()
        self.scheduler = Scheduler()
        self.finished_jobs = None
               
    def run(self):
        self.finished_jobs = set([])
        while True:
            job = self._get_next_job()
            if job:
                print 'STARTING JOB FROM SCHEDULER...'
                start_time = time()
                success,exception = self._run_job(job)
                print 'FINISHED JOB FROM SCHEDULER...'
                end_time = time()
                self._mark_job_as_finished(job,success,exception,start_time,end_time)
            else:
                sleep(2)
            
    def _get_next_job(self):
        all_jobs = self.scheduler._get_jobs()
        for job in all_jobs:
            job_name = job['job_name']
            force_run = job['force_run']
            
            if force_run and job_name in self.finished_jobs:
                self.finished_jobs.remove(job_name)
                
            if job_name not in self.finished_jobs:
                return job
                
        self.finished_jobs = set([])
        return False
        
    def _run_job(self,job):
        self._print_job(job)
        self._write_current_job(job)
        
        job_type = job['job_type']
        if job_type == 'mapreduce':
            success = self._run_mapreduce_job(job)
        elif job_type == 'script':
            success = self._run_script(job)
        elif job_type == 'file_transfer':
            success = self._run_file_transfer(job)
        
        # note: success is a tuple.
        return success
            
    def _print_job(self,job):
        print 'RUNNING JOB:'
        for key in job:
            val = job[key]
            print '\t',key+':',val
        print '---\n'
        
    def _mark_job_as_finished(self,job,success,exception,start_time,end_time):
        job_name = job['job_name']
        self.finished_jobs.add(job_name)
        self.scheduler._mark_job_as_finished(job,success,exception,start_time,end_time)
        
    def _write_current_job(self,job):
        fn = self.local_working_dir + '/CURRENT_JOB.data'
        f = open(fn,'w')
        s = ujson.dumps(job)
        f.write(s)
        f.close()
        
    def _run_mapreduce_job(self,job):
        # set job parameters and run
        try:
            self.manage_cluster.run(job)
            exception = None
            return (True,exception)
            
        except:
            exception = traceback.format_exc()
            current_phase = self.manage_cluster.current_phase
            print exception
            return (False,(exception,current_phase))
        
    def _run_script(self,job):
        
        # get script parameters
        script_location = job['script_location']
        script_arguments = job['script_arguments']
        if script_arguments:
            script = ['python',script_location] + script_arguments
        else:
            script = ['python',script_location]
        
        # run script
        try:
            val = subprocess.check_output(script,shell=False)
            exception = None
            return (True,exception)
            
        except:
            exception = traceback.format_exc()
            print exception
            return (False,exception)
            
    def _run_file_transfer(self,job):
        
        job_type = job['job_type']
        job_name = job['job_name']
        job_priority = job['job_priority']
        
        # upload/download
        input_dir = job['input_dir']
        output_dir = job['output_dir']
        transfer_type = job['transfer_type']
        reload_files = job['reload_files']
        delete_files = job['delete_files']
        compress = job['compress']
        
        # upload auxiliary
        input_file_name = job['input_file_name']
        auxiliary_data_name = job['auxiliary_data_name']
            
        try:
            
            if transfer_type == 'upload':
                self.file_transfer.upload(input_dir,output_dir,reload_files)
                
            elif transfer_type == 'upload_bulk':
                self.file_transfer.upload_bulk(input_dir,output_dir,reload_files,compress)
                
            elif transfer_type == 'download':
                self.file_transfer.download(input_dir,output_dir,delete_files)
            
            elif transfer_type == 'download_bulk':
                self.file_transfer.download_bulk(input_dir,output_dir,delete_files)
                
            elif transfer_type == 'upload_auxiliary':
                self.file_transfer.upload_auxiliary(input_file_name,auxiliary_data_name)
                
            elif transfer_type == 'delete':
                self.file_transfer.delete_files(output_dir)
                
            exception = None
            return (True,exception)
            
        except:
            exception = traceback.format_exc()
            print exception
            return (False,exception)
        
    
# for controling workflow for the Runner    
class Scheduler():
    
    def __init__(self):
        # configs
        configs = get_configs(self.__module__)
        self.local_working_dir = configs['local_working_dir']
        self.job_submitter = os.path.expanduser('~')
        
        # shared references
        self.scheduler_token = None
        
    def submit_job(self,job):
        job_name = job['job_name']
        print 'ATTEMPTING:',job_name
        
        if self._is_correctly_specified(job):
            
            new_jobs = []
            existing_jobs = self._get_jobs()
            job_name = job['job_name']
            for existing_job in existing_jobs:
                existing_job_name = existing_job['job_name']
                if existing_job_name != job_name:
                    new_jobs.append(existing_job)
                else:
                    print 'FOUND EXISTING JOB/OVERWRITING:'
                    for key in existing_job:
                        print '\t',key,existing_job[key]
            new_jobs.append(job)
                
            attempts = 0
            while attempts < 100:
                if self._lock_scheduler():
                    fn = self.local_working_dir +'/JOBS.data'
                    f = open(fn,'w')
                    for job in new_jobs:
                        s = ujson.dumps(job)
                        f.write(s+'\n')
                    f.close()
        
                    self._unlock_scheduler()
                    print 'ACCEPTED:',job_name
                    break
                else:
                    attempts = attempts + 1
                    sleep(random.uniform(0.05,0.10))
        
        else:
            print 'JOB MISSPECIFIED/JOB REJECTED:'
            for key in job:
                print '\t',key,job[key]
            print '---\n'
            
    def _is_correctly_specified(self,job):
        correct = True
        
        job_priority = job['job_priority']
        if job_priority or job_priority == 0:
            
            job_type = job['job_type']
            if job_type == 'mapreduce':
                
                job_template = self.get_mapreduce_job_template()
                template_keys = set(job_template.keys())
                job_keys = set(job.keys())
                if template_keys != job_keys:
                    print 'JOB NOT CREATED WITH TEMPLATE...'
                    correct = False
                    return correct
                
                
                # required
                job_name = job['job_name']
                project_name = job['project_name']
                input_dirs = job['input_dirs']
                max_number_dumped_items_shuffler = job['max_number_dumped_items_shuffler']
                simultaneous_files_in_redis = job['simultaneous_files_in_redis']
                reduce_function_name = job['reduce_function_name']
                max_number_dumped_items_reducer = job['max_number_dumped_items_reducer']
            
                if not job_name:
                    print 'MISSING JOB NAME...'
                    correct = False
                if not project_name:
                    print 'MISSING PROJECT NAME...'
                    correct = False
                if not input_dirs:
                    print 'MISSING INPUT DIRS...'
                    correct = False
                if not max_number_dumped_items_shuffler:
                    print 'MISSING MAX NUMBER DUMPED ITEMS SHUFFLER...'
                    correct = False
                if not simultaneous_files_in_redis:
                    print 'MISSING SIMULTANEOUS FILES IN REDIS...'
                    correct = False
                if not reduce_function_name:
                    print 'MISSING REDUCE FUNCTION NAME...'
                    correct = False
                if not max_number_dumped_items_reducer:
                    print 'MISSING MAX NUMBER DUMPED ITEMS REDUCER...'
                    correct = False
            
            elif job_type == 'script':
                
                job_template = self.get_script_template()
                template_keys = set(job_template.keys())
                job_keys = set(job.keys())
                if template_keys != job_keys:
                    print 'JOB NOT CREATED WITH TEMPLATE...'
                    correct = False
                    return correct
                        
                # required
                job_name = job['job_name']
                script_location = job['script_location']
            
                if not job_name:
                    print 'MISSING JOB NAME...'
                    correct = False
                if not script_location:
                    print 'MISSING SCRIPT LOCATION...'
                    correct = False
            
            elif job_type == 'file_transfer':
                
                job_template = self.get_file_transfer_template()
                template_keys = set(job_template.keys())
                job_keys = set(job.keys())
                if template_keys != job_keys:
                    print 'JOB NOT CREATED WITH TEMPLATE...'
                    correct = False
                    return correct
                
                # required
                job_name = job['job_name']
                if not job_name:
                    print 'MISSING JOB NAME...'
                    correct = False
                    
                transfer_type = job['transfer_type']
                if transfer_type == 'upload' or transfer_type == 'download':
                    
                    # upload/download
                    input_dir = job['input_dir']
                    output_dir = job['output_dir']
                    
                    if not input_dir:
                        print 'MISSING INPUT DIRS...'
                        correct = False
                    if not output_dir:
                        print 'MISSING OUTPUT DIR...'
                        correct = False
                        
                if transfer_type == 'upload_auxiliary':
                    
                    # upload auxiliary
                    input_file_name = job['input_file_name']
                    auxiliary_data_name = job['auxiliary_data_name']
                    
                    if not input_file_name:
                        print 'MISSING INPUT FILE NAME...'
                        correct = False
                    if not auxiliary_data_name:
                        print 'MISSING AUXILIARY DATA NAME...'
                        correct = False
                        
                if transfer_type == 'delete':
                    output_dir = job['output_dir']
                    
                    if not output_dir:
                        print 'MISSING OUTPUT DIR...'
                        correct = False

            else:
                print 'MISSING JOB TYPE...'
                correct = False
                
        else:
            print 'MISSING JOB PRIORITY...'
            correct = False
        
        return correct
        
    def delete_job(self,job_name):
        new_jobs = []
        existing_jobs = self._get_jobs()
        for existing_job in existing_jobs:
            existing_job_name = existing_job['job_name']
            if existing_job_name != job_name:
                new_jobs.append(existing_job)
            else:
                print 'FOUND JOB/DELETING:'
                for key in existing_job:
                    print '\t',key,existing_job[key]
                print '---\n'
        
        attempts = 0
        while attempts < 100:
            if self._lock_scheduler():
                fn = self.local_working_dir +'/JOBS.data'
                f = open(fn,'w')
                for job in new_jobs:
                    s = ujson.dumps(job)
                    f.write(s+'\n')
                f.close()
                self._unlock_scheduler()
                break
            else:
                attempts = attempts + 1
                sleep(random.uniform(0.05,0.10))
            
    def _delete_group(self,job_name):
        target_job = self._get_job(job_name)
        if target_job:
            target_group_name = target_job['group_name']
            existing_jobs = self._get_jobs()
            for existing_job in existing_jobs:
                existing_group_name = existing_job['group_name']
                if existing_group_name == target_group_name:
                    existing_job_name = existing_job['job_name']
                    self.delete_job(existing_job_name)
        else:
            print 'NO GROUP FOUND FOR JOB:',job_name
            
    def _get_job(self,job_name):
        existing_jobs = self._get_jobs()
        for existing_job in existing_jobs:
            existing_job_name = existing_job['job_name']
            if existing_job_name == job_name:
                return existing_job
            
    def _get_jobs(self):
        jobs = []
        fn = self.local_working_dir +'/JOBS.data'
        if not os.path.exists(fn):
            f = open(fn,'w')
            f.close()
            
        temp = []
        attempts = 0
        while attempts < 100:
            if self._lock_scheduler():
                f = open(fn)
                for l in f:
                    job = ujson.loads(l)
                    job_priority = job['job_priority']
                    temp.append((job_priority,job))
                f.close()
                self._unlock_scheduler()
                break
            else:
                attempts = attempts + 1
                sleep(random.uniform(0.05,0.10))
        
        temp.sort(reverse=True)
        for _,job in temp:
            jobs.append(job)
        
        return jobs
        
    def _current_job(self):
        current_job = self._read_current_job()
        for key in current_job:
            val = current_job[key]
            print key+':',val
        
    def _read_current_job(self):
        fn = self.local_working_dir + '/CURRENT_JOB.data'
        f = open(fn)
        s = f.read()
        f.close()
        job = ujson.loads(s)
        return job
            
    def _mark_job_as_finished(self,current_job,success,exception,start_time,end_time):
        current_job_name = current_job['job_name']
        
        if success:
            print 'SUCCESS:',current_job_name
            fn = self.local_working_dir + '/JOBS_SUCCESS.data'
            self._update_runtime(current_job_name,start_time,end_time)
            run_once = current_job['run_once']
            if run_once:
                self.delete_job(current_job_name)
        else:
            fn = self.local_working_dir + '/JOBS_FAILED.data'
            self._delete_group(current_job_name)
        
        current_job['exception'] = exception
        f = open(fn,'a')
        s = ujson.dumps(current_job)
        f.write(s+'\n')
        f.close()
            
    def _update_runtime(self,job_name,start_time,end_time):
        runtime = int(end_time - start_time)
        fn = self.local_working_dir + '/JOBS_RUNTIME.data'
        if not os.path.exists(fn):
            runtimes = {}
            s = ujson.dumps(runtimes)
            f = open(fn,'w')
            f.write(s)
            f.close()
        else:    
            f = open(fn)
            s = f.read()
            f.close()
            runtimes = ujson.loads(s)
            
            if job_name in runtimes:
                runtimes[job_name].append(runtime)
                random.shuffle(runtimes[job_name])
                runtimes[job_name] = runtimes[job_name][0:50]
            else:
                runtimes[job_name] = [runtime]
                
            s = ujson.dumps(runtimes)
            f = open(fn,'w')
            f.write(s)
            f.close()        
        
    def get_mapreduce_job_template(self):
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
        job['max_number_dumped_items_shuffler'] = None # was 500000
        job['simultaneous_files_in_redis'] = None # was 10
        
        # reducer
        job['reduce_function_name'] = None
        job['auxiliary_data_name_reducer'] = None
        job['max_number_dumped_items_reducer'] = None
        job['disk_based_input'] = False
        job['disk_based_output'] = False
        job['compress'] = False
        
        return job
        
    def get_script_template(self):
        job = {}
        
        # job/project
        job['job_submitter'] = self.job_submitter
        job['job_type'] = 'script'
        job['force_run'] = False
        job['start_time'] = None
        job['end_time'] = None
        job['job_name'] = None
        job['group_name'] = None
        job['job_priority'] = None
        job['run_once'] = False
        job['exception'] = None
        
        # script
        job['script_location'] = None
        job['script_arguments'] = None
        
        return job
        
    def get_file_transfer_template(self):
        job = {}
        
        # job/project
        job['job_submitter'] = self.job_submitter
        job['job_type'] = 'file_transfer'
        job['force_run'] = False
        job['start_time'] = None
        job['end_time'] = None
        job['job_name'] = None
        job['group_name'] = None
        job['job_priority'] = None
        job['job_exception'] = None
        job['run_once'] = False
        job['exception'] = None
        
        # upload/download
        job['input_dir'] = None
        job['output_dir'] = None
        job['transfer_type'] = None
        job['reload_files'] = True
        job['delete_files'] = True
        job['compress'] = False
        
        # auxiliary_upload
        job['input_file_name'] = None
        job['auxiliary_data_name'] = None
        
        return job
        
    def _lock_scheduler(self):
        scheduler_token = self.local_working_dir+'/scheduler.lock'
        self.scheduler_token = open(scheduler_token,'a')
        try:
            fcntl.flock(self.scheduler_token, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False
                
    def _unlock_scheduler(self):
        fcntl.flock(self.scheduler_token, fcntl.LOCK_UN)
        self.scheduler_token.close()
        
        
'''    def _mean(self,numbers):
        s = sum(numbers)
        l = len(numbers)
        mean = int(s/l)
        return mean
        
        
    def estimate_next_runtime(self,job_name):
        fn = self.local_working_dir + '/JOBS_RUNTIME.data'
        f = open(fn)
        s = f.read()
        f.close()
        runtimes = ujson.loads(s)
        
        current_job = self._read_current_job()
        current_job_name = current_job['job_name']
        if current_job_name == job_name:
            print 'JOB IS CURRENTLY RUNNING:',job_name
            return
        
        index = 0
        job_name_exists = False
        existing_jobs = self._get_jobs()
        for existing_job in existing_jobs:
            existing_job_name = existing_job['job_name']
            if existing_job_name == current_job_name:
                start_index = index
            elif existing_job_name == job_name:
                end_index = index
                job_name_exists = True
            index = index + 1
            
        if job_name_exists:
            is_current_job = True
            current_time = time()
            estimated_next_runtime = current_time
            if start_index < end_index:
                for index in xrange(start_index,end_index):
                    existing_job = existing_jobs[index]
                    existing_job_name = existing_job['job_name']
                    try:
                        estimated_job_runtime = self._mean(runtimes[existing_job_name])
                        if is_current_job:
                            estimated_next_runtime = estimated_next_runtime + 0.5*estimated_job_runtime
                            is_current_job = False
                        else:
                            estimated_next_runtime = estimated_next_runtime + estimated_job_runtime
                    except KeyError:
                        print 'NOT ENOUGH DATA TO CALCULATE AN ESTIMATE'
                        return
            else:
                l = len(existing_jobs)
                for index in xrange(start_index,l):
                    existing_job = existing_jobs[index]
                    existing_job_name = existing_job['job_name']
                    try:
                        estimated_job_runtime = self._mean(runtimes[existing_job_name])
                        if is_current_job:
                            estimated_next_runtime = estimated_next_runtime + 0.5*estimated_job_runtime
                            is_current_job = False
                        else:
                            estimated_next_runtime = estimated_next_runtime + estimated_job_runtime
                    except KeyError:
                        print 'NOT ENOUGH DATA TO CALCULATE AN ESTIMATE'
                        return
                    
                for index in xrange(0,end_index):
                    existing_job = existing_jobs[index]
                    existing_job_name = existing_job['job_name']
                    try:
                        estimated_job_runtime = self._mean(runtimes[existing_job_name])
                        estimated_next_runtime = estimated_next_runtime + estimated_job_runtime
                    except KeyError:
                        print 'NOT ENOUGH DATA TO CALCULATE AN ESTIMATE'
                        return
                
            seconds_until_next_run = int(estimated_next_runtime - current_time)
            current_time_string = ctime(current_time)
            estimated_next_runtime_string = ctime(estimated_next_runtime)
        
            print 'CURRENT TIME:',current_time_string
            print 'ESTIMATED NEXT RUNTIME:',estimated_next_runtime_string
            print 'SECONDS UNTIL NEXT RUN:',seconds_until_next_run
            print 'JOB YET TO RUN:'
            if start_index < end_index:
                for index in xrange(start_index,end_index):
                    existing_job = existing_jobs[index]
                    existing_job_name = existing_job['job_name']
                    estimated_job_runtime = int(self._mean(runtimes[existing_job_name]))
                    print '\t',existing_job_name,estimated_job_runtime,'seconds...'
                print '---\n'
            else:
                l = len(existing_jobs)
                for index in xrange(start_index,l):
                    existing_job = existing_jobs[index]
                    existing_job_name = existing_job['job_name']
                    estimated_job_runtime = int(self._mean(runtimes[existing_job_name]))
                    print '\t',existing_job_name,estimated_job_runtime,'seconds...'
                for index in xrange(0,end_index):
                    existing_job = existing_jobs[index]
                    existing_job_name = existing_job['job_name']
                    estimated_job_runtime = int(self._mean(runtimes[existing_job_name]))
                    print '\t',existing_job_name,estimated_job_runtime,'seconds...'
                print '---\n'
        else:
            print 'JOB DOES NOT EXIST:',job_name'''
                
        
        
        