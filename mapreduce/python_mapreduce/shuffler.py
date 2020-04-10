# shuffler.py takes output from the mapper and writes the data to a file that will be transferred/shuffled to a specific machine, for a specific process/server.
# the two main functions are append() and shuffle().

import uuid, os, random, fcntl, errno, sys, json, random, gzip
from math import fmod
from copy import copy
from time import sleep, ctime
from glob import glob

from configs_parser import get_configs
from profiler import Profiler


class Shuffle:

    def __init__(self, job_name, server_names, server_id, compress, max_number_dumped_items_shuffler=500000,
                 max_cache_size=5000):
        # configs
        configs = get_configs(self.__module__)
        self.base_dir = configs['base_dir']
        self.code_dir = configs['code_dir']
        self.working_dir = configs['working_dir']
        self.max_hash_cache_size = configs['max_hash_cache_size']

        # variables
        self.job_name = job_name
        self.server_names = server_names
        self.my_server_id = server_id
        self.compress = compress
        self.max_number_dumped_items_shuffler = max_number_dumped_items_shuffler
        self.max_cache_size = max_cache_size
        self.number_of_servers = len(self.server_names)

        # shared references
        self.wait_token = None
        self.my_token = None
        self.lock = None
        self.pending_file_names = set([])
        self.pending_file_name2server_name = {}
        self.processed_file_names = {}
        self.processed_server_names = set([])
        self.hash_cache = {}
        self.file_names = {}
        self.consolidated_file_names = {}
        self.have_seen_file_names = set([])
        self.number_dumped_items = {}
        self.server_id2server_name = {}
        self.server_name2server_id = {}
        self.location2server_ids = {}
        self.location2server_names = {}
        self.server_id2location = {}
        self.cache = {}
        for server_name in self.server_names:
            server_id = self.server_names[server_name]['SERVER_ID']
            self.cache[server_id] = []
            file_name, file = self._get_new_file_reference(server_id)
            file.close()

            print 'INSTANTIATING SERVER:', server_name, file_name

            self.file_names[server_id] = [file_name]
            self.number_dumped_items[server_id] = 0
            self.server_id2server_name[server_id] = server_name
            self.server_name2server_id[server_name] = server_id
            location = server_names[server_name]['LOCATION']
            self.server_id2location[server_id] = location

            if location not in self.location2server_ids:
                self.location2server_ids[location] = set([])

            if location not in self.location2server_names:
                self.location2server_names[location] = set([])

            self.location2server_ids[location].add(server_id)
            self.location2server_names[location].add(server_name)

        self.my_server_name = self.server_id2server_name[self.my_server_id]
        self.my_location = self.server_names[self.my_server_name]['LOCATION']

        self.profiler = Profiler(server_id, job_name)

    def append(self, key, item):
        h = self._hash(key)
        server_id = int(fmod(h, self.number_of_servers))
        s = json.dumps(item)
        self.cache[server_id].append(s + '\n')
        self._check_file(server_id)

    def _check_file(self, server_id):
        self.number_dumped_items[server_id] = self.number_dumped_items[server_id] + 1
        number_dumped_items = self.number_dumped_items[server_id]

        if fmod(number_dumped_items, self.max_number_dumped_items_shuffler) == 0:
            file_name, file = self._get_new_file_reference(server_id)
            self.file_names[server_id].append(file_name)
            self.cache[server_id].sort()  # sorting a sorted list is much faster than sorting an unsorted list
            for s in self.cache[server_id]:
                file.write(s)

            file.flush()
            os.fsync(file)
            file.close()
            self.cache[server_id] = []
            print 'DUMPED FILE:', file_name

            # also check kill file
            print 'CHECKING KILL FILE...'
            self._check_kill_file()

        elif fmod(number_dumped_items, self.max_cache_size) == 0:
            file_name = self.file_names[server_id][-1]

            if self.compress:
                file = gzip.open(file_name, 'a')

            else:
                file = open(file_name, 'a')

            self.cache[server_id].sort()  # sorting a sorted list is much faster than sorting an unsorted list
            for s in self.cache[server_id]:
                file.write(s)

            file.flush()
            os.fsync(file)
            file.close()
            self.cache[server_id] = []
            print 'DUMPED CACHE:', server_id, number_dumped_items

    def _get_new_file_reference(self, server_id):
        file_name = self.working_dir + '/SHUFFLE_' + str(server_id) + '_' + str(uuid.uuid4()) + '.data'

        if self.compress:
            file_name = file_name + '.gz'
            file = gzip.open(file_name, 'w', compresslevel=3)

        else:
            file = open(file_name, 'w')

        print 'CREATED NEW SHUFFLE FILE:', file_name
        return file_name, file

    def shuffle(self):
        print 'FLUSHING...'
        self._flush()

        print 'CHECKING KILL FILE...'
        self._check_kill_file()

        print 'WAITING FOR ALL SERVERS TO FINISH...'
        self._wait()

        print 'CONSOLIDATING FILES...'
        self._consolidate_files()

        server_ids = self.consolidated_file_names.keys()
        random.shuffle(server_ids)
        for server_id in server_ids:

            print 'ACQUIRING TOKEN FOR SERVER ID:', server_id, ctime()
            self._acquire_token()

            file_names = self.consolidated_file_names[server_id]
            for file_name in file_names:
                self._transfer_file(file_name, server_id)

            print 'RETURNING TOKEN FOR SERVER ID:', server_id, ctime()
            self._return_token()

            print 'CHECKING KILL FILE...'
            self._check_kill_file()

    def _flush(self):
        for server_id in self.cache:
            file_name, file = self._get_new_file_reference(server_id)
            self.file_names[server_id].append(file_name)

            for s in self.cache[server_id]:
                file.write(s)

            file.flush()
            os.fsync(file)
            file.close()
            self.cache[server_id] = []

    def _wait(self):
        while True:

            if self._lock_wait_file():
                fn = self.working_dir + '/WAIT.data'

                if not os.path.exists(fn):
                    f = open(fn, 'w')
                    f.close()

                f = open(fn, 'a')
                f.write(self.my_server_name + '\n')
                f.close()

                self._unlock_wait_file()

                break

            self._check_kill_file()
            sleep(0.5)

        while True:

            fn = self.working_dir + '/WAIT.data'
            f = open(fn)
            s = f.read()
            server_names = set(s.strip().split('\n'))

            if server_names == self.location2server_names[self.my_location]:
                break

            print 'WAITING FOR SERVERS TO FINISH...'
            self._check_kill_file()
            sleep(1.0)

    def _check_kill_file(self):
        fn = self.working_dir + '/KILL.data'

        if os.path.exists(fn):
            self._report_kill()
            raise Exception('SHUFFLER DOWN! EXITING...')

        # check current memory usage
        self.profiler.check_memory_usage()

    def _report_kill(self):
        fn = self.working_dir + '/i_own_kill_report.data'
        flock = open(fn, 'w')
        while True:
            my_lock = self._lock_file(flock)
            if my_lock:
                fn = self.working_dir + '/KILL_REPORT.data'

                if not os.path.exists(fn):
                    f = open(fn, 'w')
                    f.close()

                f = open(fn)
                s = f.read()
                f.close()
                server_ids = s.strip().split('\n')

                if str(self.my_server_id) not in server_ids:
                    s = s + str(self.my_server_id) + '\n'
                    f = open(fn, 'w')
                    f.write(s)
                    f.close()

                self._unlock_file(flock)

                return

            else:
                sleep(0.1)

    def _consolidate_files(self):
        number_servers_per_machine = len(self.location2server_ids[self.my_location])
        min_server_id = min(self.location2server_ids[self.my_location])
        my_index = self.my_server_id - min_server_id

        fns = glob(self.working_dir + '/SHUFFLE*')
        for fn in fns:

            server_id = int(fn.split('/')[-1].split('_')[1])
            if fmod(server_id, number_servers_per_machine) == my_index and os.path.getsize(fn) > 0:
                f_consolidated = self._get_consolidated_file(server_id)

                if self.compress:
                    f = gzip.open(fn, 'rb')

                else:
                    f = open(fn, 'r')

                for l in f:
                    f_consolidated.write(l)

                f.close()
                f_consolidated.close()
                os.unlink(fn)
                print 'DELETED:', fn

            elif fmod(server_id, number_servers_per_machine) == my_index and os.path.getsize(
                    fn) == 0:  # delete size 0 files
                os.unlink(fn)
                print 'DELETED:', fn

    def _get_consolidated_file(self, server_id):

        # if there are no consolidation files for this server_id, make one and close it.
        if server_id not in self.consolidated_file_names:
            fn_consolidated = self.working_dir + '/CONSOLIDATED_' + str(uuid.uuid4()) + '.data'

            if self.compress:
                fn_consolidated = fn_consolidated + '.gz'
                f_consolidated = gzip.open(fn_consolidated, 'w', compresslevel=3)

            else:
                f_consolidated = open(fn_consolidated, 'w')

            f_consolidated.close()
            self.consolidated_file_names[server_id] = [fn_consolidated]

        # guaranteed consolidation file exists
        fn_consolidated = self.consolidated_file_names[server_id][-1]  # most recent file
        consolidated_file_size = os.path.getsize(fn_consolidated)

        # if the file is quite large, create a new one and close it.
        if consolidated_file_size > 10000000:
            fn_consolidated = self.working_dir + '/CONSOLIDATED_' + str(uuid.uuid4()) + '.data'

            if self.compress:
                fn_consolidated = fn_consolidated + '.gz'
                f_consolidated = gzip.open(fn_consolidated, 'w', compresslevel=3)

            else:
                f_consolidated = open(fn_consolidated, 'w')

            f_consolidated.close()
            self.consolidated_file_names[server_id].append(fn_consolidated)

        # finally, get the consolidation file reference.
        if self.compress:
            f_consolidated = gzip.open(fn_consolidated, 'a', compresslevel=3)

        else:
            f_consolidated = open(fn_consolidated, 'a')

        return f_consolidated

    def _transfer_file(self, local_file, server_id):
        cmd = 'scp -i /home/streamline/.ssh/cluster_id_rsa LOCAL_FILE streamline@LOCATION:REMOTE_DIR'
        remote_dir = self.base_dir + '/' + self.job_name + '/map/' + str(server_id)
        location = self.server_id2location[server_id]
        custom_cmd = cmd.replace('LOCAL_FILE', local_file).replace('LOCATION', location).replace('REMOTE_DIR',
                                                                                                 remote_dir)
        print 'ATTEMPTING:', custom_cmd, os.path.getsize(local_file)
        attempts = 0
        return_code = True
        while return_code:
            return_code = os.system(custom_cmd)
            attempts = attempts + 1

            if attempts > 5:
                print cmd, return_code
                Exception('SHUFFLE FAILED...')

            else:
                sleep(1.0)

        os.unlink(local_file)
        print 'DELETED:', local_file

    def _acquire_token(self):
        while True:

            my_lock = self._lock_tokens_file()
            if my_lock:
                self._check_kill_file()

                fn = self.working_dir + '/TRANSFER_TOKENS.data'
                f = open(fn)
                s = f.read()
                f.close()
                tokens = json.loads(s)
                for token in tokens:

                    is_available = tokens[token]
                    if is_available:
                        self.my_token = token
                        is_available = False
                        tokens[self.my_token] = is_available
                        s = json.dumps(tokens)
                        f = open(fn, 'w')
                        f.write(s)
                        f.close()
                        self._unlock_tokens_file()

                        return

            else:
                self._check_kill_file()
                sleep(0.1)

    def _return_token(self):
        while True:

            my_lock = self._lock_tokens_file()
            if my_lock:
                fn = self.working_dir + '/TRANSFER_TOKENS.data'
                f = open(fn)
                s = f.read()
                f.close()
                tokens = json.loads(s)
                is_available = True
                tokens[self.my_token] = is_available
                s = json.dumps(tokens)
                f = open(fn, 'w')
                f.write(s)
                f.close()
                self._unlock_tokens_file()

                return

            else:
                self._check_kill_file()
                sleep(0.1)

    def _lock_tokens_file(self):
        lock = self.working_dir + '/i_own_tokens_file.flag'
        self.lock = open(lock, 'w')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False

    def _unlock_tokens_file(self):
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()

    def _lock_wait_file(self):
        token_name = self.working_dir + '/i_own_wait_file.flag'
        self.wait_token = open(token_name, 'w')
        try:
            fcntl.flock(self.wait_token, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False

    def _unlock_wait_file(self):
        fcntl.flock(self.wait_token, fcntl.LOCK_UN)
        self.wait_token.close()

    def _lock_file(self, file):
        try:
            # attempt lock
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False

    def _unlock_file(self, file):
        fcntl.flock(file, fcntl.LOCK_UN)

    def _hash(self, key):
        hash_cache_size = len(self.hash_cache)
        if key in self.hash_cache:
            h = self.hash_cache[key]
        elif hash_cache_size < self.max_hash_cache_size:
            random.seed(key)
            h = int(random.uniform(0, 1) * 10000000)
            self.hash_cache[key] = h
        else:
            random.seed(key)
            h = int(random.uniform(0, 1) * 10000000)
        return h
