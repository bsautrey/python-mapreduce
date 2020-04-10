# process.py is a dumb processor. It receives job settings, then uses them to configure and run a mapreduce process.

import gc, traceback, json, fcntl, os, errno
from time import sleep
from copy import copy

from mapper import Map
from reducer import Reduce
from configs_parser import get_configs


class Process:

    def __init__(self, server_name, server_id, location, server_names):
        # who am i and where am i?
        self.server_name = server_name
        self.server_id = server_id
        self.location = location
        self.server_names = copy(server_names)

        # configs
        configs = get_configs(self.__module__)
        self.working_dir = configs['working_dir']

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
        self.max_cache_size = None
        self.number_simultaneous_tranfers = None
        self.compress = None

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

            if self._set_job():

                try:

                    self._state('START')
                    self.mapper = Map(self.map_function_name, self.project_name, self.input_dirs, self.server_id,
                                      self.job_name, self.server_names, self.compress, self.hold_state, self.downsample,
                                      self.auxiliary_data_name_mapper, self.max_number_dumped_items_shuffler,
                                      self.max_cache_size)
                    self.mapper.map()
                    self._state('END')

                    self._state('START')
                    self.reducer = Reduce(self.reduce_function_name, self.project_name, self.job_name, self.server_id,
                                          self.max_number_dumped_items_reducer, self.disk_based_input,
                                          self.disk_based_output, self.auxiliary_data_name_reducer, self.compress)
                    self.reducer.sort()
                    self._state('END')

                    self._state('START')
                    self.reducer.reduce()
                    self._state('END')

                    self._report_kill()
                    # this is here to make sure that all processes end with an exception no matter what state they ended with.
                    raise Exception('CLEAN RUN. EXITING...')

                except:
                    exception = traceback.format_exc()
                    state = ['EXCEPTION', exception]
                    self._state(state)
                    self._report_kill()
                    raise

            else:
                sleep(0.5)

    def _set_job(self):
        self._get_job()

        if self.job:

            if self.job and self.job != 'DONE':
                # try to free memory before start new job
                gc.collect()

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
                self.max_cache_size = self.job['max_cache_size']
                self.number_simultaneous_tranfers = self.job['number_simultaneous_tranfers']
                self.compress = self.job['compress']

                # reduce
                self.reduce_function_name = self.job['reduce_function_name']
                self.auxiliary_data_name_reducer = self.job['auxiliary_data_name_reducer']
                self.max_number_dumped_items_reducer = self.job['max_number_dumped_items_reducer']
                self.disk_based_input = self.job['disk_based_input']
                self.disk_based_output = self.job['disk_based_output']
                self.compress = self.job['compress']

                return True

    def _get_job(self):
        while True:
            fn = self.working_dir + '/i_own_job_state.data'

            if not os.path.exists(fn):
                return

            flock = open(fn, 'r')
            my_lock = self._lock_file(flock)

            if my_lock:
                fn = self.working_dir + '/JOB_STATE.data'
                f = open(fn, 'r')
                s = f.read()
                f.close()
                self.job = json.loads(s)
                self._unlock_file(flock)
                flock.close()
                print 'GOT JOB...'
                return

            else:
                self._check_kill_file()
                sleep(2.0)

    def _state(self, state):

        if state == 'START':

            while True:
                fn = self.working_dir + '/i_own_process_state.data'
                flock = open(fn, 'w')

                while True:
                    my_lock = self._lock_file(flock)

                    if my_lock:
                        fn = self.working_dir + '/PROCESS_STATE.data'
                        f = open(fn, 'r')
                        s = f.read()
                        process_state = json.loads(s)
                        f.close()
                        self._unlock_file(flock)
                        flock.close()
                        print 'READ PROCESS STATE...'
                        break

                    else:
                        print 'ATTEMPTING TO READ PROCESS STATE...'
                        self._check_kill_file()
                        sleep(2.0)

                s = process_state['PROCESS|' + self.server_name]
                current_state = json.loads(s)

                if current_state == state:
                    return

                else:
                    flock.close()
                    self._check_kill_file()
                    sleep(2.0)

        elif state == 'END':
            fn = self.working_dir + '/i_own_process_state.data'
            flock = open(fn, 'w')

            while True:
                my_lock = self._lock_file(flock)

                if my_lock:
                    fn = self.working_dir + '/PROCESS_STATE.data'
                    f = open(fn, 'r')
                    s = f.read()
                    process_state = json.loads(s)
                    f.close()
                    print 'READ PROCESS STATE...'
                    break

                else:
                    print 'ATTEMPTING TO READ PROCESS STATE...'
                    self._check_kill_file()
                    sleep(2.0)

            s = json.dumps('DONE')
            process_state['PROCESS|' + self.server_name] = s
            f = open(fn, 'w')
            s = json.dumps(process_state)
            f.write(s)
            f.close()
            self._unlock_file(flock)
            flock.close()
            print 'WROTE PROCESS STATE...'

        elif 'EXCEPTION' in state:
            fn = self.working_dir + '/i_own_exception_state.data'
            flock = open(fn, 'w')

            while True:
                my_lock = self._lock_file(flock)

                if my_lock:
                    fn = self.working_dir + '/EXCEPTION_STATE.data'
                    f = open(fn, 'r')
                    s = f.read()
                    exception_state = json.loads(s)
                    f.close()
                    print 'READ EXCEPTION STATE...'
                    break

                else:
                    print 'ATTEMPTING TO READ EXCEPTION STATE...'
                    self._check_kill_file()
                    sleep(2.0)

            exception = state[1]
            s = json.dumps(exception)
            exception_state['EXCEPTION|' + self.server_name] = s
            f = open(fn, 'w')
            s = json.dumps(exception_state)
            f.write(s)
            f.close()
            self._unlock_file(flock)
            flock.close()
            print 'WROTE EXCEPTION STATE...'

    def _check_kill_file(self):
        fn = self.working_dir + '/KILL.data'

        if os.path.exists(fn):
            self._report_kill()
            raise Exception('PROCESS DOWN! EXITING...')

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

                if str(self.server_id) not in server_ids:
                    s = s + str(self.server_id) + '\n'
                    f = open(fn, 'w')
                    f.write(s)
                    f.close()

                self._unlock_file(flock)
                flock.close()

                return

            else:
                sleep(0.5)

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
