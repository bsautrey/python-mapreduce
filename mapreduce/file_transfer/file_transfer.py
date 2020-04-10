# file_transfer.py monitors directories and uploads new files to the cluster.

import os, shutil, uuid, random, fcntl, errno, shutil, subprocess
from glob import glob
from math import fmod
from time import sleep, ctime, time

import rpyc, ujson

from configs_parser_file_transfer import get_configs


class FileTransfer:

    def __init__(self):
        # configs
        configs = get_configs(self.__module__)
        self.working_dir = configs['working_dir']
        self.temp_dir = configs['temp_dir']
        self.auxiliary_dir = configs['auxiliary_dir']
        self.cluster_dir = configs['cluster_dir']
        self.locations = configs['locations']
        self.number_of_locations = len(self.locations)
        self.management_port = configs['management_port']
        self.max_attempts = configs['max_attempts']
        self.temp_dir = configs['temp_dir']
        self.USER = configs['USER']
        self.KEY_FILE = configs['KEY_FILE']

        # shared references
        self.have_seen = None
        self.management_connections = None
        self.number_of_locations = len(self.locations)
        self.id2location = {}
        for i in xrange(self.number_of_locations):
            location = self.locations[i]
            self.id2location[i] = location

    def _connect(self):
        self.management_connections = {}
        for location in self.locations:
            connection = rpyc.classic.connect(location, self.management_port)
            self.management_connections[location] = connection
            print 'CONNECTED:', location

    def _disconnect(self):
        for location in self.management_connections:
            connection = self.management_connections[location]
            connection.close()
            print 'CLOSED:', location

    def upload(self, input_dir_name, output_dir_name, reload_files=True):
        # setup
        self._connect()
        self._read_have_seen(input_dir_name, reload_files)
        self._delete_files_upload(output_dir_name, reload_files)

        # prepare transfer
        successes = 0
        input_file_names = self._get_input_file_names(input_dir_name)  # check file uploads

        for input_file_name in input_file_names:
            if input_file_name not in self.have_seen:
                # self._sanity_check_uploads(input_file_name,True)
                print 'INPUT FILE NOT IN HAVE SEEN:', input_file_name
                output_file_name = output_dir_name + '/' + str(uuid.uuid4()) + '.data'
                h = self._hash(input_file_name)
                server_id = fmod(h, self.number_of_locations)
                location = self.id2location[server_id]
                connection = self.management_connections[location]
                if not connection.modules.os.path.exists(output_dir_name):
                    connection.modules.os.mkdir(output_dir_name)

                # attempt transfer
                size_in = 1
                size_out = -1
                attempt = 0
                while size_in != size_out and attempt < self.max_attempts:
                    fin = open(input_file_name)
                    fout = connection.builtin.open(output_file_name, 'w')
                    self._remote_lock_file(fout, connection)
                    shutil.copyfileobj(fin, fout)
                    self._remote_unlock_file(fout, connection)
                    fin.close()
                    fout.close()

                    print '\tUPLOADED:', input_file_name
                    size_in = os.path.getsize(input_file_name)
                    size_out = connection.modules.os.path.getsize(output_file_name)

                    # transfer appears to have completed
                    if size_in == size_out:
                        file_report = {}
                        file_report['output_file_name'] = output_file_name
                        file_report['time'] = time()
                        file_report['ctime'] = ctime()
                        file_report['size'] = size_in
                        file_report['location'] = location
                        self.have_seen[input_file_name] = file_report
                        successes = successes + 1
                        print '\tUPLOADED:', location, input_file_name, size_in, size_out

                    # transfer appears to have been only partial
                    else:
                        connection.modules.os.remove(output_file_name)
                        sleep(1.0)
                        attempt = attempt + 1
                        print '\tUPLOAD FAILED:', location, input_file_name, size_in, size_out
                        raise Exception('UNABLE TO TRANSFER FILE:' + input_file_name + '-->' + output_file_name)

        # teardown
        failures = len(input_file_names) - successes
        print 'SUCCESSES:', successes
        print 'FAILURES:', failures
        self._write_have_seen(input_dir_name)
        self._disconnect()

    def upload_bulk(self, input_dir_name, output_dir_name, reload_files=True, compress=False, batch_size=10):
        # setup
        self._connect()
        self._reset_temp_dir()
        self._read_have_seen(input_dir_name, reload_files)
        self._delete_files_upload(output_dir_name, reload_files)

        # prepare transfer
        successes = 0
        failures = 0
        all_input_file_names = self._get_input_file_names(input_dir_name)  # check file uploads
        all_input_file_names = filter(lambda input_file_name: input_file_name not in self.have_seen,
                                      all_input_file_names)
        while all_input_file_names:

            l = len(all_input_file_names)
            if l > batch_size:
                input_file_names = random.sample(all_input_file_names, batch_size)
            else:
                input_file_names = all_input_file_names

            # compress
            compress_mapping = {}
            if compress:
                new_input_file_names = []
                processes = {}
                for input_file_name in input_file_names:
                    compressed_file_name = self.temp_dir + '/' + input_file_name.split('/')[-1] + '.gz'
                    process_inputs = (input_file_name, compressed_file_name)
                    p = subprocess.Popen('gzip -c ' + input_file_name + '>' + compressed_file_name,
                                         stdin=subprocess.PIPE, shell=True)
                    processes[process_inputs] = p

                for process_input in processes:
                    p = processes[process_input]
                    out, err = p.communicate()
                    return_code = p.returncode
                    if return_code == 0:
                        input_file_name, compressed_file_name = process_input
                        compress_mapping[compressed_file_name] = input_file_name
                        new_input_file_names.append(compressed_file_name)
                    else:
                        input_file_name, output_file_name = process_input
                        raise Exception('COMPRESS FAILED: ' + input_file_name + ' ' + compressed_file_name)

                input_file_names = new_input_file_names

            # transfer
            processes = {}
            for input_file_name in input_file_names:
                h = self._hash(input_file_name)
                server_id = fmod(h, self.number_of_locations)
                location = self.id2location[server_id]

                connection = self.management_connections[location]
                if not connection.modules.os.path.exists(output_dir_name):
                    connection.modules.os.mkdir(output_dir_name)

                process_inputs = (input_file_name, location)
                p = subprocess.Popen(
                    ['scp', '-i' + self.KEY_FILE, input_file_name, self.USER + '@' + location + ':' + output_dir_name])
                processes[process_inputs] = p

            for process_input in processes:
                p = processes[process_input]
                out, err = p.communicate()
                return_code = p.returncode

                input_file_name, location = process_input
                size_in = os.path.getsize(input_file_name)
                output_file_name = output_dir_name + '/' + input_file_name.split('/')[-1]
                if return_code == 0:
                    file_report = {}
                    file_report['output_file_name'] = output_file_name
                    file_report['time'] = time()
                    file_report['ctime'] = ctime()
                    file_report['size'] = size_in
                    file_report['location'] = location

                    if compress:
                        os.remove(input_file_name)
                        input_file_name = compress_mapping[input_file_name]

                    self.have_seen[input_file_name] = file_report
                    successes = successes + 1

                    all_input_file_names.remove(input_file_name)
                    print '\tUPLOADED:', location, input_file_name, size_in

                else:
                    print 'UPLOAD ERROR:', input_file_name
                    failures = failures + 1
                    if failures >= 3:
                        print 'ABORTING UPLOAD DUE TO FAILURES...'
                        raise Exception('UPLOAD FAILED: ' + location + ' ' + input_file_name)

        # teardown
        print 'SUCCESSES:', successes
        print 'FAILURES:', failures
        self._write_have_seen(input_dir_name)
        self._disconnect()

    def upload_auxiliary_bulk(self, input_file_name, auxiliary_data_name):
        # setup
        self._connect()

        # prepare transfer
        output_file_name = self.auxiliary_dir + '/' + auxiliary_data_name + '.data'
        processes = {}
        for location in self.locations:
            process_inputs = (input_file_name, location)
            p = subprocess.Popen(
                ['scp', '-i' + self.KEY_FILE, input_file_name, self.USER + '@' + location + ':' + output_file_name])
            processes[process_inputs] = p

        # check for completion
        still_working = True
        while still_working:
            return_codes = []
            for process_input in processes:
                p = processes[process_input]
                out, err = p.communicate()
                return_code = p.returncode
                return_codes.append(return_code)

            return_code_example = return_codes[0]
            return_code_size = len(set(return_codes))

            if return_code_example == 0 and return_code_size == 1:
                still_working = False

        # teardown
        self._disconnect()

    def upload_auxiliary(self, input_file_name, auxiliary_data_name):
        # setup
        self._connect()

        # prepare transfer
        output_file_name = self.auxiliary_dir + '/' + auxiliary_data_name + '.data'
        for location in self.locations:
            connection = self.management_connections[location]

            # attempt transfer
            size_in = 1
            size_out = -1
            attempt = 0
            while size_in != size_out and attempt < self.max_attempts:
                fin = open(input_file_name)
                fout = connection.builtin.open(output_file_name, 'w')
                shutil.copyfileobj(fin, fout)
                fin.close()
                fout.close()
                size_in = os.path.getsize(input_file_name)
                size_out = connection.modules.os.path.getsize(output_file_name)
                if size_in == size_out:
                    print 'UPLOADED:', location, input_file_name, size_in, size_out
                else:
                    sleep(1.0)
                    attempt = attempt + 1
                    print 'UPLOAD FAILED:', location, input_file_name, size_in, size_out
                    raise Exception('UNABLE TO TRANSFER FILE:' + input_file_name + '-->' + output_file_name)

        # teardown
        self._disconnect()

    def _reset_temp_dir(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        os.mkdir(self.temp_dir)

    def _delete_files_upload(self, output_dir_name, reload_files):
        if reload_files:
            for location in self.management_connections:
                connection = self.management_connections[location]
                if connection.modules.os.path.exists(output_dir_name):
                    if connection.modules.os.path.isdir(output_dir_name):
                        connection.modules.shutil.rmtree(output_dir_name, ignore_errors=True)
                        print 'DELETED:', location, output_dir_name
                    else:
                        connection.modules.os.remove(output_dir_name)
                        print 'DELETED:', location, output_dir_name
                else:
                    print 'NOT FOUND:', location, output_dir_name

    def delete_files(self, output_dir_name):
        self._connect()

        is_wildcard = '*' in output_dir_name
        temp = output_dir_name.split('/')
        base_dir = '/'.join(temp[0:len(temp) - 1])

        # find
        if is_wildcard:
            frags = output_dir_name.replace(base_dir, '').split('*')
            wildcard_dirs = set([])
            for location in self.management_connections:
                connection = self.management_connections[location]
                dirs = connection.modules.glob.glob(base_dir + '/*')
                wildcard_dirs.update(set(dirs))
                for d in dirs:
                    path = d.replace(base_dir, '')
                    for frag in frags:
                        if frag not in d:
                            wildcard_dirs.remove(d)
                            break

            # delete
            for wildcard_dir in wildcard_dirs:
                print 'FOUND WILDCARDS:', wildcard_dir
                self._delete_files_upload(wildcard_dir, True)

        else:
            self._delete_files_upload(output_dir_name, True)

        self._disconnect()

    def _get_input_file_names(self, input_dir_name):
        input_file_names = []
        candidates = glob(input_dir_name + '/*')
        for candidate in candidates:
            if self._check_file_name(candidate) and os.path.getsize(candidate) > 0.0:
                input_file_names.append(candidate)

        return input_file_names

    def _read_have_seen(self, input_dir_name, reload_files):
        if not os.path.exists(self.working_dir):
            os.mkdir(self.working_dir)
            print 'MADE:', self.working_dir

        file_name = self._get_formatted_state_file_name(input_dir_name)
        if reload_files and os.path.exists(file_name):
            os.remove(file_name)
            self.have_seen = {}

        if os.path.exists(file_name):
            f = open(file_name)
            s = f.read()
            f.close()
            self.have_seen = ujson.loads(s)
            print 'LOADED HAVE SEEN:', file_name

        if not self.have_seen:
            self.have_seen = {}
            print 'WARNING: EMPTY HAVE SEEN...'

    def _write_have_seen(self, input_dir_name):
        s = ujson.dumps(self.have_seen)
        file_name = self._get_formatted_state_file_name(input_dir_name)
        f = open(file_name, 'w')
        f.write(s)
        f.close()
        self.have_seen = None
        print 'WROTE HAVE SEEN:', file_name

    def _get_formatted_state_file_name(self, input_dir_name):
        file_name = self.working_dir + '/STATE' + input_dir_name.replace('/', '_') + '.data'
        return file_name

    def download(self, input_dir_name, output_dir_name, delete_files=True):
        # setup
        self._connect()

        if delete_files:
            file_names = glob(output_dir_name + '/*')
            for file_name in file_names:
                print 'DELETING:', file_name
                os.remove(file_name)

        # prepare transfer
        if not os.path.exists(output_dir_name):
            os.mkdir(output_dir_name)
            print 'MADE DIR:', output_dir_name

        for location in self.management_connections:
            print 'LOCATION:', location
            connection = self.management_connections[location]
            print '\tCHECKING:', input_dir_name
            input_file_names = connection.modules.glob.glob(input_dir_name + '/*')
            for input_file_name in input_file_names:

                extension = '.' + input_file_name.split('.')[-1]

                if extension != '.gz':
                    extension = '.data'

                output_file_name = output_dir_name + '/' + str(uuid.uuid4()) + extension

                # attempt transfer
                size_in = 1
                size_out = -1
                attempt = 0
                while size_in != size_out and attempt < self.max_attempts:
                    fin = connection.builtin.open(input_file_name)
                    fout = open(output_file_name, 'w')
                    shutil.copyfileobj(fin, fout)
                    fin.close()
                    fout.close()
                    size_in = connection.modules.os.path.getsize(input_file_name)
                    size_out = os.path.getsize(output_file_name)
                    if size_in != size_out:
                        sleep(1.0)
                        attempt = attempt + 1
                        print '\t\tDOWNLOAD FAILED:', location, input_file_name, output_file_name, size_in, size_out

        # teardown
        self._disconnect()

    def download_bulk(self, input_dir_name, output_dir_name, delete_files=True):
        # setup
        self._connect()

        if delete_files:
            file_names = glob(output_dir_name + '/*')
            for file_name in file_names:
                print 'DELETING:', file_name
                os.remove(file_name)

        if not os.path.exists(output_dir_name):
            os.mkdir(output_dir_name)
            print 'MADE DIR:', output_dir_name

        # prepare transfer
        all_locations = self.management_connections.keys()
        successes = 0
        failures = 0
        while all_locations:

            l = len(all_locations)
            if l > 10:
                locations = random.sample(all_locations, 12)
            else:
                locations = all_locations

            processes = {}
            for location in all_locations:
                p = subprocess.Popen(
                    ['scp', '-i' + self.KEY_FILE, self.USER + '@' + location + ':' + input_dir_name + '/*',
                     output_dir_name])
                processes[location] = p

            for location in processes:
                p = processes[location]
                out, err = p.communicate()
                return_code = p.returncode
                if return_code == 0:
                    successes = successes + 1
                    all_locations.remove(location)
                    print '\tDOWNLOADED:', location

                else:
                    print 'DOWNLOAD ERROR:', location
                    failures = failures + 1
                    if failures >= 3:
                        print 'ABORTING DOWNLOAD DUE TO FAILURES...'
                        raise Exception('DOWNLOAD FAILED: ' + str(out))

        print 'SUCCESSES:', successes
        print 'FAILURES:', failures

        # teardown
        self._disconnect()

    # could not get builtin hash() to generate uniformly.
    def _hash(self, file_name):
        random.seed(file_name)
        h = int(random.uniform(0, 1) * 1000000)
        return h

    def _check_file_name(self, file_name):
        file = open(file_name)
        if self._lock_file(file):
            self._unlock_file(file)
            file.close()
            return True
        else:
            file.close()
            return False

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

    def _remote_lock_file(self, file, connection):
        try:
            # attempt lock
            connection.modules.fcntl.flock(file, connection.modules.fcntl.LOCK_EX | connection.modules.fcntl.LOCK_NB)
        except IOError as e:
            if e.errno != errno.EAGAIN:
                raise
            else:
                return False

    def _remote_unlock_file(self, file, connection):
        connection.modules.fcntl.flock(file, connection.modules.fcntl.LOCK_UN)
