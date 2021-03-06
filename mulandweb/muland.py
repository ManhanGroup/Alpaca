#!/usr/bin/env python3
# coding: utf-8

from collections import namedtuple
from pathlib import Path
import os, tempfile, subprocess
import csv

from . import config

class MulandException(Exception):
    pass

class DependencyError(MulandException):
    pass

class MulandRunError(MulandException):
    pass

MulandData = namedtuple('MulandData', ['header', 'records'])

class Muland:
    '''Access Muland Application'''

    muland_binary = config.muland_binary
    work_folder = config.muland_work

    input_files = ['agents', 'agents_zones', 'bids_adjustments',
    'bids_functions', 'demand', 'demand_exogenous_cutoff',
    'real_estates_zones', 'rent_adjustments', 'rent_functions',
    'subsidies', 'supply', 'zones']

    output_files = ['bids', 'bh', 'location', 'location_probability',
    'rents']

    csv_delimiter = ';'

    # Check if muland binary and work folder are in place
    if not os.access(work_folder, os.R_OK & os.W_OK):
        if os.access(work_folder, os.F_OK):
            raise DependencyError('Could not access work folder.')
        os.mkdir(work_folder)

    if not os.access(muland_binary, os.X_OK):
        raise DependencyError('Could not find muland binary.')

    def __init__(self, **kwargs):
        '''Initialize Muland'''
        input_files = self.input_files

        for file in input_files:
            if file not in kwargs:
                raise TypeError("missing required argument: '%s'" % file)
            if not isinstance(kwargs[file], MulandData):
                raise TypeError("argument '%s' must be of type MulandData" % file)

        # Set instance attributes
        self.output_data = {}
        self.input_data = {key: value for key, value in kwargs.items()
                                      if key in input_files}

    def __getattr__(self, name):
        '''Interface to output_data keys'''
        try:
            return self.output_data[name]
        except KeyError:
            pass
        raise AttributeError('No attribute or output data named \'%s\'' % name)

    def _populate_working_dir(self, working_dir):
        '''Prepares data for Muland reading'''
        # Create input and output directories
        os.mkdir(str(Path(working_dir, 'input')))
        os.mkdir(str(Path(working_dir, 'output')))

        # Create files sent by user
        for key, value in self.input_data.items():
            header = value.header
            records = value.records
            filename = str(Path(working_dir, 'input', key + '.csv'))
            with open(filename, 'w') as file:
                writer = csv.writer(file, delimiter=self.csv_delimiter,
                                    quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(header)
                for row in records:
                    writer.writerow(row)

    def _run_muland(self, working_dir, timeout=2):
        '''Run Muland on working dir'''
        with subprocess.Popen([self.muland_binary, working_dir],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE) as process:
            try:
                stdout, stderr = process.communicate(None, timeout=timeout)
                if stdout.find(b'Algorithm ended sucessfully') is -1:
                    print(stdout.decode('ascii'))
                    print(stderr.decode('ascii'))
                    raise MulandRunError('Mu-Land finished without success message')
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                raise MulandRunError('Mu-Land proccess timeout.')
            except:
                process.kill()
                process.wait()
                raise MulandRunError('Unknown error running Mu-Land')

    def _collect_data(self, working_dir):
        '''Collects data generated by Muland'''
        for name in self.output_files:
            output_data = []
            fullname = str(Path(working_dir, 'output', name + '.csv'))
            with open(fullname) as file:
                next(file)
                reader = csv.reader(file, delimiter=self.csv_delimiter,
                                    quoting=csv.QUOTE_NONNUMERIC)
                for row in reader:
                    output_data.append(tuple(row))
            self.output_data[name] = output_data

    def run(self):
        '''Runs Muland'''
        # Create/destroy data directory
        with tempfile.TemporaryDirectory(dir = self.work_folder) as working_dir:
            # Prepare directory
            self._populate_working_dir(working_dir)

            # Run Muland
            self._run_muland(working_dir)

            # Collect data
            self._collect_data(working_dir)
