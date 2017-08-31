#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2017 Chaordic Systems All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

import os
import glob
import gzip
import datetime
import logging


def dnow():
    return datetime.datetime.now().time()


def u_print(msg):
    _msg = "[{}]>{}".format(dnow(), msg)
    print(_msg)
    logging.info(_msg)


def u_print_d(msg):
    _msg = "[{}]>{}".format(dnow(), msg)
    #print(_msg)
    logging.info(_msg)


def clean_dir(dir_list):
    """Recursively delete [all] files from an directory."""
    for dirname in dir_list:
        if not os.path.isdir(dirname):
            continue
        if dirname == '/':
            continue

        for root, dirs, files in os.walk(dirname):
            if root == '.':
                continue
            for f in files:
                filename = '{}/{}'.format(root, f)
                if os.path.exists(filename):
                    u_print_d("removing file: {}".format(filename))
                    os.remove(filename)


def clean_file(filename):
    """Removing filename."""

    if os.path.exists(filename):
        u_print_d(" clean_file() removing file: {}".format(filename))
        os.remove(filename)


def clean_files(file_prefix):
    """Removing all files with prefix."""

    for filename in glob.glob(file_prefix):
        if os.path.exists(filename):
            u_print_d(" clean_files() removing file: {}".format(filename))
            os.remove(filename)


def gzip_dec(gzip_in, file_out):
    """ Decompress a GZIP file to an given file """
    fd_in = gzip.open(gzip_in, 'rb')
    fd_out = open(file_out, 'wb')
    fd_out.write( fd_in.read() )
    fd_in.close()
    fd_out.close()


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


class Stats(object):

    def __init__(self, metrics):
        self.metrics = metrics

    def reset(self):
        """Reset all key metrics."""
        for k in self.metrics:
            if isinstance(self.metrics[k], list):
                self.metrics[k] = []
            self.metrics[k] = None

    def update(self, key, value):
        """Set key metric."""
        if isinstance(self.metrics[key], list):
            self.metrics[key].append(value)
        self.metrics[key] = value

    def get_all_str(self):
        """Return one line string with metrics."""
        m = " "
        for k in self.metrics:
            if not self.metrics[k]:
                continue
            m += "{}=[{}] ".format(k, self.metrics[k])

        return m

    def show(self, prefix=''):
        """Show metrics."""
        u_print(" {} {}".format(prefix, self.get_all_str()))
