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
# limitations under the License.

import json
import sys, os
import io
import re
import logging
from aws import S3
from kafka import KafkaProducer
from utils import u_print, u_print_d, \
                  dnow, clean_files, gzip_dec, \
                  Stats, str2bool


class Processor(object):

    def __init__(self, logging=None,
                 workdir='./', filters=None,
                 kafka_bs_servers='localhost:9092',
                 kafka_topic=None,
                 str_replace=False,
                 str_repl_src=None,
                 str_repl_dst=None):

        self.logging = logging
        if not self.logging:
            self.logging = logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        self.workdir = workdir
        self.filter = filters
        self.str_replace = str2bool(str_replace)
        self.str_repl_src = str_repl_src
        self.str_repl_dst = str_repl_dst

        self.s3 = S3()

        self.kf_servers = kafka_bs_servers
        self.kf_topic = kafka_topic
        self.kf_producer = None
        self.kf_sender = None

        self.stats = Stats({
            'proc_start': None,
            'proc_finish': None,
            'file': None,
            'file_size': None,
            's3_object': None,
            'lines': None,
            'lines_match': None,
        })

    def stats_reset(self):
        """Reset all key metrics."""
        self.stats.reset()

    def stats_update(self, key, value):
        """Set key metric."""
        self.stats.update(key, value)

    def stats_get_str(self):
        """Return one line string with metrics."""
        return self.stats.get_all_str()

    def stats_show(self, prefix=None):
        """Show metrics."""
        self.stats.show(prefix=prefix)

    def kafka_connect(self):
        """Output Kafka - Init Producer."""
        self.kf_producer = KafkaProducer(bootstrap_servers=self.kf_servers)

    def kafka_get_connection(self):
        """Output Kafka - Get Producer."""
        return self.kf_producer

    def kafka_publish_message(self, message):
        """Output Kafka - Publish message."""
        self.kf_sender = self.kf_producer.send(self.kf_topic, value=message.encode('utf-8'));

    def kafka_commit(self):
        """Output Kafka - Flush messages."""
        self.kf_producer.flush()

    def kafka_close(self):
        """Output Kafka - Close producer."""
        self.kf_producer.close()

    def parse_stream(self, fd):
        """Parser stream from file descriptor."""
        count_all = count_match = 0
        for line in fd:
            count_all += 1
            if not re.search(self.filter, line):
                continue

            count_match += 1
            if self.str_replace:
                self.kafka_publish_message(re.sub(self.str_repl_src, self.str_repl_dst, line))
            else:
                self.kafka_publish_message(line)

        #u_print(" Processor.parse_stream() - Lines: processed=[{}] matched=[{}]".format(count_all, count_match))
        self.stats_update('lines', count_all)
        self.stats_update('lines_match', count_match)

    def parser_file(self, raw_file):
        """Open file to stream."""
        try:
            with io.open(raw_file, 'r') as fd:
                #u_print(" Processor.parser_file() - Streaming file {}".format(raw_file))
                return self.parse_stream(fd)

        except IOError as e:
            u_print(" ERROR I/O ({0}): {1}".format(e.errno, e.strerror))
            return False

        except Exception as e:
            u_print(" Unexpected error: ".format(e))
            raise

    def process_body(self, msg):
        """Process each message."""

        msg_j = json.loads(msg)

        u_print_d(' Processor.process_body() - Reading message: {}'.format(msg_j))

        s3_body = msg_j['Records'][0]['s3']
        bucket = s3_body['bucket']['name']
        obj = s3_body['object']['key']

        local_file = "{}/{}/{}".format(self.workdir, bucket, obj)

        # Creating local dir
        local_dir = os.path.dirname(local_file)
        if not os.path.exists(local_dir):
            u_print_d(" Processor.process_body() - Creating directory: {}".format(local_dir))
            os.makedirs(local_dir)

        if not os.path.exists(local_dir):
            u_print(" ERROR - Failed to create directory")

        try:
            self.s3.download(bucket_name=bucket,
                             object_key=obj,
                             dest=local_file)

            u_print_d(' Processor.process_body() - Extracting ZIP file')

            #utils.gzip_dec()
            local_raw = local_file.split('.gz')[0]
            gzip_dec(local_file, local_raw)
            if not os.path.exists(local_raw):
                u_print_d(" Uncompressed file not found: {}".format(local_raw))
                clean_files("{}*".format(local_raw))
                return

            u_print_d(" Processor.process_body() - File Uncompressed here: {}".format(local_raw))
            self.stats_update('file', local_raw)
            self.stats_update('file_size', '{}'.format(os.path.getsize(local_raw)))
            self.stats_update('s3_object', '{}/{}'.format(bucket, obj))

            if not self.kafka_get_connection():
                u_print_d(" Processor.process_body() - Connecting to Kafka")
                self.kafka_connect()

            u_print_d(" Processor.process_body() - Parsing file")
            self.parser_file(local_raw)

            clean_files("{}*".format(local_raw))

        except KeyboardInterrupt as e:
            u_print(" Got interrupt, removing current spool file")
            clean_files("{}*".format(local_raw))
            self.stats_show()
            sys.exit(1)

        u_print_d(" Processor.process_body() - Commit messages to Kafka")
        self.kafka_commit()

    def processor(self, msgs):
        """
         Receives a message in JSON format, extract S3 information
         download and publish, filter and publish on kafka.
         msg_body list of messages with Body of SQS.
        """
        for msg in msgs:
            self.stats_reset()
            self.stats_update('proc_start', "{}".format(dnow()))
            u_print(" Processing message: {}".format(msg))

            self.process_body(msg)

            self.stats_update('proc_finish', "{}".format(dnow()))
            self.stats_show(prefix=" Processor finished: ")

        return True
