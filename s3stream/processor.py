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
import os, glob
import io
import re
import gzip
import logging
from aws import S3
from kafka import KafkaProducer


def gzip_decompress(gzip_in, file_out):
    """ Decompress a GZIP file to an given file """
    fd_in = gzip.open(gzip_in, 'rb')
    fd_out = open(file_out, 'wb')
    fd_out.write( fd_in.read() )
    fd_in.close()
    fd_out.close()

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
        self.str_replace = str_replace
        self.str_repl_src = str_repl_src
        self.str_repl_dst = str_repl_dst

        self.s3 = S3()

        self.kf_servers = kafka_bs_servers
        self.kf_topic = kafka_topic
        self.kf_producer = None
        self.kf_sender = None

    def kafka_connect(self):
        self.kf_producer = KafkaProducer(bootstrap_servers=self.kf_servers)

    def kafka_get_connection(self):
        return self.kf_producer

    def kafka_publish_message(self, message):
        #print("#> Sending message bellow to kafka: ")
        #print message
        self.kf_sender = self.kf_producer.send(self.kf_topic, value=message.encode('utf-8'));

    def kafka_commit(self):
        self.kf_producer.flush()
        self.kf_producer.close()

    def parse_stream(self, fd):
        """ Parse """
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

    def parser_file(self, raw_file):
        """
        First step of stream
        # Start parser stream
        #> Read local_raw
        #> filter message
        #> publish on kafka
        """
        try:
            with io.open(raw_file, 'r') as fd:
                return self.parse_stream(fd)

        except IOError as e:
            print("#> I/O error({0}): {1}".format(e.errno, e.strerror))
            return False

        except Exception as e:
            print("#> Unexpected error: ".format(e))
            raise

    def process_status(self):
        print("OK")

    def process_body(self, msgs_body):
        """
         Receives a message in JSON format, extract S3 information
         download and publish, filter and publish on kafka.
         msg_body list of messages with Body of SQS.
        """

        for msg in msgs_body:
            msg_j = json.loads(msg)

            logging.info('Processor.process_body() - Reading message: {}'.format(msg_j))
            #print(json.dumps(msg_j, indent=4))

            s3_body = msg_j['Records'][0]['s3']
            bucket = s3_body['bucket']['name']
            obj = s3_body['object']['key']

            local_file = "{}/{}/{}".format(self.workdir, bucket, obj)

            # Creating local dir
            local_dir = os.path.dirname(local_file)
            if not os.path.exists(local_dir):
                print("Creating directory: {}".format(local_dir))
                os.makedirs(local_dir)

            if not os.path.exists(local_dir):
                print("#> Error - Failed to create directory")

            self.s3.download(bucket_name=bucket,
                             object_key=obj,
                             dest=local_file)

            print("#> Extracting ZIP file")
            #utils.gzip_decompress()
            local_raw = local_file.split('.gz')[0]
            gzip_decompress(local_file, local_raw)
            if not os.path.exists(local_raw):
                print("#> UNcompressed file not found: {}".format(local_raw))
            else:
                print("#> File Uncompressed here: {}".format(local_raw))


            if not self.kafka_get_connection():
                self.kafka_connect()

            self.parser_file(local_raw)
            print("#> OK commit flush messages")
            self.kafka_commit()

            for filename in glob.glob("{}*".format(local_raw)):
                os.remove(filename)

        return True
