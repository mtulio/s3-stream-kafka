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
import sys
import os
import logging
from aws import Queue
from processor import Processor


def main():
    log_file = os.getenv('LOG_FILE') or None
    log_level = os.getenv('LOG_LEVEL') or None
    sqs_endpoint = os.getenv('SQS_URL')
    proc_filter = os.getenv('S3_FILTER') or None
    proc_kf_bs = os.getenv('KAFKA_BOOTSTRAP')
    proc_kf_tp = os.getenv('KAFKA_TOPIC')
    proc_str_replace = os.getenv('S3_STR_RPL') or False
    proc_str_replace_src = os.getenv('S3_STR_RPL_SRC') or None
    proc_str_replace_dst = os.getenv('S3_STR_RPL_DST') or None

    if log_file:
        logging.basicConfig(filename=log_file, level=logging.INFO)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    queue = Queue(queue_url=sqs_endpoint, logging=logging)
    proc = Processor(logging=logging,
                     workdir='/tmp/s3-to-kafka-workdir',
                     filters=proc_filter,
                     kafka_bs_servers=proc_kf_bs,
                     kafka_topic=proc_kf_tp,
                     str_replace=proc_str_replace,
                     str_repl_src=proc_str_replace_src,
                     str_repl_dst=proc_str_replace_dst)

    # Loop will start here
    try:
        queue.receive()
        #queue.show_messages()
        #m = queue.get_messages_body()
        if proc.process_body(queue.get_messages_body()):
            #print("#> Proc Status:".format(proc.status(state='latest')))
            queue.delete_messages()

    except Exception as e:
        print("#> Errors found getting messages= {}".format(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
