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
import time
from aws import Queue
from processor import Processor
from utils import u_print, dnow


def main():
    """Main program loop."""
    log_file = os.getenv('LOG_FILE') or None
    log_level = os.getenv('LOG_LEVEL') or logging.INFO
    sqs_loop_interval = os.getenv('SQS_LOOP_INTERVAL') or 10
    sqs_endpoint = os.getenv('SQS_URL')
    sqs_max_retrieve = os.getenv('SQS_MAX_MSGS_RETRIEVE') or 1
    proc_filter = os.getenv('S3_FILTER') or None
    proc_kf_bs = os.getenv('OUTPUT_KAFKA_BOOTSTRAP') or None
    proc_kf_tp = os.getenv('OUTPUT_KAFKA_TOPIC') or None
    proc_str_replace = os.getenv('S3_STR_RPL') or False
    proc_str_replace_src = os.getenv('S3_STR_RPL_SRC') or None
    proc_str_replace_dst = os.getenv('S3_STR_RPL_DST') or None

    if log_file:
        logging.basicConfig(filename=log_file, level=logging.INFO)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    queue = Queue(queue_url=sqs_endpoint,
                  queue_max=int(sqs_max_retrieve),
                  logging=logging)
    proc = Processor(logging=logging,
                     workdir='/tmp/s3-to-kafka-workdir',
                     filters=proc_filter,
                     kafka_bs_servers=proc_kf_bs,
                     kafka_topic=proc_kf_tp,
                     str_replace=proc_str_replace,
                     str_repl_src=proc_str_replace_src,
                     str_repl_dst=proc_str_replace_dst)

    try:
        while True:
            queue.receive()
            if queue.is_empty():
                u_print(" Queue is empty, waiting {} seconds.".format(sqs_loop_interval))
                time.sleep(int(sqs_loop_interval))
                continue

            #queue.show_messages()
            if proc.process_body(queue.get_messages_body()):
                #u_print("#> Proc Status:".format(proc.status(state='latest')))
                queue.delete_messages()
                queue.stat()

    except Exception as e:
        u_print(" Errors found getting messages= {}".format(e))
        proc.kafka_close()
        sys.exit(1)


if __name__ == '__main__':
    main()
