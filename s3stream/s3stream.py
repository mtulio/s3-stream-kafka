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
import sys, os, glob
import logging
import time
from aws import Queue
from processor import Processor
from utils import u_print, dnow
from dotenv import load_dotenv, find_dotenv


cfg = {}


def load_config():
    """Lookup .env var file and create cfg structure."""
    load_dotenv(find_dotenv())
    global cfg
    cfg = {
        'spool_dir': os.getenv('S3S_SPOOL_DIR') or '/tmp/s3-stream',
        'log_file': os.getenv('S3S_LOG_FILE') or None,
        'log_level': os.getenv('S3S_LOG_LEVEL') or logging.INFO,
        'sqs_interval': os.getenv('S3S_SQS_INTERVAL') or 10,
        'sqs_url': os.getenv('S3S_SQS_URL'),
        'sqs_max_retrieve': os.getenv('S3S_SQS_RETRIEVAL') or 1,
        'proc_filter': os.getenv('S3S_S3_FILTER') or None,
        'output': os.getenv('S3S_OUTPUT') or None,
        'proc_kf_bs': os.getenv('S3S_KAFKA_BOOTSTRAP') or None,
        'proc_kf_tp': os.getenv('S3S_KAFKA_TOPIC') or None,
        'proc_str_replace': os.getenv('S3S_S3_STR_RPL') or False,
        'proc_str_replace_src': os.getenv('S3S_S3_STR_RPL_SRC') or None,
        'proc_str_replace_dst': os.getenv('S3S_S3_STR_RPL_DST') or None
    }


def main():
    """Main S3 streamer."""
    load_config()
    #print repr(cfg)

    if cfg['log_file']:
        logging.basicConfig(filename=cfg['log_file'], level=logging.INFO)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    queue = Queue(queue_url=cfg['sqs_url'],
                  queue_max=int(cfg['sqs_max_retrieve']),
                  logging=logging)
    proc = Processor(logging=logging,
                     workdir=cfg['spool_dir'],
                     filters=cfg['proc_filter'],
                     kafka_bs_servers=cfg['proc_kf_bs'],
                     kafka_topic=cfg['proc_kf_tp'],
                     str_replace=cfg['proc_str_replace'],
                     str_repl_src=cfg['proc_str_replace_src'],
                     str_repl_dst=cfg['proc_str_replace_dst'])

    u_print(" #>>> Starting s3stream with config: {}".format(cfg))
    try:
        while True:
            queue.receive()
            if queue.is_empty():
                u_print(" Queue is empty, waiting {} seconds.".format(sqs_loop_interval))
                time.sleep(int(sqs_loop_interval))
                continue

            if proc.processor(queue.get_messages_body()):
                queue.delete_messages()
                queue.stats_show()
                queue.stats_show(prefix=' SQS - Finish Queue: ')

    except KeyboardInterrupt as e:
        u_print(" Get interrupt, cleaning files on spool dir")
        #clean_dir([cfg['spool_dir']])
        sys.exit(1)

    except Exception as e:
        u_print(" Errors found getting messages= {}".format(e))
        if proc.kf_sender:
            proc.kafka_close()
        raise
        sys.exit(1)


if __name__ == '__main__':
    main()
