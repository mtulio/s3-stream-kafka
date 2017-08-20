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
from aws_resource import Queue
from processor import Processor


def main():
    sqs_endpoint = os.getenv('SQS_URL')
    proc_filter = os.getenv('S3_FILTER')
    proc_kf_bs = os.getenv('KAFKA_BOOTSTRAP')
    proc_kf_tp = os.getenv('KAFKA_TOPIC')

    queue = Queue(queue_url=sqs_endpoint)
    proc = Processor(workdir='/tmp/s3-to-kafka-workdir',
                     filters=proc_filter,
                     kafka_bs_servers=proc_kf_bs,
                     kafka_topic=proc_kf_tp)

    # Loop will start here
    try:
        queue.receive()
        m = queue.get_messages_body()
        print(m)
        if proc.process_body(queue.get_messages_body()):
            print("#> Proc Status:".format(proc.status(state='latest')))

    except Exception as e:
        print("#> Errors found getting messages= {}".format(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
