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

import boto3
import json
import logging
from utils import u_print, dnow

class S3(object):

    def __init__(self):
        self.s3 = self.resource()
        self.s3_conn = self.connect()

    def resource(self):
        return boto3.resource('s3')

    def connect(self):
        return boto3.client('s3')

    def download(self, bucket_name=None,
                     object_key=None,
                     dest=None):
        """ Download a file from S3 """

        if bucket_name == None or \
            object_key == None or \
            dest == None:
            u_print(" Error - argument is missing")

        u_print('S3.download() - bucket=[{}] key=[{}] dest=[{}]'.format(bucket_name,
                                                                       object_key,
                                                                       dest))
        s3_resp = self.s3.Object(bucket_name, object_key).download_file(dest)


class SQS(object):

    def __init__(self):
        self.sqs = self.resource()
        self.sqs_conn = self.connect()

    def resource(self):
        return boto3.resource('sqs')

    def connect(self):
        return boto3.client('sqs')


class Queue(SQS):
    """ Queue keeps only one message """

    def __init__(self, queue_url=None, queue_name=None,
                 queue_max=1, logging=None):

        SQS.__init__(self)
        self.queue_url = queue_url

        if self.queue_url == None and queue_name == None:
            u_print(" No URL or Name found. Exiting")
            sys.exit(1)

        if self.queue_url == None:
            self.queue_url = self.sqs.get_queue_url(QueueName=queue_name)

        self.queue = self.sqs.Queue(self.queue_url)
        self.sqs_attr = None
        self.queue_max = queue_max
        self.messages = []

        self.logging = logging
        if not self.logging:
            self.logging = logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        self.metrics_queue_time = (None, None)

    def is_empty(self):
        if len(self.messages) < 1:
            return True
        else:
            return False

    def attributes_update(self):
        self.sqs_attr = self.sqs_conn.get_queue_attributes(QueueUrl=self.queue_url,
                                                           AttributeNames=[
                                                                'All'
                                                           ])['Attributes'] or None

    def attributes_get(self, attr_name):
        if not self.sqs_attr:
            return None

        if attr_name not in self.sqs_attr:
            return None

        return self.sqs_attr[attr_name]

    def stat(self):
        q_start, q_len = self.metrics_queue_time
        u_print(" Queue stat N=[{}] messages: start=[{}] finish=[{}]".format(q_len, q_start, dnow()))

    def receive(self):
        try:
            m = self.queue.receive_messages(AttributeNames=[
                                                'SentTimestamp'
                                            ],
                                            MaxNumberOfMessages=self.queue_max,
                                            MessageAttributeNames=[
                                                'All'
                                            ],
                                            VisibilityTimeout=0,
                                            WaitTimeSeconds=0)

            u_print(' Queue.receive() Get {} messages.'.format(len(m)))
            self.attributes_update()
            u_print(' Queue.receive() Total aproximated=[{}] Delayed=[{}] NotVisible=[{}]'.format(
                                    self.attributes_get('ApproximateNumberOfMessages'),
                                    self.attributes_get('ApproximateNumberOfMessagesDelayed'),
                                    self.attributes_get('ApproximateNumberOfMessagesNotVisible')))
            self.messages = m
            return True
        except:
            raise

    def show_message(self, msg_body):
        """ Show message body, maybe can keep out of Object """
        try:
            b = json.loads(msg_body)
            u_print(json.dumps(b, indent=4))
        except:
            raise

    def show_messages(self):
        """ Show Message(s) from current Queue """
        #print("show_messages()")
        #print self.messages
        if not self.messages:
            u_print(" Queue.show_messages() ERR - There is no messages or malformed messages on queue. ")
            u_print(json.dumps(self.messages, indent=4))
            sys.exit(1)

        try:
            for m in self.messages:
                #u_print('#> Message: ')
                self.show_message(m.body)
        except:
            raise


    def get_messages_body(self):
        """ Return only body of messages in the Queue """
        msgs_body = []
        if not self.messages:
            u_print(" Queue.get_messages_body() ERR - There is no messages or malformed messages on queue. ")
            u_print(json.dumps(self.messages, indent=4))
            sys.exit(1)

        try:
            for m in self.messages:
                msgs_body.append(m.body)
        except:
            raise

        return msgs_body


    def delete_messages(self):
        """ Delete Message(s) to SQS """
        msgs_body = []
        if not self.messages:
            u_print(" Queue.delete_messages() ERR - There is no messages or malformed messages on queue. ")
            u_print(json.dumps(self.messages, indent=4))
            sys.exit(1)

        try:
            for m in self.messages:
                u_print(" Queue.delete_messages() Deleting the message: {}".format(m.message_id))
                r = self.queue.delete_messages(Entries=[
                        {
                            'Id': m.message_id,
                            'ReceiptHandle': m.receipt_handle
                        },
                    ])
                print r
                u_print(" Queue.delete_messages() Deletied: {}".format(r))
        except:
            raise

        return msgs_body
