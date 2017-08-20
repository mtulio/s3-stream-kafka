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
            print("#> Error - argument is missing")

        print("# Downloading from S3: {}".format(dest))
        self.s3.Object(bucket_name, object_key).download_file(dest)


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
                 queue_max=1):

        SQS.__init__(self)
        self.queue_url = queue_url

        if self.queue_url == None and queue_name == None:
            print("#> No URL or Name found. Exiting")
            sys.exit(1)

        if self.queue_url == None:
            self.queue_url = self.sqs.get_queue_url(QueueName=queue_name)

        self.queue = self.sqs.Queue(self.queue_url)
        self.queue_max = queue_max
        self.messages = None

    def receive(self):
        try:
            # self.messages = self.queue.receive_messages(AttributeNames=['All'],
            #                                             MessageAttributeNames=['All'])
            m = self.sqs_conn.receive_message(
                                              QueueUrl=self.queue_url,
                                              AttributeNames=[
                                                  'SentTimestamp'
                                              ],
                                              MaxNumberOfMessages=1,
                                              MessageAttributeNames=[
                                                  'All'
                                              ],
                                              VisibilityTimeout=0,
                                              WaitTimeSeconds=0)
            self.messages = m
            return True
        except:
            raise

    def show_message_body(self, msg_body):
        """ Show message body, maybe can keep out of Object """
        try:
            b = json.loads(msg_body)
            print(json.dumps(b, indent=4))
        except:
            raise

    def show_messages(self):
        """ Show Message(s) from current Queue """
        if not 'Messages' in self.messages:
            print("#> ERR - There is no messages or malformed messages on queue. ")
            print(json.dumps(self.messages, indent=4))
            sys.exit(1)

        try:
            for m in self.messages['Messages']:
                print('#> Message1: ')
                self.show_message_body(m['Body'])
        except:
            raise


    def get_messages_body(self):
        """ Return only body of messages in the Queue """
        msgs_body = []
        if not 'Messages' in self.messages:
            print("#> ERR - There is no messages or malformed messages on queue. ")
            print(json.dumps(self.messages, indent=4))
            sys.exit(1)

        try:
            for m in self.messages['Messages']:
                msgs_body.append(m['Body'])
        except:
            raise

        return msgs_body


    def delete(self):
        """ Delete Message(s) to SQS """
        print("TODO")
