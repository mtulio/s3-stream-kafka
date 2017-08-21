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
from utils import u_print, u_print_d, dnow, Stats

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

        u_print_d('S3.download() - bucket=[{}] key=[{}] dest=[{}]'.format(bucket_name,
                                                                       object_key,
                                                                       dest))
        return self.s3.Object(bucket_name, object_key).download_file(dest)


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

        self.stats = Stats({
            'msgs_received': None,
            'msgs_total': None,
            'msgs_delayed': None,
            'msgs_not_visible': None,
            'msgs_deleted': [],
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

    def stats_show(self, prefix=''):
        """Show metrics."""
        self.stats.show(prefix=prefix)

    def is_empty(self):
        """Check if the current Queue(local) is empty."""
        if len(self.messages) < 1:
            return True
        else:
            return False

    def attributes_update(self):
        """Update local attributes from SQS."""
        self.sqs_attr = self.sqs_conn.get_queue_attributes(QueueUrl=self.queue_url,
                                                           AttributeNames=[
                                                                'All'
                                                           ])['Attributes'] or None

    def attributes_get(self, attr_name):
        """Get SQS [local] attribute name."""
        if not self.sqs_attr:
            return None

        if attr_name not in self.sqs_attr:
            return None

        return self.sqs_attr[attr_name]

    def receive(self):
        """Retrieve messages from SQS queue."""
        self.stats_reset()
        try:
            self.messages = self.queue.receive_messages(AttributeNames=[
                                                            'SentTimestamp'
                                                        ],
                                                        MaxNumberOfMessages=self.queue_max,
                                                        MessageAttributeNames=[
                                                            'All'
                                                        ],
                                                        VisibilityTimeout=0,
                                                        WaitTimeSeconds=0)


            self.attributes_update()
            self.stats_update('msgs_received', len(self.messages))
            self.stats_update('msgs_total', self.attributes_get('ApproximateNumberOfMessages'))
            self.stats_update('msgs_delayed', self.attributes_get('ApproximateNumberOfMessagesDelayed'))
            self.stats_update('msgs_not_visible', self.attributes_get('ApproximateNumberOfMessagesNotVisible'))
            self.stats_show(prefix=' SQS - Starting Queue: ')
            return True
        except:
            raise

    def show_message(self, msg_body):
        """ Show message body."""
        try:
            b = json.loads(msg_body)
            u_print(json.dumps(b, indent=4))
        except:
            raise

    def show_messages(self):
        """ Show message(s) from current Queue."""
        if not self.messages:
            u_print(" Queue.show_messages() ERR - There is no messages or malformed messages on queue. ")
            u_print(json.dumps(self.messages, indent=4))
            sys.exit(1)

        try:
            for m in self.messages:
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
                u_print_d(" Queue.delete_messages() Deleting the message: {}".format(m.message_id))
                r = self.queue.delete_messages(Entries=[
                        {
                            'Id': m.message_id,
                            'ReceiptHandle': m.receipt_handle
                        },
                    ])
                u_print_d(" Queue.delete_messages() Deletied: {}".format(r))
                self.stats_update('msgs_deleted', m.message_id)
        except:
            raise

        return msgs_body
