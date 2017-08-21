# s3-stream

S3stream streaming S3 object text data, like Cloud Front logs, to an given
output. Supported output are:

* Kafka

But we will support these outputs:

* Syslog (in dev)
* Elasticsearch
* Raw logs

## Overview

This project will get an S3 file, from an SQS notification (we are assuming
that you have already create it), filter something (no required) and publish
on Kafka, or other output providers.

The simple architecture are:

```
  |_S3_| -> |_SQS_|<-----------.
               |               |
.--------------:---------------|--------------.
:|_INIT_|      |               |-|_ONE_SHOOT_|:--> sys.exit(0)
:   |          |         |_SQS_DELETE_|       :
:   '-->|_SQS_POOLER_|<--------|              :
:              |               |              :
:        |_PARSE_MSG_|         |              :
:              |               |              :
:           |_S3_GET_|         |              :
:              |               |              :
:        |_EXTRACTOR_|         |              :
:              |               |              :
:           |_FILTER_|         |              :
:              |               |              :
:          |_PUBLISH_|         |              :
:              |             |_OK_| |_FAIL_|--:--> sys.exit(1)
:              |               |_______|      :
'--------------:---------------|--------------'
               |               |
               |           |_RESULT_|
     __________|               |
    |                          |
  |_KAFKA_|------------------->|
  |_ELASTICSEARCH_|----------->|
  |_GRAYLOG_GELF_|------------>|
  |_SYSLOG_|------------------>|

```

* Limitations

We do not create the SNS topic and we are assuming that you have notifications
when queue is too long, or consumir (s3stream) stopped, etc.

## Use case

* Near real time Cloud Front log processor

## Goals

* Filter messages before stream output
* Support SQS pooler to run with interval
* Support stream to kafka
* Support stream to syslog
* Support stream to gelf (graylog server)
* Support stream to Elasticsearch
* Support dry-run
* Support to run as a deamon
* Support to get last executions information
* Support to run the consumer, when it's already running
* Improve consumer metrics
