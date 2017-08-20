# s3-stream

Streaming S3 object text data, like Cloud Front logs, to an given broker, log service, ES, etc

## Overview

This project will get an S3 file, from an SQS notification, filter it and publish on Kafka.

## Initial Architecture

```
  |_S3_| -> |_SQS_|<-----------.
               |               |
.--------------:---------------|--------------.
:|_INIT_|      |               |--|_DRY_RUN_|-:--> sys.exit(0)
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

```
  |_S3_| -> |_SQS_|
               | ------ [COMMIT]--------------------------------.
               |                                                [OK]
               '--[RECEIVE]--,                                    '-,-[FAIL] --> Exit(1)
  |_S3-to-kafka_|-> |_CONSUMER_|--> |_S3_GET_| --> |_FILTER_| --> |_K_PUBLISH_|
                                                                       |
                                                                       |
                                                                    |_KAFKA_|
```

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
