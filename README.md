# s3-stream-kafka
S3 Streaming data to Kafka

## Overview

This project will get an S3 file, from an SQS notification, filter it and publish on Kafka.

## Architecture

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
