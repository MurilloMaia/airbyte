#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import json
import logging
from typing import Any, Iterable, Mapping, Optional
from uuid import uuid4

import boto3
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from airbyte_protocol_dataclasses.models import AirbyteRecordMessage, AirbyteStateStats
from boto3 import Session
from botocore.exceptions import ClientError

logger = logging.getLogger("airbyte")


class DestinationAmazonSqs(Destination):

    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
    def write(
            self,
            config: Mapping[str, Any],
            configured_catalog: ConfiguredAirbyteCatalog,
            input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        logger.info("Amazon SQS Destination - Writing to queue with config: " + str(config))
        # Required properties
        queue_url = config["queue_url"]
        queue_region = config["region"]

        # TODO: Implement optional params for batch
        # Optional Properties
        # max_batch_size = config.get("max_batch_size", 10)
        # send_as_batch = config.get("send_as_batch", False)
        message_delay = config.get("message_delay")
        message_body_key = config.get("message_body_key")

        # FIFO Properties
        message_group_id = config.get("message_group_id")

        # Sensitive Properties
        access_key = config["access_key"]
        secret_key = config["secret_key"]

        aws_session: Session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=queue_region
        )
        aws_sqs_client = aws_session.resource("sqs")

        # TODO: Make access/secret key optional, support public access & profiles
        # TODO: Support adding/setting attributes in the UI
        # TODO: Support extract a specific path as message attributes

        logger.info("Amazon SQS Destination - Writing to queue: " + queue_url)
        record_count: int = 0
        for message in input_messages:
            if message.type == Type.STATE:
                logger.info(f"Amazon SQS Destination - Writing state message: {message}")
                message.state.destinationStats = AirbyteStateStats(recordCount=record_count)
                message.connectionStatus = AirbyteConnectionStatus(status=Status.SUCCEEDED)
                yield message
            if message.type == Type.RECORD:
                record_count += 1
                logger.info(f"Amazon SQS Destination - Writing record message: {message}")
                sqs_message = self.build_sqs_message(message.record, message_body_key)

                if message_delay:
                    sqs_message = self.set_message_delay(sqs_message, message_delay)

                sqs_message = self.add_attributes_to_message(sqs_message, message.record)

                queue = self.get_queue(aws_sqs_client, queue_url, message.record)
                if self.queue_is_fifo(queue_url):
                    use_content_dedupe = False if queue.attributes.get("ContentBasedDeduplication") == "false" else "true"
                    self.set_message_fifo_properties(sqs_message, message_group_id, use_content_dedupe)

                self.send_single_message(queue, sqs_message)
            else:
                logger.info(f"Amazon SQS Destination - Skipping message of type: {message}")

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            # Required properties
            queue_url = config["queue_url"]
            logger.info("Amazon SQS Destination Config Check - queue_url: " + queue_url)
            queue_region = config["region"]
            logger.info("Amazon SQS Destination Config Check - region: " + queue_region)

            # Sensitive Properties
            access_key = config["access_key"]
            logger.info("Amazon SQS Destination Config Check - access_key (ends with): " + access_key[-1])
            secret_key = config["secret_key"]
            logger.info("Amazon SQS Destination Config Check - secret_key (ends with): " + secret_key[-1])

            logger.info("Amazon SQS Destination Config Check - Starting connection test ---")
            aws_session: Session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=queue_region
            )
            aws_sqs_client = aws_session.resource("sqs")
            queue = self.get_queue(aws_sqs_client, queue_url)

            if hasattr(queue, "attributes"):
                logger.info("Amazon SQS Destination Config Check - Connection test successful ---")

                if self.queue_is_fifo(queue_url):
                    fifo = queue.attributes.get("FifoQueue", False)
                    if not fifo:
                        logger.error("Amazon SQS Destination Config Check - Queue is not FIFO")
                        raise Exception("FIFO Queue URL set but Queue is not FIFO")

                    message_group_id = config.get("message_group_id")
                    if message_group_id is None:
                        logger.error("Amazon SQS Destination Config Check - Message Group ID is not set")
                        raise Exception("Message Group ID is not set, but is required for FIFO Queues.")

                    # TODO: Support referencing an ID inside the Record to use as de-dupe ID
                    # message_dedupe_key = config.get("message_dedupe_key")
                    # content_dedupe = queue.attributes.get('ContentBasedDeduplication')
                    # if content_dedupe == "false":
                    #     if message_dedupe_id is None:
                    #         raise Exception("You must provide a Message Deduplication ID when ContentBasedDeduplication is not used.")

                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                return AirbyteConnectionStatus(
                    status=Status.FAILED, message="Amazon SQS Destination Config Check - Could not connect to queue"
                )
        except ClientError as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED, message=f"Amazon SQS Destination Config Check - Error in AWS Client: {str(e)}"
            )
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED, message=f"Amazon SQS Destination Config Check - An exception occurred: {str(e)}"
            )

    @staticmethod
    def get_queue(aws_sqs_client, queue_url: str, record_message: AirbyteRecordMessage = None):
        queue_url = DestinationAmazonSqs.get_queue_name(queue_url, record_message)
        return aws_sqs_client.Queue(url=queue_url)

    @staticmethod
    def get_queue_name(queue_url: str, record_message: Optional[AirbyteRecordMessage] = None) -> str:
        # URL of the SQS Queue, you can use meta tags eg: ${NAMESPACE} and ${STREAM} to replace with the actual values from the stream
        # https://sqs.eu-west-1.amazonaws.com/1234567890/my-example-queue
        # https://sqs.eu-west-1.amazonaws.com/1234567890/my-example-queue-${NAMESPACE}-${STREAM}
        if queue_url is None:
            raise Exception("Queue URL is required")

        # Replace the ${NAMESPACE} placeholder with the actual value
        if "${NAMESPACE}" in queue_url:
            namespace = record_message.namespace if record_message and record_message.namespace else "default"
            queue_url = queue_url.replace("${NAMESPACE}", namespace)

        # Replace the ${STREAM} placeholder with the actual value
        if "${STREAM}" in queue_url:
            stream = record_message.stream if record_message and record_message.stream else "default"
            queue_url = queue_url.replace("${STREAM}", stream)
        logger.info(f"Queue URL: {queue_url} parsed from record: {record_message.__str__()}")
        return queue_url

    @staticmethod
    def queue_is_fifo(url: str) -> bool:
        return url.endswith(".fifo")

    @staticmethod
    def parse_queue_name(url: str) -> str:
        return url.rsplit("/", 1)[-1]

    @staticmethod
    def send_single_message(queue, message) -> dict:
        return queue.send_message(**message)

    @staticmethod
    def build_sqs_message(record: AirbyteRecordMessage, message_body_key=None):
        data = None
        if message_body_key:
            data = record.data.get(message_body_key)
            if data is None:
                raise Exception("Message had no attribute of the configured Message Body Key: " + message_body_key)
        else:
            data = json.dumps(record.data)

        message = {"MessageBody": data}
        logger.info("Building SQS Message - " + str(message))
        return message

    @staticmethod
    def add_attributes_to_message(message, record: AirbyteRecordMessage):
        attributes = {
            "airbyte_emitted_at": {"StringValue": str(record.emitted_at), "DataType": "String"},
            "stream_name": {"StringValue": str(record.stream), "DataType": "String"},
            "stream_namespace": {"StringValue": str(record.namespace), "DataType": "String"},
        }
        logger.info("Adding attributes to message - " + str(attributes))
        message["MessageAttributes"] = attributes
        return message

    @staticmethod
    def set_message_delay(message, message_delay):
        message["DelaySeconds"] = message_delay
        return message

    # MessageGroupID and MessageDeduplicationID are required properties for FIFO queues
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
    @staticmethod
    def set_message_fifo_properties(message, message_group_id, use_content_dedupe=False):
        # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
        if not message_group_id:
            raise Exception("Failed to build message - Message Group ID is required for FIFO queues")
        else:
            message["MessageGroupId"] = message_group_id
        # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html
        if not use_content_dedupe:
            message["MessageDeduplicationId"] = str(uuid4())
        # TODO: Support getting MessageDeduplicationId from a key in the record
        # if message_dedupe_id:
        #     message['MessageDeduplicationId'] = message_dedupe_id
        return message

    # TODO: Support batch send
    # def send_batch_messages(messages, queue):
    #     entry = {
    #         'Id': "1",
    #         'MessageBody': str(record.data),
    #     }
    #     response = queue.send_messages(Entries=messages)
    #     if 'Successful' in response:
    #         for status in response['Successful']:
    #             print("Message sent: " + status['MessageId'])
    #     if 'Failed' in response:
    #         for status in response['Failed']:
    #             print("Message sent: " + status['MessageId'])
