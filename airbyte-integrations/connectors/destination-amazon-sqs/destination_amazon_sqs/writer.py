#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import json
import logging
from typing import List
from uuid import uuid4

import boto3
from airbyte_cdk.models import AirbyteRecordMessage
from airbyte_protocol.models import ConfiguredAirbyteCatalog, ConfiguredAirbyteStream
from boto3 import Session
from botocore.exceptions import ClientError

logger = logging.getLogger("airbyte")


class SqsWriter:
    def __init__(
            self,
            queue_url: str,
            region: str = None,
            access_key: str = None,
            secret_key: str = None,
            message_group_id: str = None,
            message_delay=None,
            message_body_key=None,
            configured_catalog: ConfiguredAirbyteCatalog = None,
    ):
        self.configured_catalog = configured_catalog
        # Required properties
        self.queue_url = queue_url
        self.region = region

        # Sensitive Properties
        self.access_key = access_key
        self.secret_key = secret_key

        self.message_group_id = message_group_id

        self.message_delay = message_delay

        self.message_body_key = message_body_key

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Amazon SQS Destination Config Check - queue_url: {queue_url}")
            logger.debug(f"Amazon SQS Destination Config Check - region: {region}")
            logger.debug(f"Amazon SQS Destination Config Check - access_key (ends with): {access_key[-1]}")
            logger.debug(f"Amazon SQS Destination Config Check - secret_key (ends with): {secret_key[-1]}")
            logger.debug(f"Amazon SQS Destination Config Check - message_group_id: {message_group_id}")
            logger.debug(f"Amazon SQS Destination Config Check - message_delay: {message_delay}")
            logger.debug(f"Amazon SQS Destination Config Check - message_body_key: {message_body_key}")

        aws_session: Session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        self.aws_sqs_client = aws_session.resource("sqs")

    def check(self) -> bool:
        try:
            logger.info("Amazon SQS Destination Config Check - Starting connection test ")
            queue = self.get_queue_for_airbyte_record_message()

            if hasattr(queue, "attributes"):

                if self.queue_is_fifo(queue.url):
                    fifo = queue.attributes.get("FifoQueue", False)
                    if not fifo:
                        logger.error("Amazon SQS Destination Config Check - Queue is not FIFO")
                        raise Exception("FIFO Queue URL set but Queue is not FIFO")

                    message_group_id = self.message_group_id
                    if message_group_id is None:
                        logger.error("Amazon SQS Destination Config Check - Message Group ID is not set")
                        raise Exception("Message Group ID is not set, but is required for FIFO Queues.")

                    # TODO: Support referencing an ID inside the Record to use as de-dupe ID
                    # message_dedupe_key = config.get("message_dedupe_key")
                    # content_dedupe = queue.attributes.get('ContentBasedDeduplication')
                    # if content_dedupe == "false":
                    #     if message_dedupe_id is None:
                    #         raise Exception("You must provide a Message Deduplication ID when ContentBasedDeduplication is not used.")

                logger.info("Amazon SQS Destination Config Check - Connection test successful")
                return True
            else:
                raise Exception("Amazon SQS Destination Config Check - Could not connect to queue")
        except ClientError as e:
            raise Exception(f"Amazon SQS Destination Config Check - Error in AWS Client: {str(e)}")

    @staticmethod
    def queue_is_fifo(url: str) -> bool:
        return url.endswith(".fifo")

    def write(self, record: AirbyteRecordMessage) -> None:
        # TODO: Make access/secret key optional, support public access & profiles
        # TODO: Support adding/setting attributes in the UI
        # TODO: Support extract a specific path as message attributes
        sqs_message = self.build_sqs_message(record)

        sqs_message = self.add_attributes_to_message(sqs_message, record)

        queue = self.get_queue_for_airbyte_record_message(record)

        if self.queue_is_fifo(queue.url):
            use_content_dedupe = False if queue.attributes.get("ContentBasedDeduplication") == "false" else "true"
            self.set_message_fifo_properties(sqs_message, use_content_dedupe)

        self.send_single_message(queue, sqs_message)

    def purge(self, stream: str) -> None:
        pass

    def build_sqs_message(self, record: AirbyteRecordMessage):
        data = None
        if self.message_body_key:
            data = record.data.get(self.message_body_key)
            if data is None:
                raise Exception("Message had no attribute of the configured Message Body Key: " + self.message_body_key)
        else:
            data = json.dumps(record.data)

        message = {"MessageBody": data}

        if self.message_delay:
            message["DelaySeconds"] = self.message_delay

        logger.info("Building SQS Message - " + str(message))
        return message

    def get_queue_url(self, namespace: str, stream: str) -> str:

        # URL of the SQS Queue, you can use meta tags eg: ${NAMESPACE} and ${STREAM} to replace with the actual values from the stream
        # https://sqs.eu-west-1.amazonaws.com/1234567890/my-example-queue
        # https://sqs.eu-west-1.amazonaws.com/1234567890/my-example-queue${NAMESPACE}${STREAM}

        if self.queue_url is None:
            raise Exception("Queue URL is required")

        queue_url_parsed = self.queue_url

        # Replace the ${NAMESPACE} placeholder with the actual value
        if "${NAMESPACE}" in queue_url_parsed:
            namespace = f"-{namespace}" if namespace else ""
            queue_url_parsed = queue_url_parsed.replace("${NAMESPACE}", namespace)

        # Replace the ${STREAM} placeholder with the actual value
        if "${STREAM}" in queue_url_parsed:
            stream = f"-{stream}" if stream else ""
            queue_url_parsed = queue_url_parsed.replace("${STREAM}", stream)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Queue URL: {queue_url_parsed} parsed for namespace: {namespace} and stream: {stream}")

        return queue_url_parsed

    def get_queue(self, namespace, stream):
        queue_url = self.get_queue_url(namespace=namespace, stream=stream)
        return self.aws_sqs_client.Queue(url=queue_url)

    def get_queue_for_airbyte_record_message(self, record_message: AirbyteRecordMessage = None):
        namespace = record_message.namespace if record_message else None
        stream = record_message.stream if record_message else None

        return self.get_queue(namespace, stream)

    # MessageGroupID and MessageDeduplicationID are required properties for FIFO queues
    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
    def set_message_fifo_properties(self, message, use_content_dedupe=False):
        # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
        if not self.message_group_id:
            raise Exception("Failed to build message - Message Group ID is required for FIFO queues")
        else:
            message["MessageGroupId"] = self.message_group_id
        # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html
        if not use_content_dedupe:
            message["MessageDeduplicationId"] = str(uuid4())
        # TODO: Support getting MessageDeduplicationId from a key in the record
        # if message_dedupe_id:
        #     message['MessageDeduplicationId'] = message_dedupe_id
        return message

    @staticmethod
    def send_single_message(queue, message) -> dict:
        return queue.send_message(**message)

    @staticmethod
    def add_attributes_to_message(message, record: AirbyteRecordMessage):
        attributes = {
            "airbyte_emitted_at": {"StringValue": str(record.emitted_at), "DataType": "String"},
            "stream_name": {"StringValue": str(record.stream), "DataType": "String"},
            "stream_namespace": {"StringValue": str(record.namespace), "DataType": "String"},
        }
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Adding attributes to message - " + str(attributes))
        message["MessageAttributes"] = attributes
        return message

    def check_available_streams(self, configured_streams: List[ConfiguredAirbyteStream]):
        for configured_stream in configured_streams:
            stream = configured_stream.stream

            queue_url = self.get_queue_url(stream.namespace, stream.name)

            queue = self.aws_sqs_client.Queue(url=queue_url)
            try:
                if hasattr(queue, "attributes"):
                    logger.info(f"Amazon SQS Destination - Found queue: {queue_url}")
            except ClientError as e:
                logger.warning(f"Amazon SQS Destination - Could not find queue: {queue_url}")
