#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import logging
from typing import Any, Iterable, Mapping, List

from airbyte_cdk.destinations import Destination
from airbyte_cdk.exception_handler import init_uncaught_exception_handler
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    Status,
    Type
)

from .writer import SqsWriter

logger = logging.getLogger("airbyte")


class DestinationAmazonSqs(Destination):

    # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
    def write(
            self,
            config: Mapping[str, Any],
            configured_catalog: ConfiguredAirbyteCatalog,
            input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        writer = SqsWriter(configured_catalog=configured_catalog, **config)
        logger.info("Amazon SQS Destination - Catalog: " + str(configured_catalog))

        if configured_catalog.streams:
            writer.check_available_streams(configured_catalog.streams)
            for configured_stream in configured_catalog.streams:
                if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                    writer.purge(configured_stream.stream.name)

        for message in input_messages:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Amazon SQS Destination - Processing Message: {message.model_dump_json()}")

            if message.type == Type.STATE:
                logger.info(f"Amazon SQS Destination - Writing state")
                yield message

            elif message.type == Type.RECORD:
                logger.debug(f"Amazon SQS Destination - Writing record")
                record = message.record
                writer.write(record)

            else:
                logger.warning(f"Amazon SQS Destination - Ignoring message {message.model_dump_json()}")
                continue

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            writer = SqsWriter(**config)
            writer.check()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=repr(e))

    def run(self, args: List[str]) -> None:
        init_uncaught_exception_handler(logger)
        parsed_args = self.parse_args(args)
        output_messages = self.run_cmd(parsed_args)
        for message in output_messages:
            print(message.model_dump_json(exclude_unset=True, by_alias=True))
