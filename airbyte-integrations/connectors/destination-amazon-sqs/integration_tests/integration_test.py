#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
from typing import Any, Mapping

import pytest
from airbyte_cdk.models import AirbyteStream, ConfiguredAirbyteCatalog, ConfiguredAirbyteStream, DestinationSyncMode, \
    Status, SyncMode
from airbyte_protocol.models import AirbyteRecordMessage

from destination_amazon_sqs import DestinationAmazonSqs


@pytest.fixture(name="config")
def config_fixture() -> Mapping[str, Any]:
    with open("secrets/config.json", "r") as f:
        return json.loads(f.read())


@pytest.fixture(name="configured_catalog")
def configured_catalog_fixture() -> ConfiguredAirbyteCatalog:
    stream_schema = {"type": "object", "properties": {"string_col": {"type": "str"}, "int_col": {"type": "integer"}}}

    append_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(name="append_stream", json_schema=stream_schema, supported_sync_modes=[SyncMode.incremental]),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append,
    )

    overwrite_stream = ConfiguredAirbyteStream(
        stream=AirbyteStream(name="overwrite_stream", json_schema=stream_schema, supported_sync_modes=[SyncMode.incremental]),
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.overwrite,
    )

    return ConfiguredAirbyteCatalog(streams=[append_stream, overwrite_stream])


def test_check_valid_config(config: Mapping):
    outcome = DestinationAmazonSqs().check(logging.getLogger("airbyte"), config)
    assert outcome.status == Status.SUCCEEDED


def test_check_invalid_config():
    outcome = DestinationAmazonSqs().check(logging.getLogger("airbyte"), {"secret_key": "not_a_real_secret"})
    assert outcome.status == Status.FAILED


# Define the fixture for AirbyteRecordMessage
@pytest.fixture(name="message_record")
def message_record_fixture() -> AirbyteRecordMessage:
    return AirbyteRecordMessage(
        namespace="some_namespace",
        stream="some_stream_name",
        data={},
        emitted_at=1
    )


@pytest.mark.parametrize(
    "queue_url, expected_queue_name",
    [
        pytest.param(
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-${NAMESPACE}",
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-some_namespace",
            id="valid_namespace_placeholder",
        ),
        pytest.param(
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-${STREAM}",
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-some_stream_name",
            id="valid_stream_placeholder",
        ),
        pytest.param(
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-${NAMESPACE}-${STREAM}",
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-some_namespace-some_stream_name",
            id="valid_both_placeholders",
        ),
        pytest.param(
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue",
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue",
            id="no_placeholders",
        ),
    ],
)
def test_sqs_queue_parser(message_record: AirbyteRecordMessage, queue_url: str, expected_queue_name: str):
    outcome = DestinationAmazonSqs.get_queue_name(queue_url=queue_url, record_message=message_record)
    assert outcome == expected_queue_name


@pytest.mark.parametrize(
    "queue_url, expected_queue_name",
    [
        pytest.param(
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-${NAMESPACE}",
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-default",
            id="missing_namespace_in_message",
        ),
        pytest.param(
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-${STREAM}",
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-default",
            id="missing_stream_in_message",
        ),
        pytest.param(
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-${NAMESPACE}-${STREAM}",
            "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-default-default",
            id="missing_both_in_message",
        ),
    ],
)
def test_sqs_queue_parser_with_none_fields(queue_url: str, expected_queue_name: str):
    message_record_with_none_fields = AirbyteRecordMessage(
        namespace="",
        stream="",
        data={},
        emitted_at=1
    )
    outcome = DestinationAmazonSqs.get_queue_name(queue_url=queue_url, record_message=message_record_with_none_fields)
    assert outcome == expected_queue_name


def test_sqs_queue_parser_without_message():
    queue_url = "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-${NAMESPACE}-${STREAM}"
    expected_queue_name = "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue-default-default"
    outcome = DestinationAmazonSqs.get_queue_name(queue_url=queue_url, record_message=None)
    assert outcome == expected_queue_name


def test_sqs_queue_parser_raises_exception_for_none_url():
    with pytest.raises(Exception, match="Queue URL is required"):
        DestinationAmazonSqs.get_queue_name(queue_url=None)
