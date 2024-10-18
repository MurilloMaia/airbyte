#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from .destination import DestinationAmazonSqs


def run():
    destination = DestinationAmazonSqs()
    destination.run(sys.argv[1:])
