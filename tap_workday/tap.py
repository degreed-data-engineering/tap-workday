"""workday tap class."""

from pathlib import Path
from typing import List
import logging
import click
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_workday.streams import (
    HumanResources,
)

PLUGIN_NAME = "tap-workday"

STREAM_TYPES = [ 
    HumanResources,
]

class TapWorkday(Tap):
    """workday tap class."""

    name = "tap-workday"
    config_jsonschema = th.PropertiesList(
        th.Property("username", th.StringType, required=False, description="username"),
        th.Property("password", th.StringType, required=False, description="password"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams =  [stream_class(tap=self) for stream_class in STREAM_TYPES]

        return streams


# CLI Execution:
cli = TapWorkday.cli