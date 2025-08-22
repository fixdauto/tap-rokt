"""Rokt tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_rokt import streams


class TapRokt(Tap):
    """Rokt tap class."""

    name = "tap-rokt"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Client ID",
            description="The client ID to authenticate against the API service",
        ),
        th.Property(
            "client_secret",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Client Secret",
            description="The client secret to authenticate against the API service",
        ),
        th.Property(
            "account_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Account ID",
            description="The account ID to sync",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.RoktStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CampaignsBreakdownStream(self),
        ]


if __name__ == "__main__":
    TapRokt.cli()


