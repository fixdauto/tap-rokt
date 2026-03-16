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
            secret=True,
            title="Client ID",
            description="The client ID to authenticate against the API service",
        ),
        th.Property(
            "client_secret",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Client Secret",
            description="The client secret to authenticate against the API service",
        ),
        th.Property(
            "account_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Account ID",
            description="The account ID to sync",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync (YYYY-MM-DD)",
        ),
        th.Property(
            "end_date",
            th.DateTimeType(nullable=True),
            description="The exclusive end date for syncing (YYYY-MM-DD)",
        ),
        th.Property(
            "days_back",
            th.IntegerType(nullable=True),
            description="Number of days back from today to sync (default: 14)",
        ),
        th.Property(
            "currency",
            th.StringType(nullable=True),
            description="Currency code for monetary metrics (default: USD)",
        ),
        th.Property(
            "time_zone_variation",
            th.StringType(nullable=True),
            description="Timezone in Olson format (default: UTC)",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.CampaignsBreakdownStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CampaignsBreakdownStream(self),
        ]


if __name__ == "__main__":
    TapRokt.cli()


