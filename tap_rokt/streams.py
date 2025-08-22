"""Stream type classes for tap-rokt."""

from __future__ import annotations

import typing as t
from importlib import resources
from datetime import datetime, timedelta

from singer_sdk import Stream
from singer_sdk import typing as th

from tap_rokt.client import RoktClient

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class CampaignsBreakdownStream(Stream):
    """
    Account Campaigns Breakdown: metrics by campaign for an account.
    """

    name = "campaigns_breakdown"
    path = "/query/accounts/{account_id}/campaigns/"
    primary_keys: t.ClassVar[list[str]] = ["campaign_id", "date"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("campaign_id", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("date", th.StringType),
        th.Property("impressions", th.NumberType),
        th.Property("referrals", th.NumberType),
        th.Property("gross_cost", th.NumberType),
        th.Property("net_cost", th.NumberType),
        th.Property("click_thru_acquisitions", th.NumberType),
        th.Property("click_thru_acquisitions_by_conversion_time", th.NumberType),
        th.Property("view_thru_acquisitions_by_conversion_time", th.NumberType),
        th.Property("acquisitions_by_conversion_time", th.NumberType),
        th.Property("click_thru_conversions", th.NumberType),
        th.Property("click_thru_conversions_by_conversion_time", th.NumberType),
        th.Property("view_thru_conversions_by_conversion_time", th.NumberType),
        th.Property("view_thru_acquisitions", th.NumberType),
        th.Property("view_thru_conversions", th.NumberType),
        th.Property("acquisitions", th.NumberType),
        th.Property("conversions", th.NumberType),
        th.Property("conversions_by_conversion_time", th.NumberType),
        th.Property("unique_creatives", th.NumberType),
        th.Property("unique_campaigns", th.NumberType),
        th.Property("unique_audiences", th.NumberType),
        th.Property("unique_campaign_countries", th.NumberType),
        th.Property("click_thru_conversion_value", th.NumberType),
        th.Property("acquisitions_value", th.NumberType),
        th.Property("conversion_value", th.NumberType),
        th.Property("view_thru_acquisitions_value", th.NumberType),
        th.Property("view_thru_conversion_value", th.NumberType),
        th.Property("click_thru_acquisition_value", th.NumberType),
    ).to_dict()

    def __init__(self, tap):
        super().__init__(tap)
        cfg = tap.config
        self.client = RoktClient(
            client_id=cfg["client_id"],
            client_secret=cfg["client_secret"],
            token_url="https://api.rokt.com/auth/oauth2/token",
            api_base="https://api.rokt.com",
        )
        self.account_id = cfg["account_id"]

        now = datetime.utcnow()
        default_end = now.strftime("%Y-%m-%d") + "T23:59:59.000"
        default_start = (now - timedelta(days=cfg.get("days_back", 14))).strftime("%Y-%m-%d")
        default_start = default_start + "T00:00:00.000"

        self.start_date = cfg.get("start_date") or default_start
        self.end_date = cfg.get("end_date") or default_end

        self.currency = cfg.get("currency", "USD")
        self.time_zone_variation = cfg.get("time_zone_variation", "UTC")

    def parse_response(self, response: dict) -> list[dict]:
        return response if isinstance(response, list) else []

    def get_records(self, context: dict | None) -> t.Iterable[dict]:
        """Fetch campaign breakdown data one day at a time, inclusive."""
        path = self.path.format(account_id=self.account_id)
        start_date_obj = self.start_date
        end_date_obj = self.end_date

        body = {
                "interval": "day",
                "startDate": start_date_obj,
                "endDate": end_date_obj,
                "dimensionFilters": {},
                "metrics": [
                    "impressions",
                    "referrals",
                    "gross_cost",
                    "net_cost",
                    "click_thru_acquisitions",
                    "click_thru_acquisitions_by_conversion_time",
                    "view_thru_acquisitions_by_conversion_time",
                    "acquisitions_by_conversion_time",
                    "click_thru_conversions",
                    "click_thru_conversions_by_conversion_time",
                    "view_thru_conversions_by_conversion_time",
                    "view_thru_acquisitions",
                    "view_thru_conversions",
                    "acquisitions",
                    "conversions",
                    "conversions_by_conversion_time",
                    "unique_creatives",
                    "unique_campaigns",
                    "unique_audiences",
                    "unique_campaign_countries",
                    "click_thru_conversion_value",
                    "acquisitions_value",
                    "conversion_value",
                    "view_thru_acquisitions_value",
                    "view_thru_conversion_value",
                    "click_thru_acquisition_value"
                ],
                "dimensions": ["campaign_id", "campaign_name"],
                "orderBys": [
                    {
                    "column": "referrals",
                    "direction": "desc"
                    }
                ]
        }
        self.logger.info(f"Fetching data for {start_date_obj} to {end_date_obj}")
        response = self.client.post(path, body=body)
        records = response["data"]
        for record in records:
            if record == []:
                continue
            if record["campaign_id"] is None:
                continue
            record["date"] = record["datetime"].split("T")[0]
            yield record
