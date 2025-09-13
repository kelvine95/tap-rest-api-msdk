# tap_rest_api_msdk/tap.py
from __future__ import annotations

from typing import List, Dict, Any, Optional

from singer_sdk import Tap
from singer_sdk import typing as th

from .streams import (
    TwitterAdvancedSearchStream,
    TwitterMentionsStream,
    TwitterLatestStream,
    TwitterUsersStream,
)

PLUGIN_NAME = "tap-rest-api-msdk"


class TapRestApiMsdk(Tap):
    """TwitterAPI.io – purpose-built dynamic tap using Singer SDK."""

    name = PLUGIN_NAME

    config_jsonschema = th.PropertiesList(
        th.Property("api_url", th.StringType, required=True),
        th.Property("auth_method", th.StringType, required=True),
        th.Property("api_keys", th.ObjectType(additional_properties=th.StringType)),
        th.Property("headers", th.ObjectType(additional_properties=th.StringType)),
        th.Property("start_date", th.StringType),
        th.Property("pagination_request_style", th.StringType),
        th.Property("pagination_page_size", th.IntegerType),
        th.Property("max_records_total", th.IntegerType),
        th.Property("max_records_per_stream", th.IntegerType),
        th.Property("store_raw_json_message", th.BooleanType),
        th.Property("schema_overrides", th.ObjectType()),
        th.Property("streams", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def discover_streams(self) -> List:
        """Create stream instances from config['streams']."""
        cfg_streams: List[Dict[str, Any]] = self.config.get("streams", [])
        streams: List = []

        for s in cfg_streams:
            name = s["name"]
            path = s["path"]
            records_path = s.get("records_path")
            primary_keys = s.get("primary_keys", [])
            replication_key = s.get("replication_key")  # may be None
            next_page_token_path = s.get("next_page_token_path")
            params = s.get("params", {}) or {}
            iteration_config = s.get("iteration_config", {}) or {}
            pagination_results_limit = s.get("pagination_results_limit")

            common_kwargs = dict(
                tap=self,
                name=name,
                path=path,
                records_path=records_path,
                primary_keys=primary_keys,
                replication_key=replication_key,
                next_page_token_path=next_page_token_path,
                params=params,
                iteration_config=iteration_config,
                pagination_results_limit=pagination_results_limit,
            )

            if path == "/twitter/user/info":
                streams.append(TwitterUsersStream(**common_kwargs))
            elif path == "/twitter/user/mentions":
                streams.append(TwitterMentionsStream(**common_kwargs))
            elif path == "/twitter/user/last_tweets":
                streams.append(TwitterLatestStream(**common_kwargs))
            elif path == "/twitter/tweet/advanced_search":
                streams.append(TwitterAdvancedSearchStream(**common_kwargs))
            else:
                # Fallback to advanced search behavior (same paginator/shape)
                streams.append(TwitterAdvancedSearchStream(**common_kwargs))

        return streams
