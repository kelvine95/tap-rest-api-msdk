from __future__ import annotations

from typing import Any, Dict, List

from singer_sdk import Tap, typing as th

from .streams import (
    TwitterAdvancedSearchStream,
    TwitterMentionsStream,
    TwitterLatestStream,
    TwitterUsersStream,
)

PLUGIN_NAME = "tap-rest-api-msdk"


class TapRestApiMsdk(Tap):
    name = PLUGIN_NAME

    # Keep config schema broad but strict enough for validation.
    config_jsonschema = th.PropertiesList(
        th.Property("api_url", th.StringType, required=True),
        th.Property("auth_method", th.StringType, required=True),
        th.Property("api_keys", th.ObjectType(additional_properties=th.StringType)),
        th.Property("headers", th.ObjectType(additional_properties=th.StringType)),
        th.Property("start_date", th.StringType),  # e.g. 2025-08-15T00:00:00Z
        th.Property("pagination_request_style", th.StringType),
        th.Property("pagination_page_size", th.IntegerType),
        th.Property("max_records_total", th.IntegerType),
        th.Property("max_records_per_stream", th.IntegerType),
        th.Property("store_raw_json_message", th.BooleanType),
        th.Property("schema_overrides", th.ObjectType()),
        th.Property(
            "streams",
            th.ArrayType(
                th.ObjectType(
                    additional_properties=True  # allow stream-specific knobs
                )
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List:
        """Instantiate streams from YAML 'streams' section.

        We map by path to concrete stream classes. The `name` from config
        becomes the Singer stream name and is used for per-partition state.
        """
        cfg_streams: List[Dict[str, Any]] = self.config.get("streams", []) or []
        streams: List = []

        for s in cfg_streams:
            common_kwargs = dict(
                tap=self,
                name=s["name"],
                path=s["path"],
                records_path=s.get("records_path"),
                primary_keys=s.get("primary_keys", []),
                replication_key=s.get("replication_key"),
                next_page_token_path=s.get("next_page_token_path"),
                params=s.get("params", {}) or {},
                iteration_config=s.get("iteration_config", {}) or {},
                pagination_results_limit=s.get("pagination_results_limit"),
            )

            p = s["path"].strip()
            if p == "/twitter/user/info":
                streams.append(TwitterUsersStream(**common_kwargs))
            elif p == "/twitter/user/mentions":
                streams.append(TwitterMentionsStream(**common_kwargs))
            elif p == "/twitter/user/last_tweets":
                streams.append(TwitterLatestStream(**common_kwargs))
            elif p == "/twitter/tweet/advanced_search":
                streams.append(TwitterAdvancedSearchStream(**common_kwargs))
            else:
                # Default to advanced search semantics (uses $.tweets[*], cursor)
                streams.append(TwitterAdvancedSearchStream(**common_kwargs))

        return streams
