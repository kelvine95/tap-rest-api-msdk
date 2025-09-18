# tap.py
"""Generic REST tap with optional TwitterAPI helpers (backward compatible)."""

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List, Optional, Set

from genson import SchemaBuilder
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_rest_api_msdk.streams import DynamicStream
from tap_rest_api_msdk.utils import flatten_json
from tap_rest_api_msdk.auth import (
    ConfigurableOAuthAuthenticator,
    get_authenticator,
)


class TapRestApiMsdk(Tap):
    """REST tap supporting dynamic streams with optional Twitter special-casing."""

    name = "tap-rest-api-msdk"

    # cache authenticator across streams
    _authenticator: Optional[APIAuthenticatorBase] = None
    http_auth = None

    # Twitter in-memory state (used only if twitter_stream_type is provided)
    _tw_collected_tweet_ids: Set[str] = set()
    _tw_pending_ops: Dict[str, Set[str]] = {"replies": set(), "quotes": set()}

    # ---- Common per-stream config schema (BACKWARD COMPATIBLE) ----
    common_properties = th.PropertiesList(
        th.Property("name", th.StringType, description="Stream name."),
        th.Property("path", th.StringType, required=False),
        th.Property("records_path", th.StringType, required=False, description="JSONPath"),
        th.Property("params", th.ObjectType(), required=False, default={}),
        th.Property("headers", th.ObjectType(), required=False, default={}),
        th.Property("primary_keys", th.ArrayType(th.StringType), required=False, default=[]),
        th.Property("replication_key", th.StringType, required=False),
        th.Property("except_keys", th.ArrayType(th.StringType), required=False, default=[]),
        th.Property("num_inference_records", th.IntegerType, required=False, default=50),
        th.Property("start_date", th.DateTimeType, required=False),
        th.Property("source_search_field", th.StringType, required=False),
        th.Property("source_search_query", th.StringType, required=False),
        th.Property("offset_records_jsonpath", th.StringType, required=False),

        # Pagination knobs (preserve legacy names)
        th.Property("next_page_token_path", th.StringType, required=False),
        th.Property("pagination_request_style", th.StringType, required=False, default="default"),
        th.Property("pagination_response_style", th.StringType, required=False, default="default"),
        th.Property("pagination_page_size", th.IntegerType, required=False),
        th.Property("pagination_results_limit", th.IntegerType, required=False),
        th.Property("pagination_next_page_param", th.StringType, required=False),
        th.Property("pagination_limit_per_page_param", th.StringType, required=False),
        th.Property("pagination_total_limit_param", th.StringType, required=False, default="total"),
        th.Property("pagination_initial_offset", th.IntegerType, required=False, default=1),

        # Backoff & extras
        th.Property("use_request_body_not_params", th.BooleanType, required=False, default=False),
        th.Property("backoff_type", th.StringType, required=False, allowed_values=[None, "message", "header"]),
        th.Property("backoff_param", th.StringType, required=False, default="Retry-After"),
        th.Property("backoff_time_extension", th.IntegerType, required=False, default=0),
        th.Property("store_raw_json_message", th.BooleanType, required=False, default=False),

        # Twitter routing (optional)
        th.Property("twitter_stream_type", th.StringType, required=False),
        th.Property("twitter_usernames", th.ArrayType(th.StringType), required=False),
        th.Property("twitter_hashtags", th.ArrayType(th.StringType), required=False),
        th.Property("twitter_parent_streams", th.ArrayType(th.StringType), required=False),
        th.Property("twitter_max_per_run", th.IntegerType, required=False, default=20),
    )

    # ---- Top-level config schema ----
    top_level_properties = th.PropertiesList(
        th.Property("api_url", th.StringType, required=True, description="Base URL"),
        th.Property(
            "auth_method",
            th.StringType,
            required=False,
            default="no_auth",
            description="one of: no_auth, api_key, bearer_token, basic, oauth, aws",
        ),
        # SINGLE place to define API key(s). No duplication required.
        th.Property(
            "api_keys",
            th.ObjectType(),
            required=False,
            description="Header-style API keys, e.g. {\"X-API-Key\":\"<key>\"}",
        ),
        th.Property("headers", th.ObjectType(), required=False, default={}, description="Global headers"),
        th.Property("params", th.ObjectType(), required=False, default={}, description="Global query params"),

        # OAuth/bearer/basic/etc supported via existing helpers
        th.Property("client_id", th.StringType, required=False),
        th.Property("client_secret", th.StringType, required=False),
        th.Property("username", th.StringType, required=False),
        th.Property("password", th.StringType, required=False),
        th.Property("bearer_token", th.StringType, required=False),
        th.Property("refresh_token", th.StringType, required=False),
        th.Property("grant_type", th.StringType, required=False),
        th.Property("scope", th.StringType, required=False),
        th.Property("access_token_url", th.StringType, required=False),
        th.Property("redirect_uri", th.StringType, required=False),
        th.Property("oauth_extras", th.ObjectType(), required=False),
        th.Property("oauth_expiration_secs", th.IntegerType, required=False),

        # Pagination defaults (can be overridden per-stream)
        th.Property("next_page_token_path", th.StringType, required=False),
        th.Property("pagination_request_style", th.StringType, required=False, default="default"),
        th.Property("pagination_response_style", th.StringType, required=False, default="default"),
        th.Property("pagination_page_size", th.IntegerType, required=False),
        th.Property("pagination_results_limit", th.IntegerType, required=False),
        th.Property("pagination_next_page_param", th.StringType, required=False),
        th.Property("pagination_limit_per_page_param", th.StringType, required=False),
        th.Property("pagination_total_limit_param", th.StringType, required=False, default="total"),
        th.Property("pagination_initial_offset", th.IntegerType, required=False, default=1),
        th.Property("offset_records_jsonpath", th.StringType, required=False),

        th.Property("use_request_body_not_params", th.BooleanType, required=False, default=False),
        th.Property("backoff_type", th.StringType, required=False, allowed_values=[None, "message", "header"]),
        th.Property("backoff_param", th.StringType, required=False, default="Retry-After"),
        th.Property("backoff_time_extension", th.IntegerType, required=False, default=0),
        th.Property("store_raw_json_message", th.BooleanType, required=False, default=False),

        # Twitter top-level defaults (optional)
        th.Property("twitter_usernames", th.ArrayType(th.StringType), required=False),
        th.Property("twitter_hashtags", th.ArrayType(th.StringType), required=False),

        # Dynamic stream array (legacy compatible)
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*common_properties.wrapped.values())),
            required=False,
            description="Dynamic stream definitions.",
        ),
    )

    # publish config jsonschema
    config_jsonschema = top_level_properties.to_dict()

    # ----------------- Discovery -----------------
    def discover_streams(self) -> List[DynamicStream]:
        streams: List[DynamicStream] = []

        for s in self.config.get("streams", []):
            # merge per-stream with top-level for headers/params
            headers = {**self.config.get("headers", {}), **s.get("headers", {})}
            # inject API keys exactly once here (no double entry!)
            for hk, hv in (self.config.get("api_keys") or {}).items():
                headers.setdefault(hk, hv)

            params = {**self.config.get("params", {}), **s.get("params", {})}

            # schema resolution
            schema: Dict[str, Any]
            schema_cfg = s.get("schema")
            if isinstance(schema_cfg, str):
                with open(schema_cfg, "r") as f:
                    schema = json.load(f)
            elif isinstance(schema_cfg, dict):
                b = SchemaBuilder()
                b.add_schema(schema_cfg)
                schema = b.to_schema()
            else:
                # infer schema with one sample request, if possible
                schema = self._infer_schema(
                    path=s.get("path", ""),
                    headers=headers.copy(),
                    params=params.copy(),
                    records_path=s.get("records_path", "$[*]"),
                    except_keys=s.get("except_keys", []),
                    max_records=int(s.get("num_inference_records", 50)),
                )

            streams.append(
                DynamicStream(
                    tap=self,
                    name=s["name"],
                    path=s.get("path", ""),
                    records_path=s.get("records_path", "$[*]"),
                    params=params,
                    headers=headers,
                    primary_keys=s.get("primary_keys", []),
                    replication_key=s.get("replication_key"),
                    except_keys=s.get("except_keys", []),
                    next_page_token_path=s.get("next_page_token_path", self.config.get("next_page_token_path")),
                    pagination_request_style=s.get("pagination_request_style", self.config.get("pagination_request_style", "default")),
                    pagination_response_style=s.get("pagination_response_style", self.config.get("pagination_response_style", "default")),
                    pagination_page_size=s.get("pagination_page_size", self.config.get("pagination_page_size")),
                    pagination_results_limit=s.get("pagination_results_limit", self.config.get("pagination_results_limit")),
                    pagination_next_page_param=s.get("pagination_next_page_param", self.config.get("pagination_next_page_param")),
                    pagination_limit_per_page_param=s.get("pagination_limit_per_page_param", self.config.get("pagination_limit_per_page_param")),
                    pagination_total_limit_param=s.get("pagination_total_limit_param", self.config.get("pagination_total_limit_param", "total")),
                    pagination_initial_offset=s.get("pagination_initial_offset", self.config.get("pagination_initial_offset", 1)),
                    offset_records_jsonpath=s.get("offset_records_jsonpath", self.config.get("offset_records_jsonpath")),
                    schema=schema,
                    start_date=s.get("start_date", self.config.get("start_date")),
                    source_search_field=s.get("source_search_field", self.config.get("source_search_field")),
                    source_search_query=s.get("source_search_query", self.config.get("source_search_query")),
                    use_request_body_not_params=s.get("use_request_body_not_params", self.config.get("use_request_body_not_params", False)),
                    backoff_type=s.get("backoff_type", self.config.get("backoff_type")),
                    backoff_param=s.get("backoff_param", self.config.get("backoff_param", "Retry-After")),
                    backoff_time_extension=s.get("backoff_time_extension", self.config.get("backoff_time_extension", 0)),
                    store_raw_json_message=s.get("store_raw_json_message", self.config.get("store_raw_json_message", False)),
                    authenticator=self._authenticator,
                    # optional twitter routing
                    twitter_stream_type=s.get("twitter_stream_type"),
                    twitter_usernames=s.get("twitter_usernames", self.config.get("twitter_usernames", [])),
                    twitter_hashtags=s.get("twitter_hashtags", self.config.get("twitter_hashtags", [])),
                    twitter_parent_streams=s.get("twitter_parent_streams", []),
                    twitter_max_per_run=s.get("twitter_max_per_run", 20),
                    tap_instance=self,
                )
            )

        return streams

    # --------------- Helpers ---------------
    def _ensure_auth(self, headers: Dict[str, Any], params: Dict[str, Any]) -> None:
        auth_method = self.config.get("auth_method", "no_auth")
        if auth_method and auth_method != "no_auth":
            # set or reuse authenticator
            get_authenticator(self)
            if auth_method == "oauth" and isinstance(self._authenticator, ConfigurableOAuthAuthenticator):
                self._authenticator.get_initial_oauth_token()

            # merge any runtime auth headers/params (do not clobber explicit per-stream settings)
            headers.update(getattr(self._authenticator, "auth_headers", {}) or {})
            params.update(getattr(self._authenticator, "auth_params", {}) or {})

    def _infer_schema(
        self,
        path: str,
        headers: Dict[str, Any],
        params: Dict[str, Any],
        records_path: str,
        except_keys: List[str],
        max_records: int,
    ) -> Dict[str, Any]:
        if not path:
            # no path => empty schema
            return th.PropertiesList().to_dict()

        # plug in auth if configured
        self._ensure_auth(headers, params)

        import requests

        r = requests.get(
            self.config["api_url"] + path,
            headers=headers,
            params=params,
            auth=self.http_auth,
            timeout=60,
        )
        if not r.ok:
            # expose server reason in logs AND raise (so discovery failure is explicit)
            self.logger.error("Schema sample request failed %s %s: %s", r.status_code, path, r.text)
            r.raise_for_status()

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())

        cnt = 0
        for rec in extract_jsonpath(records_path, input=r.json()):
            if not isinstance(rec, dict):
                continue
            flat = flatten_json(rec, except_keys, store_raw_json_message=False)
            builder.add_object(flat)
            if self.config.get("store_raw_json_message"):
                builder.add_object({"_sdc_raw_json": {}})
            cnt += 1
            if cnt >= max_records:
                break

        return builder.to_schema()

    # ---- Twitter state (optional) ----
    def add_collected_tweet_id(self, tweet_id: str) -> None:
        self._tw_collected_tweet_ids.add(tweet_id)

    def get_collected_tweet_ids(self) -> Set[str]:
        return set(self._tw_collected_tweet_ids)

    def add_pending_tweet_id(self, tweet_id: str, op: str) -> None:
        if op in self._tw_pending_ops:
            self._tw_pending_ops[op].add(tweet_id)

    def get_pending_tweet_ids(self, op: str) -> Set[str]:
        return set(self._tw_pending_ops.get(op, set()))

    def remove_pending_tweet_id(self, tweet_id: str, op: str) -> None:
        if op in self._tw_pending_ops:
            self._tw_pending_ops[op].discard(tweet_id)
