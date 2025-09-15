"""Config-driven REST tap with safe discovery and Twitter-friendly adapters."""

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List, Optional

import requests
from genson import SchemaBuilder
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_rest_api_msdk.auth import ConfigurableOAuthAuthenticator, get_authenticator
from tap_rest_api_msdk.streams import DynamicStream
from tap_rest_api_msdk.utils import flatten_json


class TapRestApiMsdk(Tap):
    name = "tap-rest-api-msdk"
    tap_name = name  # used by SDK auth base

    _authenticator: Optional[APIAuthenticatorBase] = None

    # ---------- Common stream-level options ----------
    common_properties = th.PropertiesList(
        th.Property("path", th.StringType),
        th.Property("params", th.ObjectType(), default={}),
        th.Property("headers", th.ObjectType(), default={}),
        th.Property("records_path", th.StringType, description="JSONPath to records."),
        th.Property("primary_keys", th.ArrayType(th.StringType), default=[]),
        th.Property("replication_key", th.StringType),
        th.Property("except_keys", th.ArrayType(th.StringType), default=[]),
        th.Property("num_inference_records", th.IntegerType, default=50),
        th.Property("start_date", th.DateTimeType),
        th.Property("source_search_field", th.StringType),
        th.Property("source_search_query", th.StringType),
        th.Property("next_page_token_path", th.StringType),
        th.Property("pagination_request_style", th.StringType, default="default"),
        th.Property("pagination_response_style", th.StringType, default="default"),
        th.Property("pagination_page_size", th.IntegerType),
        th.Property("pagination_results_limit", th.IntegerType),
        th.Property("pagination_next_page_param", th.StringType),
        th.Property("pagination_limit_per_page_param", th.StringType),
        th.Property("pagination_total_limit_param", th.StringType, default="total"),
        th.Property("pagination_initial_offset", th.IntegerType, default=1),
        th.Property("offset_records_jsonpath", th.StringType),
        th.Property("use_request_body_not_params", th.BooleanType, default=False),
        th.Property("backoff_type", th.StringType, allowed_values=[None, "message", "header"], default=None),
        th.Property("backoff_param", th.StringType, default="Retry-After"),
        th.Property("backoff_time_extension", th.IntegerType, default=0),
        th.Property("store_raw_json_message", th.BooleanType, default=False),
        # New:
        th.Property("replication_request_adapter", th.ObjectType(), description="Translate bookmarks into request params."),
        th.Property("disable_discovery_probe", th.BooleanType, default=False, description="Skip live request during discovery."),
    )

    # ---------- Top-level config ----------
    top_level_properties = th.PropertiesList(
        th.Property("api_url", th.StringType, required=True),
        th.Property("auth_method", th.StringType, default="no_auth"),
        th.Property("api_keys", th.ObjectType()),
        th.Property("client_id", th.StringType),
        th.Property("client_secret", th.StringType),
        th.Property("username", th.StringType),
        th.Property("password", th.StringType),
        th.Property("bearer_token", th.StringType),
        th.Property("refresh_token", th.StringType),
        th.Property("grant_type", th.StringType),
        th.Property("scope", th.StringType),
        th.Property("access_token_url", th.StringType),
        th.Property("redirect_uri", th.StringType),
        th.Property("oauth_extras", th.ObjectType()),
        th.Property("oauth_expiration_secs", th.IntegerType),
        th.Property("aws_credentials", th.ObjectType()),
        th.Property("next_page_token_path", th.StringType),
        th.Property("pagination_request_style", th.StringType, default="default"),
        th.Property("pagination_response_style", th.StringType, default="default"),
        th.Property("use_request_body_not_params", th.BooleanType, default=False),
        th.Property("backoff_type", th.StringType, allowed_values=[None, "message", "header"], default=None),
        th.Property("backoff_param", th.StringType, default="Retry-After"),
        th.Property("backoff_time_extension", th.IntegerType, default=0),
        th.Property("store_raw_json_message", th.BooleanType, default=False),
        th.Property("pagination_page_size", th.IntegerType),
        th.Property("pagination_results_limit", th.IntegerType),
        th.Property("pagination_next_page_param", th.StringType),
        th.Property("pagination_limit_per_page_param", th.StringType),
        th.Property("pagination_total_limit_param", th.StringType, default="total"),
        th.Property("pagination_initial_offset", th.IntegerType, default=1),
        th.Property("offset_records_jsonpath", th.StringType),
        th.Property("schema_overrides", th.ObjectType(), description="Singer schema fragments to merge per stream name."),
        th.Property("streams", th.ArrayType(th.ObjectType())),
        # Budgets + resiliency:
        th.Property("max_records_total", th.IntegerType),
        th.Property("max_records_per_stream", th.IntegerType),
        th.Property("soft_fail_status_codes", th.ArrayType(th.IntegerType), description="Status codes to treat as empty."),
        th.Property("request_timeout_secs", th.IntegerType, description="Per-request timeout in seconds."),
    )

    # extend top-level with the common stream fields
    for prop in common_properties.wrapped.values():
        top_level_properties.append(prop)

    # stream schema entry (optional)
    stream_properties = th.PropertiesList()
    stream_properties.wrapped = copy.copy(common_properties.wrapped)
    stream_properties.append(th.Property("name", th.StringType, required=True))
    stream_properties.append(
        th.Property(
            "schema",
            th.CustomType({"anyOf": [{"type": "string"}, {"type": "null"}, {"type": "object"}]}),
            description="Singer schema dict OR a path to a JSON file containing a schema."
        )
    )

    top_level_properties.append(
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*stream_properties.wrapped.values())),
        )
    )

    config_jsonschema = top_level_properties.to_dict()

    # ---------- Discovery ----------
    def _infer_schema(
        self,
        stream_cfg: Dict[str, Any],
        records_path: str,
        except_keys: list,
        inference_records: int,
        path: str,
        params: dict,
        headers: dict,
        disable_probe: bool = False,
    ) -> Dict[str, Any]:
        """Attempt to infer schema safely, with graceful fallback."""
        if disable_probe:
            self.logger.info("Stream '%s': discovery probe disabled; using empty schema.", stream_cfg["name"])
            return th.PropertiesList().to_dict()

        # If the endpoint requires a parameter which isn't present (e.g., userName),
        # a probe would 400. Detect a required iteration key and provide the first value.
        iter_cfg = stream_cfg.get("iteration_config") or {}
        params = dict(params or {})
        api_param_key = iter_cfg.get("api_param_key")
        values = iter_cfg.get("values") or []
        templ = iter_cfg.get("api_param_template", "{value}")
        if api_param_key and values:
            params.setdefault(api_param_key, templ.format(value=values[0]))

        # Obtain auth if configured
        auth_method = self.config.get("auth_method", "no_auth")
        http_auth = None
        if auth_method != "no_auth":
            get_authenticator(self)
            if auth_method == "oauth" and isinstance(self._authenticator, ConfigurableOAuthAuthenticator):
                self._authenticator.get_initial_oauth_token()
            headers.update(getattr(self._authenticator, "auth_headers", {}))
            params.update(getattr(self._authenticator, "auth_params", {}))
            http_auth = getattr(self, "http_auth", None)

        url = (self.config["api_url"].rstrip("/") + path)
        try:
            r = requests.get(url, auth=http_auth, params=params, headers=headers, timeout=float(self.config.get("request_timeout_secs") or 30))
            if not r.ok:
                self.logger.error("Schema probe failed (%s): %s", f"{r.status_code} {r.reason}", (r.text or "")[:500])
                # safe fallback: empty object schema (flatten_json will still emit)
                return th.PropertiesList().to_dict()
            records = extract_jsonpath(records_path, input=r.json())
        except Exception as exc:
            self.logger.exception("Schema probe error for %s", url)
            # fallback
            return th.PropertiesList().to_dict()

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())
        for i, record in enumerate(records):
            if not isinstance(record, dict):
                # flatten non-dict values into a single value column
                record = {"value": record}
            flat = flatten_json(record, except_keys, store_raw_json_message=False)
            builder.add_object(flat)
            if self.config.get("store_raw_json_message"):
                builder.add_object({"_sdc_raw_json": {}})
            if i >= int(inference_records or 50):
                break
        return builder.to_schema()

    def _merge_schema_overrides(self, stream_name: str, base_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Merge user-provided schema fragments (e.g., to ensure replication_key presence)."""
        overrides = (self.config.get("schema_overrides") or {}).get(stream_name)
        if not overrides:
            return base_schema
        out = json.loads(json.dumps(base_schema))  # deep copy
        # naive deep-merge
        def merge(a, b):
            for k, v in b.items():
                if isinstance(v, dict) and isinstance(a.get(k), dict):
                    merge(a[k], v)
                else:
                    a[k] = v
        merge(out, overrides)
        return out

    def discover_streams(self) -> List[DynamicStream]:
        streams: List[DynamicStream] = []
        for s in self.config.get("streams", []):
            # Resolve config precedence (stream overrides top-level)
            path = s.get("path", self.config.get("path", ""))
            params = {**(self.config.get("params") or {}), **(s.get("params") or {})}
            headers = {**(self.config.get("headers") or {}), **(s.get("headers") or {})}
            records_path = s.get("records_path", self.config.get("records_path", "$[*]"))
            except_keys = s.get("except_keys", self.config.get("except_keys", []))
            start_date = s.get("start_date", self.config.get("start_date"))
            replication_key = s.get("replication_key", self.config.get("replication_key"))
            offset_records_jsonpath = s.get("offset_records_jsonpath", self.config.get("offset_records_jsonpath"))

            # schema selection
            schema: Dict[str, Any] = {}
            schema_cfg = s.get("schema")
            disable_probe = bool(s.get("disable_discovery_probe", self.config.get("disable_discovery_probe", False)))
            if isinstance(schema_cfg, str):
                self.logger.info("Stream '%s': loading schema from file.", s["name"])
                with open(schema_cfg, "r") as f:
                    schema = json.load(f)
            elif isinstance(schema_cfg, dict):
                self.logger.info("Stream '%s': using inline schema.", s["name"])
                schema = schema_cfg
            else:
                self.logger.info("Stream '%s': inferring schema from API", s["name"])
                schema = self._infer_schema(
                    stream_cfg=s,
                    records_path=records_path,
                    except_keys=except_keys,
                    inference_records=int(s.get("num_inference_records", self.config.get("num_inference_records", 50))),
                    path=path,
                    params=params,
                    headers=headers,
                    disable_probe=disable_probe,
                )

            # merge overrides (e.g., to ensure replication_key fields and nullability)
            schema = self._merge_schema_overrides(s["name"], schema)

            streams.append(
                DynamicStream(
                    tap=self,
                    name=s["name"],
                    path=path,
                    params=params,
                    headers=headers,
                    records_path=records_path,
                    primary_keys=s.get("primary_keys", self.config.get("primary_keys", [])),
                    replication_key=replication_key,
                    except_keys=except_keys,
                    next_page_token_path=s.get("next_page_token_path", self.config.get("next_page_token_path")),
                    pagination_request_style=s.get("pagination_request_style", self.config.get("pagination_request_style", "default")),
                    pagination_response_style=s.get("pagination_response_style", self.config.get("pagination_response_style", "default")),
                    pagination_page_size=s.get("pagination_page_size", self.config.get("pagination_page_size")),
                    pagination_results_limit=s.get("pagination_results_limit", self.config.get("pagination_results_limit")),
                    pagination_next_page_param=s.get("pagination_next_page_param", self.config.get("pagination_next_page_param")),
                    pagination_limit_per_page_param=s.get("pagination_limit_per_page_param", self.config.get("pagination_limit_per_page_param")),
                    pagination_total_limit_param=s.get("pagination_total_limit_param", self.config.get("pagination_total_limit_param", "total")),
                    pagination_initial_offset=s.get("pagination_initial_offset", self.config.get("pagination_initial_offset", 1)),
                    offset_records_jsonpath=offset_records_jsonpath,
                    schema=schema,
                    start_date=start_date,
                    source_search_field=s.get("source_search_field", self.config.get("source_search_field")),
                    source_search_query=s.get("source_search_query", self.config.get("source_search_query")),
                    use_request_body_not_params=s.get("use_request_body_not_params", self.config.get("use_request_body_not_params", False)),
                    backoff_type=s.get("backoff_type", self.config.get("backoff_type")),
                    backoff_param=s.get("backoff_param", self.config.get("backoff_param", "Retry-After")),
                    backoff_time_extension=s.get("backoff_time_extension", self.config.get("backoff_time_extension", 0)),
                    store_raw_json_message=s.get("store_raw_json_message", self.config.get("store_raw_json_message", False)),
                    authenticator=self._authenticator,
                    replication_request_adapter=s.get("replication_request_adapter"),
                    disable_discovery_probe=disable_probe,
                )
            )
        return streams
