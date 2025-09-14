"""rest-api tap class (generic + twitterapi-friendly)."""

import copy
import json
from typing import Any, List, Optional, Dict

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
    tap_name = name

    _authenticator: Optional[APIAuthenticatorBase] = None

    # ------------- Common (per-stream) properties -------------
    common_properties = th.PropertiesList(
        th.Property("name", th.StringType, description="Stream name.", required=True),
        th.Property("path", th.StringType, description="Path appended to api_url."),
        th.Property("params", th.ObjectType(), default={}, description="Request query params."),
        th.Property("headers", th.ObjectType(), description="Request headers."),
        th.Property("records_path", th.StringType, description="JsonPath to records; default `$[*]`."),
        th.Property("primary_keys", th.ArrayType(th.StringType), description="Primary key column(s)."),
        th.Property("replication_key", th.StringType, description="Monotonic key for incremental replication."),
        th.Property(
            "except_keys",
            th.ArrayType(th.StringType),
            default=[],
            description="Keep these keys as JSON (don’t flatten deeper).",
        ),
        th.Property(
            "num_inference_records",
            th.NumberType,
            default=50,
            description="Sample size for schema inference when no schema is provided.",
        ),
        th.Property("start_date", th.DateTimeType, description="Initial start date when no state exists."),
        th.Property(
            "source_search_field",
            th.StringType,
            description="Param name to carry server-side filter (e.g. 'filter', 'query', 'since').",
        ),
        th.Property(
            "source_search_query",
            th.StringType,
            description="Template using $last_run_date to push server-side incremental filters.",
        ),
        th.Property(
            "keep_fields",
            th.ArrayType(th.StringType),
            description="Optional allowlist of flattened output columns to retain (PKs & replication_key auto-kept).",
        ),
        th.Property("next_page_token_path", th.StringType, description="JsonPath to next-page token."),
        # ----- Iteration support (generic) -----
        th.Property(
            "iteration_config",
            th.ObjectType(
                th.Property("iteration_type", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
                th.Property("api_param_key", th.StringType),
                th.Property("api_param_template", th.StringType, description="Python format string with {value}."),
                th.Property("metadata_key", th.StringType, description="Column to stamp the iteration value into."),
            ),
            description="Partition a stream by a list of values; inject into params and stamp as metadata.",
        ),
        # ----- Optional bookmark injection into an existing param (generic) -----
        th.Property(
            "bookmark_target_param",
            th.StringType,
            description="Param to modify with a bookmark-aware template (e.g., 'query').",
        ),
        th.Property(
            "bookmark_query_template",
            th.StringType,
            description="Template to append/inject bookmark into a param. "
                        "Vars: {existing} {last_run_date} {last_run_date_utc_twitter}.",
        ),
    )

    # ------------- Top-level properties -------------
    top_level_properties = th.PropertiesList(
        th.Property("api_url", th.StringType, required=True, description="Base API URL."),
        th.Property(
            "auth_method",
            th.StringType,
            default="no_auth",
            description="Auth: oauth | basic | api_key | bearer_token | aws | no_auth",
        ),
        th.Property("api_keys", th.ObjectType(), description="API-key pairs for api_key auth."),
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

        # Pagination defaults
        th.Property("pagination_request_style", th.StringType, default="default"),
        th.Property("pagination_response_style", th.StringType, default="default"),
        th.Property("use_request_body_not_params", th.BooleanType, default=False),
        th.Property("backoff_type", th.StringType, default=None),
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

        # Run safety & logging
        th.Property("max_records_total", th.IntegerType, default=5000),
        th.Property("max_records_per_stream", th.IntegerType),
        th.Property("drop_on_bookmark_le", th.BooleanType, default=True),
        th.Property("log_response_body_preview", th.IntegerType, default=1000),

        # Schema overrides (kept compatible with your yml)
        th.Property("schema_overrides", th.ObjectType(), description="Per-stream JSON-schema overrides."),
        # Streams array
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*common_properties.wrapped.values())),
            description="Configured streams.",
        ),
    )

    config_jsonschema = top_level_properties.to_dict()

    # ---------------------- Discovery ----------------------
    def discover_streams(self) -> List[DynamicStream]:  # type: ignore
        streams: List[DynamicStream] = []
        cfg = self.config

        for stream_cfg in cfg["streams"]:
            name = stream_cfg["name"]
            records_path = stream_cfg.get("records_path", cfg.get("records_path", "$[*]"))
            except_keys = stream_cfg.get("except_keys", cfg.get("except_keys", []))
            path = stream_cfg.get("path", cfg.get("path", ""))
            params = {**cfg.get("params", {}), **stream_cfg.get("params", {})}
            headers = {**cfg.get("headers", {}), **stream_cfg.get("headers", {})}
            start_date = stream_cfg.get("start_date", cfg.get("start_date", ""))
            replication_key = stream_cfg.get("replication_key", cfg.get("replication_key", ""))
            source_search_field = stream_cfg.get("source_search_field", cfg.get("source_search_field", ""))
            source_search_query = stream_cfg.get("source_search_query", cfg.get("source_search_query", ""))
            offset_records_jsonpath = stream_cfg.get(
                "offset_records_jsonpath", cfg.get("offset_records_jsonpath")
            )
            keep_fields = stream_cfg.get("keep_fields", [])
            iteration_config = stream_cfg.get("iteration_config", None)
            bookmark_target_param = stream_cfg.get("bookmark_target_param")
            bookmark_query_template = stream_cfg.get("bookmark_query_template")

            # Build schema: file path, inline dict, or infer
            schema: Dict[str, Any]
            schema_setting = stream_cfg.get("schema")
            if isinstance(schema_setting, str):
                self.logger.info("Stream '%s': using schema file at %s", name, schema_setting)
                with open(schema_setting, "r") as f:
                    schema = json.load(f)
            elif isinstance(schema_setting, dict):
                self.logger.info("Stream '%s': using inline schema", name)
                b = SchemaBuilder()
                b.add_schema(schema_setting)
                schema = b.to_schema()
            else:
                self.logger.info("Stream '%s': inferring schema from API", name)
                schema = self._infer_schema(
                    records_path=records_path,
                    except_keys=except_keys,
                    inference_records=int(stream_cfg.get("num_inference_records", cfg["num_inference_records"])),
                    path=path,
                    params=params,
                    headers=headers,
                )

            # Apply schema overrides if provided
            overrides = (cfg.get("schema_overrides") or {}).get(name)
            if overrides:
                self.logger.info("Stream '%s': applying schema_overrides", name)
                props = schema.setdefault("properties", {})
                for k, v in (overrides.get("properties") or {}).items():
                    props[k] = v

            # If keep_fields is specified, trim schema to those + keys we must keep
            if keep_fields:
                props = schema.setdefault("properties", {})
                must_keep = set(keep_fields) | set(stream_cfg.get("primary_keys", []))
                if replication_key:
                    must_keep.add(replication_key)
                if iteration_config and iteration_config.get("metadata_key"):
                    must_keep.add(iteration_config["metadata_key"])
                schema["properties"] = {k: v for k, v in props.items() if k in must_keep}

            streams.append(
                DynamicStream(
                    tap=self,
                    name=name,
                    path=path,
                    params=params,
                    headers=headers,
                    records_path=records_path,
                    primary_keys=stream_cfg.get("primary_keys", cfg.get("primary_keys", [])),
                    replication_key=replication_key,
                    except_keys=except_keys,
                    next_page_token_path=stream_cfg.get("next_page_token_path") or cfg.get("next_page_token_path"),
                    pagination_request_style=cfg["pagination_request_style"],
                    pagination_response_style=cfg["pagination_response_style"],
                    pagination_page_size=cfg.get("pagination_page_size"),
                    pagination_results_limit=stream_cfg.get("pagination_results_limit") or cfg.get("pagination_results_limit"),
                    pagination_next_page_param=cfg.get("pagination_next_page_param"),
                    pagination_limit_per_page_param=cfg.get("pagination_limit_per_page_param"),
                    pagination_total_limit_param=cfg.get("pagination_total_limit_param"),
                    pagination_initial_offset=cfg.get("pagination_initial_offset", 1),
                    offset_records_jsonpath=offset_records_jsonpath,
                    schema=schema,
                    start_date=start_date,
                    source_search_field=source_search_field,
                    source_search_query=source_search_query,
                    use_request_body_not_params=cfg.get("use_request_body_not_params"),
                    backoff_type=cfg.get("backoff_type"),
                    backoff_param=cfg.get("backoff_param"),
                    backoff_time_extension=cfg.get("backoff_time_extension"),
                    store_raw_json_message=cfg.get("store_raw_json_message"),
                    authenticator=self._authenticator,
                    iteration_config=iteration_config,
                    keep_fields=keep_fields,
                    bookmark_target_param=bookmark_target_param,
                    bookmark_query_template=bookmark_query_template,
                )
            )

        # cross-stream counter
        setattr(self, "_global_emitted_total", 0)
        return streams

    # ---------------------- Schema inference ----------------------
    def _infer_schema(
        self,
        records_path: str,
        except_keys: list,
        inference_records: int,
        path: str,
        params: dict,
        headers: dict,
    ) -> Dict[str, Any]:
        auth_method = self.config.get("auth_method", "")
        self.http_auth = None

        if auth_method and auth_method != "no_auth":
            get_authenticator(self)
            if auth_method == "oauth" and isinstance(self._authenticator, ConfigurableOAuthAuthenticator):
                self._authenticator.get_initial_oauth_token()
            headers.update(getattr(self._authenticator, "auth_headers", {}))
            params.update(getattr(self._authenticator, "auth_params", {}))

        r = requests.get(self.config["api_url"] + path, auth=self.http_auth, params=params, headers=headers)
        if not r.ok:
            self.logger.error("Schema probe failed (%s %s): %s", r.status_code, r.reason, r.text)
            raise ValueError(r.text)

        records = extract_jsonpath(records_path, input=r.json())

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())
        for i, record in enumerate(records):
            if not isinstance(record, dict):
                self.logger.error("Schema inference requires object records; got %s.", type(record).__name__)
                raise ValueError("Input must be a dict object.")
            flat = flatten_json(record, except_keys, store_raw_json_message=False)
            builder.add_object(flat)
            if self.config.get("store_raw_json_message"):
                builder.add_object({"_sdc_raw_json": {}})
            if i >= inference_records:
                break

        self.logger.debug("%s", builder.to_json(indent=2))
        return builder.to_schema()
