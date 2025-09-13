# tap.py
"""REST API tap with iteration, partitioned state, and run caps for cost control."""

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
from tap_rest_api_msdk.streams import IterativeDynamicStream
from tap_rest_api_msdk.utils import flatten_json


class TapRestApiMsdk(Tap):
    name = "tap-rest-api-msdk"
    tap_name = name
    _authenticator: Optional[APIAuthenticatorBase] = None

    # ---------- Common stream-level properties ----------
    common_properties = th.PropertiesList(
        th.Property("path", th.StringType),
        th.Property("params", th.ObjectType(), default={}),
        th.Property("headers", th.ObjectType()),
        th.Property("records_path", th.StringType),
        th.Property("primary_keys", th.ArrayType(th.StringType)),
        th.Property("replication_key", th.StringType),
        th.Property("except_keys", th.ArrayType(th.StringType), default=[]),
        th.Property("num_inference_records", th.NumberType, default=50),
        th.Property("start_date", th.DateTimeType),
        th.Property("source_search_field", th.StringType),
        th.Property("source_search_query", th.StringType),

        # Iteration config
        th.Property(
            "iteration_config",
            th.ObjectType(
                th.Property("iteration_type", th.StringType, allowed_values=["usernames","keywords","handles","hashtags","custom","none"]),
                th.Property("values", th.ArrayType(th.StringType)),
                th.Property("api_param_key", th.StringType),
                th.Property("api_param_template", th.StringType, default="{value}"),
                th.Property("path_template", th.StringType),
                th.Property("metadata_key", th.StringType),
                th.Property("continue_on_error", th.BooleanType, default=True),
                th.Property("add_extracted_at", th.BooleanType, default=True),
            ),
        ),

        # Pagination and cursor
        th.Property("next_page_token_path", th.StringType),
        th.Property("pagination_request_style", th.StringType),
        th.Property("pagination_response_style", th.StringType),
        th.Property("pagination_page_size", th.IntegerType),
        th.Property("pagination_results_limit", th.IntegerType),
        th.Property("pagination_next_page_param", th.StringType),
        th.Property("pagination_limit_per_page_param", th.StringType),
        th.Property("pagination_total_limit_param", th.StringType),
        th.Property("pagination_initial_offset", th.IntegerType),
        th.Property("offset_records_jsonpath", th.StringType),
    )

    # ---------- Tap-level properties ----------
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
        th.Property("oauth_expiration_secs", th.IntegerType, default=None),
        th.Property("aws_credentials", th.ObjectType(), default=None),
        th.Property("next_page_token_path", th.StringType, default=None),
        th.Property("pagination_request_style", th.StringType, default="default"),
        th.Property("pagination_response_style", th.StringType, default="default"),
        th.Property("use_request_body_not_params", th.BooleanType, default=False),
        th.Property("backoff_type", th.StringType, allowed_values=[None,"message","header"], default=None),
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
        th.Property("global_iteration_config", th.ObjectType()),

        # NEW: run caps to control costs
        th.Property("max_records_per_stream", th.IntegerType),
        th.Property("max_records_total", th.IntegerType),
    )

    # allow stream-level props at the top level for inheritance
    for prop in common_properties.wrapped.values():
        top_level_properties.append(prop)

    # per-stream schema container
    stream_properties = th.PropertiesList()
    stream_properties.wrapped = copy.copy(common_properties.wrapped)
    stream_properties.append(th.Property("name", th.StringType, required=True))
    stream_properties.append(
        th.Property(
            "schema",
            th.CustomType({"anyOf": [{"type": "string"}, {"type": "null"}, {"type": "object"}]}),
        )
    )

    # streams array
    top_level_properties.append(
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*stream_properties.wrapped.values())),
        )
    )

    config_jsonschema = top_level_properties.to_dict()

    # ---------- Discovery ----------
    def discover_streams(self) -> List[IterativeDynamicStream]:
        streams: List[IterativeDynamicStream] = []

        for stream_config in self.config.get("streams", []):
            records_path = stream_config.get("records_path", self.config.get("records_path", "$[*]"))
            except_keys = stream_config.get("except_keys", self.config.get("except_keys", []))
            path = stream_config.get("path", self.config.get("path", ""))
            params = {**self.config.get("params", {}), **stream_config.get("params", {})}
            headers = {**self.config.get("headers", {}), **stream_config.get("headers", {})}
            start_date = stream_config.get("start_date", self.config.get("start_date", ""))
            replication_key = stream_config.get("replication_key", self.config.get("replication_key", ""))

            source_search_field = stream_config.get("source_search_field", self.config.get("source_search_field", ""))
            source_search_query = stream_config.get("source_search_query", self.config.get("source_search_query", ""))
            offset_records_jsonpath = stream_config.get("offset_records_jsonpath", self.config.get("offset_records_jsonpath", None))

            next_page_token_path = stream_config.get("next_page_token_path", self.config.get("next_page_token_path", None))
            pagination_request_style = stream_config.get("pagination_request_style", self.config.get("pagination_request_style", "default"))
            pagination_response_style = stream_config.get("pagination_response_style", self.config.get("pagination_response_style", "default"))
            pagination_page_size = stream_config.get("pagination_page_size", self.config.get("pagination_page_size", None))
            pagination_results_limit = stream_config.get("pagination_results_limit", self.config.get("pagination_results_limit", None))
            pagination_next_page_param = stream_config.get("pagination_next_page_param", self.config.get("pagination_next_page_param", None))
            pagination_limit_per_page_param = stream_config.get("pagination_limit_per_page_param", self.config.get("pagination_limit_per_page_param", None))
            pagination_total_limit_param = stream_config.get("pagination_total_limit_param", self.config.get("pagination_total_limit_param", "total"))
            pagination_initial_offset = stream_config.get("pagination_initial_offset", self.config.get("pagination_initial_offset", 1))

            iteration_config = stream_config.get("iteration_config", self.config.get("global_iteration_config", {}))

            # Build (or infer) schema using first iteration value
            schema = self._build_stream_schema_for_discovery(
                stream_config=stream_config,
                records_path=records_path,
                except_keys=except_keys,
                path=path,
                params=params,
                headers=headers,
                iteration_config=iteration_config,
            )

            streams.append(
                IterativeDynamicStream(
                    tap=self,
                    name=stream_config["name"],
                    path=path,
                    params=params,
                    headers=headers,
                    records_path=records_path,
                    primary_keys=stream_config.get("primary_keys", self.config.get("primary_keys", [])),
                    replication_key=replication_key,
                    except_keys=except_keys,
                    next_page_token_path=next_page_token_path,
                    pagination_request_style=pagination_request_style,
                    pagination_response_style=pagination_response_style,
                    pagination_page_size=pagination_page_size,
                    pagination_results_limit=pagination_results_limit,
                    pagination_next_page_param=pagination_next_page_param,
                    pagination_limit_per_page_param=pagination_limit_per_page_param,
                    pagination_total_limit_param=pagination_total_limit_param,
                    pagination_initial_offset=pagination_initial_offset,
                    offset_records_jsonpath=offset_records_jsonpath,
                    schema=schema,
                    start_date=start_date,
                    source_search_field=source_search_field,
                    source_search_query=source_search_query,
                    use_request_body_not_params=self.config.get("use_request_body_not_params"),
                    backoff_type=self.config.get("backoff_type"),
                    backoff_param=self.config.get("backoff_param"),
                    backoff_time_extension=self.config.get("backoff_time_extension"),
                    store_raw_json_message=self.config.get("store_raw_json_message"),
                    authenticator=self._authenticator,
                    iteration_config=iteration_config,
                )
            )

        return streams

    def _build_stream_schema_for_discovery(
        self,
        stream_config: dict,
        records_path: str,
        except_keys: List[str],
        path: str,
        params: Dict[str, Any],
        headers: Dict[str, Any],
        iteration_config: dict,
    ) -> dict:
        schema: dict = {}
        schema_config = stream_config.get("schema")

        if isinstance(schema_config, str):
            self.logger.info("Loading schema from file: %s", schema_config)
            with open(schema_config, "r") as f:
                loaded = json.load(f)
            builder = SchemaBuilder()
            builder.add_schema(loaded)
            schema = builder.to_schema()

        elif isinstance(schema_config, dict):
            self.logger.info("Using provided inline schema.")
            builder = SchemaBuilder()
            builder.add_schema(schema_config)
            schema = builder.to_schema()

        else:
            # Sample with first iteration value to hit required params
            path_eff, params_eff = self._apply_first_iteration_for_discovery(
                path=path, params=params, iteration_config=iteration_config
            )
            self.logger.info("Inferring schema from API (path=%s)", path_eff)
            schema = self.get_schema(
                records_path=records_path,
                except_keys=except_keys,
                inference_records=stream_config.get("num_inference_records", self.config.get("num_inference_records", 50)),
                path=path_eff,
                params=params_eff,
                headers=headers,
            )

        # Extend with iteration metadata
        iter_conf = iteration_config or {}
        if iter_conf and iter_conf.get("iteration_type") and iter_conf.get("iteration_type") != "none":
            schema.setdefault("properties", {})
            metadata_key = iter_conf.get("metadata_key", f"_source_{iter_conf.get('iteration_type','value')}")
            schema["properties"][metadata_key] = {"type": ["string", "null"]}
            schema["properties"]["_source_kind"] = {"type": ["string", "null"]}
            if iter_conf.get("add_extracted_at", True):
                schema["properties"]["_extracted_at"] = {"type": ["string","null"], "format": "date-time"}

        return schema

    def _apply_first_iteration_for_discovery(self, path: str, params: Dict[str, Any], iteration_config: dict):
        if not iteration_config or not iteration_config.get("values"):
            return path, dict(params)

        first_val = iteration_config["values"][0]
        tpl = iteration_config.get("api_param_template", "{value}")
        key = iteration_config.get("api_param_key")
        path_tpl = iteration_config.get("path_template")

        path_eff = path
        params_eff = dict(params)
        if path_tpl:
            try:
                path_eff = path_tpl.format(value=first_val)
            except Exception as ex:
                self.logger.warning("Failed to format path_template for discovery: %s", ex)
        if key:
            try:
                params_eff[key] = tpl.format(value=first_val)
            except Exception as ex:
                self.logger.warning("Failed to apply api_param_template for discovery: %s", ex)

        return path_eff, params_eff

    def get_schema(self, records_path: str, except_keys: list, inference_records: int, path: str, params: dict, headers: dict) -> Any:
        """Infer schema from a single API call."""
        auth_method = self.config.get("auth_method", "")
        self.http_auth = None

        if auth_method and auth_method != "no_auth":
            get_authenticator(self)
            if auth_method == "oauth" and isinstance(self._authenticator, ConfigurableOAuthAuthenticator):
                self._authenticator.get_initial_oauth_token()
            headers = {**headers, **getattr(self._authenticator, "auth_headers", {})}
            params = {**params, **getattr(self._authenticator, "auth_params", {})}

        r = requests.get(self.config["api_url"] + path, auth=self.http_auth, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()
        records = extract_jsonpath(records_path, input=data)

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())

        count = 0
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("Record must be a dict object.")
            flat_record = flatten_json(record, except_keys, store_raw_json_message=False)
            builder.add_object(flat_record)
            if self.config.get("store_raw_json_message"):
                builder.add_object({"_sdc_raw_json": {}})
            count += 1
            if count >= max(1, int(inference_records)):
                break

        self.logger.debug("Inferred schema from %d records.", count)
        return builder.to_schema()


if __name__ == "__main__":
    TapRestApiMsdk.cli()
