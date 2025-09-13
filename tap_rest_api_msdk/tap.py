"""rest-api tap class with iteration support (iteration-aware discovery)."""

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
    """rest-api tap class with iteration support."""

    name = "tap-rest-api-msdk"
    tap_name = name
    _authenticator: Optional[APIAuthenticatorBase] = None

    # ---------------------------
    # Common (stream) properties
    # ---------------------------
    common_properties = th.PropertiesList(
        th.Property(
            "path",
            th.StringType,
            required=False,
            description="Path appended to `api_url`. Stream-level path overwrites top-level path.",
        ),
        th.Property(
            "params",
            th.ObjectType(),
            default={},
            required=False,
            description="Query/body params for `requests.get`. Stream-level merges/overwrites top-level.",
        ),
        th.Property(
            "headers",
            th.ObjectType(),
            required=False,
            description="Headers to pass to the API call. Stream-level merges/overwrites top-level.",
        ),
        th.Property(
            "records_path",
            th.StringType,
            required=False,
            description="JSONPath to the records array/object in the response.",
        ),
        th.Property(
            "primary_keys",
            th.ArrayType(th.StringType),
            required=False,
            description="JSON keys of the primary key for the stream.",
        ),
        th.Property(
            "replication_key",
            th.StringType,
            required=False,
            description="JSON field name used as replication key.",
        ),
        th.Property(
            "except_keys",
            th.ArrayType(th.StringType),
            default=[],
            required=False,
            description="Keys which will not be recursively flattened.",
        ),
        th.Property(
            "num_inference_records",
            th.NumberType,
            default=50,
            required=False,
            description="How many records to sample for schema inference.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="Initial starting date for date-based replication.",
        ),
        th.Property(
            "source_search_field",
            th.StringType,
            required=False,
            description="Field name for querying specific records from the API.",
        ),
        th.Property(
            "source_search_query",
            th.StringType,
            required=False,
            description="Query template for API searches.",
        ),
        # Iteration configuration per stream
        th.Property(
            "iteration_config",
            th.ObjectType(
                th.Property(
                    "iteration_type",
                    th.StringType,
                    required=False,
                    allowed_values=["usernames", "keywords", "handles", "hashtags", "custom", "none"],
                    description="Type of iteration performed by the stream.",
                ),
                th.Property(
                    "values",
                    th.ArrayType(th.StringType),
                    required=False,
                    description="List of values to iterate over (e.g., handles/keywords).",
                ),
                th.Property(
                    "api_param_key",
                    th.StringType,
                    required=False,
                    description="API parameter key to set per iteration (e.g., 'query', 'userName').",
                ),
                th.Property(
                    "api_param_template",
                    th.StringType,
                    required=False,
                    default="{value}",
                    description="Template to format the param value, e.g., 'from:{value}'.",
                ),
                th.Property(
                    "path_template",
                    th.StringType,
                    required=False,
                    description="Optional path template per iteration value.",
                ),
                th.Property(
                    "metadata_key",
                    th.StringType,
                    required=False,
                    description="Record field to store the iteration value (e.g., '_source_handle').",
                ),
                th.Property(
                    "continue_on_error",
                    th.BooleanType,
                    required=False,
                    default=True,
                    description="Continue iterating if one value fails.",
                ),
                th.Property(
                    "add_extracted_at",
                    th.BooleanType,
                    required=False,
                    default=True,
                    description="Add extraction timestamp (_extracted_at) to emitted records.",
                ),
            ),
            required=False,
            description="Configuration for iterating over multiple values (usernames, keywords, etc.)",
        ),
        # Allow per-stream overrides for pagination/cursor discovery
        th.Property(
            "next_page_token_path",
            th.StringType,
            required=False,
            description="Stream-level JSONPath to next page token.",
        ),
        th.Property(
            "pagination_request_style",
            th.StringType,
            required=False,
            description="Stream-level override for pagination request style.",
        ),
        th.Property(
            "pagination_response_style",
            th.StringType,
            required=False,
            description="Stream-level override for pagination response style.",
        ),
        th.Property(
            "pagination_page_size",
            th.IntegerType,
            required=False,
            description="Stream-level override for page size.",
        ),
        th.Property(
            "pagination_results_limit",
            th.IntegerType,
            required=False,
            description="Stream-level override for total results cap.",
        ),
        th.Property(
            "pagination_next_page_param",
            th.StringType,
            required=False,
            description="Stream-level override for page/offset param name.",
        ),
        th.Property(
            "pagination_limit_per_page_param",
            th.StringType,
            required=False,
            description="Stream-level override for limit/per_page param name.",
        ),
        th.Property(
            "pagination_total_limit_param",
            th.StringType,
            required=False,
            description="Stream-level override for total limit param name.",
        ),
        th.Property(
            "pagination_initial_offset",
            th.IntegerType,
            required=False,
            description="Stream-level override for initial offset.",
        ),
        th.Property(
            "offset_records_jsonpath",
            th.StringType,
            required=False,
            description="Stream-level records JSONPath for offset paginator.",
        ),
    )

    # ---------------------------
    # Top-level (tap) properties
    # ---------------------------
    top_level_properties = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="Base URL for the API.",
        ),
        th.Property(
            "auth_method",
            th.StringType,
            default="no_auth",
            required=False,
            description="Authentication method (oauth, basic, api_key, bearer_token, aws, no_auth).",
        ),
        th.Property(
            "api_keys",
            th.ObjectType(),
            required=False,
            description="API Key/Value pairs for api_key auth method.",
        ),
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
        th.Property(
            "oauth_expiration_secs",
            th.IntegerType,
            default=None,
            required=False,
        ),
        th.Property("aws_credentials", th.ObjectType(), default=None, required=False),
        th.Property("next_page_token_path", th.StringType, default=None, required=False),
        th.Property("pagination_request_style", th.StringType, default="default", required=False),
        th.Property("pagination_response_style", th.StringType, default="default", required=False),
        th.Property("use_request_body_not_params", th.BooleanType, default=False, required=False),
        th.Property(
            "backoff_type",
            th.StringType,
            default=None,
            required=False,
            allowed_values=[None, "message", "header"],
        ),
        th.Property("backoff_param", th.StringType, default="Retry-After", required=False),
        th.Property("backoff_time_extension", th.IntegerType, default=0, required=False),
        th.Property("store_raw_json_message", th.BooleanType, default=False, required=False),
        th.Property("pagination_page_size", th.IntegerType, default=None, required=False),
        th.Property("pagination_results_limit", th.IntegerType, default=None, required=False),
        th.Property("pagination_next_page_param", th.StringType, default=None, required=False),
        th.Property("pagination_limit_per_page_param", th.StringType, default=None, required=False),
        th.Property("pagination_total_limit_param", th.StringType, default="total", required=False),
        th.Property("pagination_initial_offset", th.IntegerType, default=1, required=False),
        th.Property("offset_records_jsonpath", th.StringType, default=None, required=False),
        th.Property(
            "global_iteration_config",
            th.ObjectType(),
            required=False,
            description="Global iteration configuration applied when a stream has none.",
        ),
    )

    # Add the common stream props to top-level so they can be inherited
    for prop in common_properties.wrapped.values():
        top_level_properties.append(prop)

    # Per-stream JSON schema container
    stream_properties = th.PropertiesList()
    stream_properties.wrapped = copy.copy(common_properties.wrapped)
    stream_properties.append(th.Property("name", th.StringType, required=True))
    stream_properties.append(
        th.Property(
            "schema",
            th.CustomType({"anyOf": [{"type": "string"}, {"type": "null"}, {"type": "object"}]}),
            required=False,
            description="Inline JSON schema or path to a schema file.",
        ),
    )

    # Add "streams" array to the tap config
    top_level_properties.append(
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*stream_properties.wrapped.values())),
            required=False,
            description="Array of stream configurations.",
        ),
    )

    config_jsonschema = top_level_properties.to_dict()

    # ---------------------------
    # Discovery
    # ---------------------------
    def discover_streams(self) -> List[IterativeDynamicStream]:
        """Return a list of discovered streams with iteration support."""
        streams: List[IterativeDynamicStream] = []

        for stream_config in self.config.get("streams", []):
            # Merge effective config for this stream
            records_path = stream_config.get("records_path", self.config.get("records_path", "$[*]"))
            except_keys = stream_config.get("except_keys", self.config.get("except_keys", []))
            path = stream_config.get("path", self.config.get("path", ""))
            params = {**self.config.get("params", {}), **stream_config.get("params", {})}
            headers = {**self.config.get("headers", {}), **stream_config.get("headers", {})}
            start_date = stream_config.get("start_date", self.config.get("start_date", ""))
            replication_key = stream_config.get("replication_key", self.config.get("replication_key", ""))

            source_search_field = stream_config.get(
                "source_search_field", self.config.get("source_search_field", "")
            )
            source_search_query = stream_config.get(
                "source_search_query", self.config.get("source_search_query", "")
            )
            offset_records_jsonpath = stream_config.get(
                "offset_records_jsonpath", self.config.get("offset_records_jsonpath", None)
            )

            # Pagination & cursor (allow per-stream overrides)
            next_page_token_path = stream_config.get(
                "next_page_token_path", self.config.get("next_page_token_path", None)
            )
            pagination_request_style = stream_config.get(
                "pagination_request_style", self.config.get("pagination_request_style", "default")
            )
            pagination_response_style = stream_config.get(
                "pagination_response_style", self.config.get("pagination_response_style", "default")
            )
            pagination_page_size = stream_config.get(
                "pagination_page_size", self.config.get("pagination_page_size", None)
            )
            pagination_results_limit = stream_config.get(
                "pagination_results_limit", self.config.get("pagination_results_limit", None)
            )
            pagination_next_page_param = stream_config.get(
                "pagination_next_page_param", self.config.get("pagination_next_page_param", None)
            )
            pagination_limit_per_page_param = stream_config.get(
                "pagination_limit_per_page_param",
                self.config.get("pagination_limit_per_page_param", None),
            )
            pagination_total_limit_param = stream_config.get(
                "pagination_total_limit_param",
                self.config.get("pagination_total_limit_param", "total"),
            )
            pagination_initial_offset = stream_config.get(
                "pagination_initial_offset", self.config.get("pagination_initial_offset", 1)
            )

            # Iteration config (stream-level overrides global)
            iteration_config = stream_config.get("iteration_config", self.config.get("global_iteration_config", {}))

            # Build (or infer) schema - IMPORTANT: make discovery iteration-aware
            schema = self._build_stream_schema_for_discovery(
                stream_config=stream_config,
                records_path=records_path,
                except_keys=except_keys,
                path=path,
                params=params,
                headers=headers,
                iteration_config=iteration_config,
            )

            # Create the stream instance
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

    # ---------------------------
    # Schema building (discovery)
    # ---------------------------
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
        """Get or infer the stream schema, applying the first iteration value (if any) for discovery."""
        schema: dict = {}
        schema_config = stream_config.get("schema")

        # If inline or file schema is provided, prefer it.
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
            # Infer the schema from a real API call.
            # Patch in the FIRST iteration value so endpoints that require query/userName succeed.
            path_eff, params_eff = self._apply_first_iteration_for_discovery(
                path=path,
                params=params,
                iteration_config=iteration_config,
            )
            self.logger.info("Inferring schema from API (path=%s)", path_eff)
            schema = self.get_schema(
                records_path=records_path,
                except_keys=except_keys,
                inference_records=stream_config.get(
                    "num_inference_records", self.config.get("num_inference_records", 50)
                ),
                path=path_eff,
                params=params_eff,
                headers=headers,
            )

        # If iteration is configured, extend schema with iteration metadata fields.
        iter_conf = iteration_config or {}
        if iter_conf and iter_conf.get("iteration_type") and iter_conf.get("iteration_type") != "none":
            if "properties" not in schema:
                schema["properties"] = {}
            metadata_key = iter_conf.get(
                "metadata_key", f"_source_{iter_conf.get('iteration_type', 'value')}"
            )
            schema["properties"][metadata_key] = {"type": ["string", "null"]}
            schema["properties"]["_source_kind"] = {"type": ["string", "null"]}
            if iter_conf.get("add_extracted_at", True):
                schema["properties"]["_extracted_at"] = {"type": ["string", "null"], "format": "date-time"}

        return schema

    def _apply_first_iteration_for_discovery(
        self,
        path: str,
        params: Dict[str, Any],
        iteration_config: dict,
    ) -> (str, Dict[str, Any]):
        """Return effective (path, params) for discovery by applying the FIRST iteration value, if present."""
        if not iteration_config or not iteration_config.get("values"):
            # No iteration configured, return as-is.
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

    # ---------------------------
    # Schema inference helper
    # ---------------------------
    def get_schema(
        self,
        records_path: str,
        except_keys: list,
        inference_records: int,
        path: str,
        params: dict,
        headers: dict,
    ) -> Any:
        """Infer schema from API response using the effective path/params."""
        auth_method = self.config.get("auth_method", "")
        self.http_auth = None

        # Prepare auth
        if auth_method and auth_method != "no_auth":
            get_authenticator(self)
            if auth_method == "oauth" and isinstance(self._authenticator, ConfigurableOAuthAuthenticator):
                self._authenticator.get_initial_oauth_token()

            headers = {**headers, **getattr(self._authenticator, "auth_headers", {})}
            params = {**params, **getattr(self._authenticator, "auth_params", {})}

        # Call API once to sample records
        r = requests.get(
            self.config["api_url"] + path,
            auth=self.http_auth,
            params=params,
            headers=headers,
        )

        if r.ok:
            try:
                data = r.json()
            except Exception:
                self.logger.error("Failed to parse JSON from discovery request.")
                raise
            records = extract_jsonpath(records_path, input=data)
        else:
            self.logger.error("Error connecting during discovery: %s", r.text)
            raise ValueError(r.text)

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())

        count = 0
        for record in records:
            if not isinstance(record, dict):
                self.logger.error("Record must be a dict object for schema inference.")
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


# CLI entrypoint
if __name__ == "__main__":
    TapRestApiMsdk.cli()
