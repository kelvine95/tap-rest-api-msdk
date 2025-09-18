# tap_rest_api_msdk/tap.py

"""rest-api tap class."""

import copy
import json
from datetime import datetime, timezone
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


def _iso_utc(dt_obj: datetime) -> str:
    """Format datetime as strict ISO-8601 in UTC with 'Z' suffix.

    Args:
        dt_obj: Datetime object, naive treated as UTC.

    Returns:
        ISO-8601 formatted string (YYYY-MM-DDTHH:MM:SSZ).
    """
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch_seconds(dt_obj: datetime) -> int:
    """Convert a datetime (naive treated as UTC) to epoch seconds.

    Args:
        dt_obj: Datetime object.

    Returns:
        Integer epoch seconds.
    """
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return int(dt_obj.timestamp())


class TapRestApiMsdk(Tap):
    """rest-api tap class."""

    name = "tap-rest-api-msdk"

    # Required for Authentication in tap.py - function APIAuthenticatorBase
    tap_name = name

    # Used to cache the Authenticator to prevent over hitting the Authentication
    # end-point for each stream.
    _authenticator: Optional[APIAuthenticatorBase] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.total_records_processed = 0
        self.max_ingestion_limit = self.config.get("max_ingestion_limit")
        self.reached_max_limit = False

    # ----------------------
    # Common (stream) schema
    # ----------------------
    common_properties = th.PropertiesList(
        th.Property(
            "path",
            th.StringType,
            required=False,
            description="the path appended to the `api_url`. Stream-level path will "
            "overwrite top-level path",
        ),
        th.Property(
            "params",
            th.ObjectType(),
            default={},
            required=False,
            description="an object providing the `params` in a `requests.get` method. "
            "Stream level params will be merged"
            "with top-level params with stream level params overwriting"
            "top-level params with the same key.",
        ),
        th.Property(
            "headers",
            th.ObjectType(),
            required=False,
            description="An object of headers to pass into the api calls. Stream level"
            "headers will be merged with top-level params with stream"
            "level params overwriting top-level params with the same key.",
        ),
        th.Property(
            "records_path",
            th.StringType,
            required=False,
            description="a jsonpath string representing the path in the requests "
            "response that contains the records to process. Defaults "
            "to `$[*]`. Stream level records_path will overwrite "
            "the top-level records_path",
        ),
        th.Property(
            "primary_keys",
            th.ArrayType(th.StringType),
            required=False,
            description="a list of the json keys of the primary key for the stream.",
        ),
        th.Property(
            "replication_key",
            th.StringType,
            required=False,
            description="the json response field representing the replication key."
            "Note that this should be an incrementing integer or datetime object.",
        ),
        th.Property(
            "except_keys",
            th.ArrayType(th.StringType),
            default=[],
            required=False,
            description="This tap automatically flattens the entire json structure "
            "and builds keys based on the corresponding paths.; Keys, "
            "whether composite or otherwise, listed in this dictionary "
            "will not be recursively flattened, but instead their values "
            "will be; turned into a json string and processed in that "
            "format. This is also automatically done for any lists within "
            "the records; therefore, records are not duplicated for each "
            "item in lists.",
        ),
        th.Property(
            "num_inference_records",
            th.NumberType,
            default=50,
            required=False,
            description="number of records used to infer the stream's schema. "
            "Defaults to 50.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="An optional field. Normally required when using the"
            "replication_key. This is the initial starting date when using a"
            "date based replication key and there is no state available.",
        ),
        th.Property(
            "source_search_field",
            th.StringType,
            required=False,
            description="An optional field name which can be used for querying "
            "specific records from supported API's. The intend for this "
            "parameter is to continue incrementally processing from a "
            "previous state. Example `last-updated`. Note: You must also "
            "set the replication_key, where the replication_key isjson "
            "response representation of the API `source_search_field`. "
            "You shouldalso supply the `source_search_query`, "
            "`replication_key` and `start_date`.",
        ),
        th.Property(
            "source_search_query",
            th.StringType,
            required=False,
            description="An optional query template to be issued against the API."
            "Substitute the query field you are querying against with "
            "$last_run_date. Atrun-time, the tap will dynamically update "
            "the token with either the `start_date`or the last bookmark / "
            "state value. A simple template Example for FHIR API's: "
            "gt$last_run_date. A more complex example against an "
            "Opensearch API, "
            '{"bool": {"filter": [{"range": '
            '{ "meta.lastUpdated": { "gt": "$last_run_date" }}}] }} .'
            "Note: Any required double quotes in the query template must "
            "be escaped.",
        ),
    )

    # -------------------
    # Top-level schema
    # -------------------
    top_level_properties = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="the base url/endpoint for the desired api",
        ),
        th.Property(
            "auth_method",
            th.StringType,
            default="no_auth",
            required=False,
            description="The method of authentication used by the API. Supported "
            "options include oauth: for OAuth2 authentication, basic: "
            "Basic Header authorization - base64-encoded username + "
            "password config items, api_key: for API Keys in the header "
            "e.g. X-API-KEY,bearer_token: for Bearer token authorization, "
            "aws: for AWS Authentication.Defaults to no_auth which will "
            "take authentication parameters passed via the headersconfig.",
        ),
        th.Property(
            "api_keys",
            th.ObjectType(),
            required=False,
            description="A object of API Key/Value pairs used by the api_key auth "
            "method Example: { X-API-KEY: my secret value}.",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The public "
            "application ID that's assigned for Authentication. The "
            "client_id should accompany a client_secret.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The client_secret "
            "is a secret known only to the application and the "
            "authorization server. It is essential the application's "
            "own password.",
        ),
        th.Property(
            "username",
            th.StringType,
            required=False,
            description="Used for a number of authentication methods that use a user "
            "password combination for authentication.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="Used for a number of authentication methods that use a user "
            "password combination for authentication.",
        ),
        th.Property(
            "bearer_token",
            th.StringType,
            required=False,
            description="Used for the Bearer Authentication method, which uses a token "
            "as part of the authorization header for authentication.",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=False,
            description="An OAuth2 Refresh Token is a string that the OAuth2 "
            "client can use to get a new access token without the user's "
            "interaction.",
        ),
        th.Property(
            "grant_type",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The grant_type "
            "is required to describe the OAuth2 flow. Flows support by "
            "this tap include client_credentials, refresh_token, password.",
        ),
        th.Property(
            "scope",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. The scope is "
            "optional, it is a mechanism to limit the amount of access "
            "that is granted to an access token. One or more scopes can "
            "be provided delimited by a space.",
        ),
        th.Property(
            "access_token_url",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. This is the "
            "end-point for the authentication server used to exchange "
            "the authorization codes for a access token.",
        ),
        th.Property(
            "redirect_uri",
            th.StringType,
            required=False,
            description="Used for the OAuth2 authentication method. This is optional "
            "as the redirect_uri may be part of the token returned by "
            "the authentication server. If a redirect_uri is provided, "
            "it determines where the API server redirects the user after "
            "the user completes the authorization flow.",
        ),
        th.Property(
            "oauth_extras",
            th.ObjectType(),
            required=False,
            description="A object of Key/Value pairs for additional oauth config "
            "parameters which may be required by the authorization server."
            "Example: "
            "{resource: https://analysis.windows.net/powerbi/api}.",
        ),
        th.Property(
            "oauth_expiration_secs",
            th.IntegerType,
            default=None,
            required=False,
            description="Used for OAuth2 authentication method. This optional "
            "setting is a timer for the expiration of a token in "
            "seconds. If not set the OAuth will use the default "
            "expiration set in the token by the authorization server.",
        ),
        th.Property(
            "aws_credentials",
            th.ObjectType(),
            default=None,
            required=False,
            description="An object of aws credentials to authenticate to access AWS "
            "services. This example is to access the AWS OpenSearch "
            "service. Example: { aws_access_key_id: my_aws_key_id, "
            "aws_secret_access_key: my_aws_secret_access_key, "
            "aws_region: us-east-1, "
            "aws_service: es, use_signed_credentials: true} ",
        ),
        th.Property(
            "next_page_token_path",
            th.StringType,
            default=None,
            required=False,
            description="a jsonpath string representing the path to the 'next page' "
            "token. Defaults to `$.next_page`",
        ),
        th.Property(
            "pagination_request_style",
            th.StringType,
            default="default",
            required=False,
            description="the pagination style to use for requests. "
            "Defaults to `default`",
        ),
        th.Property(
            "pagination_response_style",
            th.StringType,
            default="default",
            required=False,
            description="the pagination style to use for response. "
            "Defaults to `default`",
        ),
        th.Property(
            "use_request_body_not_params",
            th.BooleanType,
            default=False,
            required=False,
            description="sends the request parameters in the request body."
            "This is normally not required, a few API's like OpenSearch"
            "require this. Defaults to `False`",
        ),
        th.Property(
            "backoff_type",
            th.StringType,
            default=None,
            required=False,
            allowed_values=[None, "message", "header"],
            description="The style of Backoff applied to rate limited APIs."
            "None: Default Meltano SDK backoff_wait_generator, message: Scans "
            "the response message for a time interval, header: retrieves the "
            "backoff value from a header key response."
            " Defaults to `None`",
        ),
        th.Property(
            "backoff_param",
            th.StringType,
            default="Retry-After",
            required=False,
            description="The name of the key which contains a the "
            "backoff value in the response. This is very applicable to backoff"
            " values in headers. Defaults to `Retry-After`",
        ),
        th.Property(
            "backoff_time_extension",
            th.IntegerType,
            default=0,
            required=False,
            description="A time extension (in seconds) to add to the backoff "
            "value from the API plus jitter. Some APIs are not precise"
            ", this adds an additional wait delay. Defaults to `0`",
        ),
        th.Property(
            "store_raw_json_message",
            th.BooleanType,
            default=False,
            required=False,
            description="Adds an additional _SDC_RAW_JSON column as an "
            "object. This will store the raw incoming message in this "
            "column when provisioned. Useful for semi-structured records "
            "when the schema is not well defined. Defaults to `False`",
        ),
        th.Property(
            "pagination_page_size",
            th.IntegerType,
            default=None,
            required=False,
            description="the size of each page in records. Defaults to None",
        ),
        th.Property(
            "pagination_results_limit",
            th.IntegerType,
            default=None,
            required=False,
            description="limits the max number of records. Defaults to None",
        ),
        th.Property(
            "pagination_next_page_param",
            th.StringType,
            default=None,
            required=False,
            description="The name of the param that indicates the page/offset/cursor. "
            "Defaults to None",
        ),
        th.Property(
            "pagination_limit_per_page_param",
            th.StringType,
            default=None,
            required=False,
            description="The name of the param that indicates the limit/per_page. "
            "Defaults to None",
        ),
        th.Property(
            "pagination_total_limit_param",
            th.StringType,
            default="total",
            required=False,
            description="The name of the param that indicates the total limit e.g. "
            "total, count. Defaults to total",
        ),
        th.Property(
            "pagination_initial_offset",
            th.IntegerType,
            default=1,
            required=False,
            description="The initial offset to start pagination from. Defaults to 1",
        ),
        th.Property(
            "offset_records_jsonpath",
            th.StringType,
            default=None,
            required=False,
            description="Optional jsonpath string representing the path in the results "
            "Defaults to `None`.",
        ),
    )

    # add common properties to top-level properties
    for prop in common_properties.wrapped.values():
        top_level_properties.append(prop)

    # --------------
    # Stream schema
    # --------------
    stream_properties = th.PropertiesList()
    stream_properties.wrapped = copy.copy(common_properties.wrapped)
    stream_properties.append(
        th.Property(
            "name", th.StringType, required=True, description="name of the stream"
        ),
    )
    stream_properties.append(
        th.Property(
            "schema",
            th.CustomType(
                {"anyOf": [{"type": "string"}, {"type": "null"}, {"type:": "object"}]}
            ),
            required=False,
            description="A valid Singer schema or a path-like string that provides "
            "the path to a `.json` file that contains a valid Singer "
            "schema. If provided, the schema will not be inferred from "
            "the results of an api call.",
        ),
    )

    # Allow per-stream pagination overrides & cursor handling.
    stream_properties.append(
        th.Property(
            "next_page_token_path",
            th.StringType,
            required=False,
            description="JSONPath for next-page token (e.g., '$.next_cursor').",
        )
    )
    stream_properties.append(
        th.Property(
            "pagination_next_page_param",
            th.StringType,
            required=False,
            description="Name of request param that carries the next-page token (e.g., 'cursor').",
        )
    )
    stream_properties.append(
        th.Property(
            "pagination_limit_per_page_param",
            th.StringType,
            required=False,
            description="Name of request param for page size (e.g., 'limit', 'per_page').",
        )
    )
    stream_properties.append(
        th.Property(
            "pagination_page_size",
            th.IntegerType,
            required=False,
            description="Page size for this stream only (overrides top-level).",
        )
    )
    stream_properties.append(
        th.Property(
            "pagination_results_limit",
            th.IntegerType,
            required=False,
            description="Max total records for this stream only (overrides top-level).",
        )
    )

    # iteration_config support (usernames/hashtags/keywords or registry-driven).
    stream_properties.append(
        th.Property(
            "iteration_config",
            th.ObjectType(
                th.Property(
                    "iteration_type",
                    th.StringType,
                    required=False,
                    description="Semantics only (usernames, hashtags, keywords, tweet_ids, registry).",
                ),
                th.Property(
                    "values",
                    th.ArrayType(th.StringType),
                    required=False,
                    description="Explicit set of values to iterate (e.g., handles or hashtags).",
                ),
                th.Property(
                    "api_param_key",
                    th.StringType,
                    required=False,
                    description="Request parameter name to set for each value (e.g., 'query', 'userName', 'tweetId').",
                ),
                th.Property(
                    "api_param_template",
                    th.StringType,
                    required=False,
                    description="String template to render the value (default '{value}').",
                ),
                th.Property(
                    "metadata_key",
                    th.StringType,
                    required=False,
                    description="Record key to inject for provenance (e.g., '_source_handle').",
                ),
                # Optional registry-driven fan-out (tweet IDs from earlier streams)
                th.Property(
                    "from_registry",
                    th.StringType,
                    required=False,
                    description="State registry key to read values from (e.g., 'tweet_ids:twitter_timeline').",
                ),
                th.Property(
                    "max_values",
                    th.IntegerType,
                    required=False,
                    description="If from_registry is used, cap how many values to iterate.",
                ),
            ),
            required=False,
            description="Expand one logical stream into per-value concrete streams without custom code.",
        )
    )

    # Minimal replication request adapter (Twitter-friendly).
    stream_properties.append(
        th.Property(
            "replication_request_adapter",
            th.ObjectType(
                th.Property(
                    "mode",
                    th.StringType,
                    required=True,
                    description="add_query_suffix | param",
                ),
                th.Property(
                    "key",
                    th.StringType,
                    required=True,
                    description="Request param to mutate (e.g., 'query', 'sinceTime').",
                ),
                th.Property(
                    "template",
                    th.StringType,
                    required=False,
                    description="For add_query_suffix: e.g., ' since:${iso_utc}'.",
                ),
                th.Property(
                    "transform",
                    th.StringType,
                    required=False,
                    description="For mode=param: e.g., 'epoch_seconds'.",
                ),
            ),
            required=False,
            description="Lightweight adapter to append 'since:' suffixes or set 'sinceTime' epoch values.",
        )
    )

    # Optional tweet-id registry capture on producing streams.
    stream_properties.append(
        th.Property(
            "id_registry_config",
            th.ObjectType(
                th.Property(
                    "registry_key",
                    th.StringType,
                    required=True,
                    description="State registry key to store IDs under (e.g., 'tweet_ids:twitter_timeline').",
                ),
                th.Property(
                    "id_path",
                    th.StringType,
                    required=False,
                    description="JSONPath to extract the ID from the raw row (default: '$.id').",
                ),
                th.Property(
                    "max_to_register_per_run",
                    th.IntegerType,
                    required=False,
                    description="Max number of IDs to register in a single run for this stream.",
                ),
                th.Property(
                    "min_like_count",
                    th.IntegerType,
                    required=False,
                    description="Filter: only register IDs with likeCount >= this value.",
                ),
                th.Property(
                    "min_view_count",
                    th.IntegerType,
                    required=False,
                    description="Filter: only register IDs with viewCount >= this value.",
                ),
            ),
            required=False,
            description="Enable storing record IDs (e.g., tweet IDs) into Singer state "
            "so downstream streams can iterate them without hardcoding.",
        )
    )

    # add streams schema to top-level properties
    top_level_properties.append(
        th.Property(
            "streams",
            th.ArrayType(th.ObjectType(*stream_properties.wrapped.values())),
            required=False,
            description="An array of streams, designed for separate paths using the"
            "same base url.",
        ),
    )

    config_jsonschema = top_level_properties.to_dict()

    # ----------------------
    # Discovery and helpers
    # ----------------------
    def discover_streams(self) -> List[DynamicStream]:  # type: ignore
        """Return a list of discovered streams."""
        streams: List[DynamicStream] = []

        def _make_stream(stream_cfg: dict, inject_meta: Optional[dict] = None) -> DynamicStream:
            """Create a DynamicStream with fully-resolved per-stream settings."""
            records_path = stream_cfg.get("records_path", self.config.get("records_path", "$[*]"))
            except_keys = stream_cfg.get("except_keys", self.config.get("except_keys", []))
            path = stream_cfg.get("path", self.config.get("path", ""))
            params = {**self.config.get("params", {}), **stream_cfg.get("params", {})}
            headers = {**self.config.get("headers", {}), **stream_cfg.get("headers", {})}
            replication_key = stream_cfg.get("replication_key", self.config.get("replication_key"))

            schema: Dict[str, Any] = {}
            schema_config = stream_cfg.get("schema")
            if isinstance(schema_config, str):
                self.logger.info(f"Stream '{stream_cfg['name']}': Found path to a schema, not doing discovery.")
                with open(schema_config, "r") as f:
                    schema = json.load(f)
            elif isinstance(schema_config, dict):
                self.logger.info(f"Stream '{stream_cfg['name']}': Found schema in config, not doing discovery.")
                builder = SchemaBuilder()
                builder.add_schema(schema_config)
                schema = builder.to_schema()
            else:
                self.logger.info(f"Stream '{stream_cfg['name']}': No schema found. Inferring schema from API call.")
                schema = self.get_schema(
                    records_path,
                    except_keys,
                    stream_cfg.get("num_inference_records", self.config.get("num_inference_records", 50)),
                    path,
                    params,
                    headers,
                )
            
            next_page_token_path = stream_cfg.get("next_page_token_path", self.config.get("next_page_token_path"))
            pagination_next_page_param = stream_cfg.get("pagination_next_page_param", self.config.get("pagination_next_page_param"))

            return DynamicStream(
                tap=self,
                name=stream_cfg["name"],
                path=path,
                params=params,
                headers=headers,
                records_path=records_path,
                primary_keys=stream_cfg.get("primary_keys", self.config.get("primary_keys", [])),
                replication_key=replication_key,
                except_keys=except_keys,
                schema=schema,
                next_page_token_path=next_page_token_path,
                pagination_request_style=self.config.get("pagination_request_style", "default"),
                pagination_response_style=self.config.get("pagination_response_style", "default"),
                pagination_next_page_param=pagination_next_page_param,
                max_records_limit=stream_cfg.get("max_records_limit"),
                id_registry_config=stream_cfg.get("id_registry_config"),
                inject_metadata=inject_meta or {},
                authenticator=self._authenticator,
                config=stream_cfg
            )

        for base_stream in self.config.get("streams", []):
            if self.reached_max_limit:
                self.logger.info("Global record limit reached, skipping remaining streams.")
                break

            iter_cfg = base_stream.get("iteration_config")
            if not iter_cfg:
                streams.append(_make_stream(base_stream))
                continue

            values: List[Any] = []
            inject_key = iter_cfg.get("metadata_key")
            api_param_key = iter_cfg.get("api_param_key")
            api_param_template = iter_cfg.get("api_param_template", "{value}")

            if "values" in iter_cfg and iter_cfg["values"]:
                values = list(iter_cfg["values"])
            elif "from_registry" in iter_cfg:
                reg_key = str(iter_cfg["from_registry"])
                max_values = int(iter_cfg.get("max_values", 0)) or None
                reg = self.state.get("registry", {})
                seq = reg.get(reg_key, []) if isinstance(reg, dict) else []
                cleaned = [x for x in seq if isinstance(x, (str, int))]
                values = cleaned[:max_values] if max_values else cleaned

            if not values or not api_param_key:
                streams.append(_make_stream(base_stream))
                continue

            for val in values:
                s = copy.deepcopy(base_stream)
                safe_val = str(val).replace("#", "hash_").replace(" ", "_").replace(":", "_")
                s["name"] = f"{base_stream['name']}__{safe_val}"
                s.setdefault("params", {})
                s["params"][api_param_key] = api_param_template.replace("{value}", str(val))
                inject_meta = {inject_key: val} if inject_key else {}
                streams.append(_make_stream(s, inject_meta=inject_meta))

        return streams

    # --------------------------
    # Schema inference / helper
    # --------------------------
    def get_schema(
        self,
        records_path: str,
        except_keys: list,
        inference_records: int,
        path: str,
        params: dict,
        headers: dict,
    ) -> Any:
        """Infer schema from the first records returned by api. Creates a Stream object.

        If auth_method is set, will call get_authenticator to obtain credentials
        to issue a request to sample some records. The get_authenticator will:
        - stores the authenticator in self._authenticator
        - sets the self.http_auth if required by a given authenticator
        - use an existing authenticator if one exists and is cached.

        Args:
            records_path: required - see config_jsonschema.
            except_keys: required - see config_jsonschema.
            inference_records: required - see config_jsonschema.
            path: required - see config_jsonschema.
            params: required - see config_jsonschema.
            headers: required - see config_jsonschema.

        Raises:
            ValueError: if the response is not valid or a record is not valid json.

        Returns:
            A schema for the stream.

        """
        # TODO: this request format is not very robust

        # Initialise Variables
        auth_method = self.config.get("auth_method", "")
        self.http_auth = None

        if auth_method and not auth_method == "no_auth":
            # Obtaining Authenticator for authorisation to obtain a schema.
            get_authenticator(self)

            # Get an initial oauth token if an oauth method
            if auth_method == "oauth" and isinstance(
                self._authenticator, ConfigurableOAuthAuthenticator
            ):
                self._authenticator.get_initial_oauth_token()

            headers.update(getattr(self._authenticator, "auth_headers", {}))
            params.update(getattr(self._authenticator, "auth_params", {}))

        r = requests.get(
            self.config["api_url"] + path,
            auth=self.http_auth,
            params=params,
            headers=headers,
        )
        if r.ok:
            records = extract_jsonpath(records_path, input=r.json())
        else:
            self.logger.error(f"Error Connecting, message = {r.text}")
            raise ValueError(r.text)

        builder = SchemaBuilder()
        builder.add_schema(th.PropertiesList().to_dict())
        for i, record in enumerate(records):
            if type(record) is not dict:
                self.logger.error("Input must be a dict object.")
                raise ValueError("Input must be a dict object.")

            flat_record = flatten_json(
                record, except_keys, store_raw_json_message=False
            )

            builder.add_object(flat_record)
            # Optional add _sdc_raw_json field to store the raw message
            if self.config.get("store_raw_json_message"):
                builder.add_object({"_sdc_raw_json": {}})

            if i >= inference_records:
                break

        self.logger.debug(f"{builder.to_json(indent=2)}")
        return builder.to_schema()
