"""Stream type classes for tap-rest-api-msdk."""

import email.utils
import json
from datetime import datetime
from string import Template
from typing import Any, Dict, Generator, Iterable, Optional, Union
from urllib.parse import parse_qs, parse_qsl, urlparse

import requests
from singer_sdk.helpers import types
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import (
    BaseHATEOASPaginator,
    HeaderLinkPaginator,
    JSONPathPaginator,
    SimpleHeaderPaginator,
    SinglePagePaginator,
)
from tap_rest_api_msdk.client import RestApiStream
from tap_rest_api_msdk.pagination import (
    RestAPIBasePageNumberPaginator,
    RestAPIHeaderLinkPaginator,
    RestAPIOffsetPaginator,
    SimpleOffsetPaginator,
)
from tap_rest_api_msdk.utils import flatten_json, get_start_date

# Remove commented section to show http_request for debugging
# import logging
# import http.client
# http.client.HTTPConnection.debuglevel = 1
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True


class DynamicStream(RestApiStream):
    """Define custom stream.

    Backward compatible, with optional enhancements:
    - `inject_metadata`: inject constant key/values into every flattened record
      (e.g., {"_source_handle": "coinlist"}).
    - `id_registry_config`: optionally register tweet IDs into Singer state so that
      downstream streams (e.g., replies/quotes) can iterate tweet IDs without
      hardcoding them in YAML. See `id_registry_config` docstring below.
    - `max_records_limit`: Per-stream limit for total records to fetch
    """

    def __init__(
        self,
        tap: Any,
        name: str,
        records_path: str,
        path: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        primary_keys: Optional[list] = None,
        replication_key: Optional[str] = None,
        except_keys: Optional[list] = None,
        next_page_token_path: Optional[str] = None,
        schema: Optional[dict] = None,
        pagination_request_style: str = "default",
        pagination_response_style: str = "default",
        pagination_page_size: Optional[int] = None,
        pagination_results_limit: Optional[int] = None,
        pagination_next_page_param: Optional[str] = None,
        pagination_limit_per_page_param: Optional[str] = None,
        pagination_total_limit_param: Optional[str] = None,
        pagination_initial_offset: int = 1,
        offset_records_jsonpath: Optional[str] = None,
        start_date: Optional[datetime] = None,
        source_search_field: Optional[str] = None,
        source_search_query: Optional[str] = None,
        use_request_body_not_params: Optional[bool] = False,
        backoff_type: Optional[str] = None,
        backoff_param: Optional[str] = "Retry-After",
        backoff_time_extension: Optional[int] = 0,
        store_raw_json_message: Optional[bool] = False,
        authenticator: Optional[object] = None,
        inject_metadata: Optional[dict] = None,
        id_registry_config: Optional[dict] = None,
        max_records_limit: Optional[int] = None,  # Per-stream total record limit
    ) -> None:
        """Class initialization.

        Args:
            tap: see tap.py
            name: see tap.py
            path: see tap.py
            params: see tap.py
            headers: see tap.py
            primary_keys: see tap.py
            replication_key: see tap.py
            except_keys: see tap.py
            records_path: see tap.py
            next_page_token_path: see tap.py
            schema: the json schema for the stream.
            pagination_request_style: see tap.py
            pagination_response_style: see tap.py
            pagination_page_size: see tap.py
            pagination_results_limit: see tap.py
            pagination_next_page_param: see tap.py
            pagination_limit_per_page_param: see tap.py
            pagination_total_limit_param: see tap.py
            pagination_initial_offset: see tap.py
            start_date: see tap.py
            source_search_field: see tap.py
            source_search_query: see tap.py
            use_request_body_not_params: see tap.py
            backoff_type: see tap.py
            backoff_param: see tap.py
            backoff_time_extension: see tap.py
            store_raw_json_message: see tap.py
            authenticator: see tap.py
            inject_metadata: Optional constant metadata to inject into every record
                (e.g., {"_source_handle": "coinlist"}). Backward compatible.
            id_registry_config: Optional dict enabling tweet-ID registration for
                downstream fan-out (e.g., replies/quotes). Example:
                {
                  "registry_key": "tweet_ids:twitter_timeline",
                  "id_path": "$.id",                    # JSONPath to the tweet id
                  "max_to_register_per_run": 50,        # cap per stream per run
                  "min_like_count": 0,                  # filter by flattened fields
                  "min_view_count": 0                   #   (uses 'likeCount', 'viewCount')
                }
                If omitted or invalid, no registry capture occurs.
            max_records_limit: Optional per-stream total record limit
        """
        super().__init__(tap=tap, name=tap.name, schema=schema)

        if primary_keys is None:
            primary_keys = []

        self.name = name
        self.path = path
        self.params = params if params else {}
        self.headers = headers
        self.assigned_authenticator = authenticator
        self._authenticator = authenticator
        self.primary_keys = primary_keys
        self.replication_key = replication_key
        self.except_keys = except_keys
        self.records_path = records_path

        # Optional enhancements
        self.inject_metadata = inject_metadata or {}
        self.id_registry_config = id_registry_config or {}
        self._id_registry_cache: set[str] = set()  # dedupe within this run
        self.max_records_limit = max_records_limit  # Per-stream record limit
        self._records_processed = 0  # Track records processed in this stream

        # Store pagination styles FIRST, before any conditional logic
        self.pagination_request_style = pagination_request_style
        self.pagination_response_style = pagination_response_style
        
        # Respect stream-level next_page_token_path when provided.
        if next_page_token_path:
            self.next_page_token_jsonpath = next_page_token_path
        elif (
            self.pagination_request_style == "jsonpath_paginator"
            or self.pagination_request_style == "default"
        ):
            self.next_page_token_jsonpath = (
                "$.next_page"  # Set default for jsonpath_paginator
            )

        # Selecting the appropriate method to send Parameters as part of the
        # request. If use_request_body_not_params is set the parameters are sent
        # in the request body instead of request parameters. The
        # pagination_response_style config determines what style of parameter
        # processing is invoked.

        get_url_params_styles = {
            "style1": self._get_url_params_offset_style,
            "offset": self._get_url_params_offset_style,
            "page": self._get_url_params_page_style,
            "header_link": self._get_url_params_header_link,
            "hateoas_body": self._get_url_params_hateoas_body,
        }

        self.use_request_body_not_params = use_request_body_not_params
        self.backoff_type = backoff_type
        self.backoff_param = backoff_param
        self.backoff_time_extension = backoff_time_extension
        self.store_raw_json_message = store_raw_json_message
        if self.use_request_body_not_params:
            self.prepare_request_payload = get_url_params_styles.get(  # type: ignore
                pagination_response_style, self._get_url_params_page_style
            )  # Defaults to page_style url_params
        else:
            self.get_url_params = get_url_params_styles.get(  # type: ignore
                pagination_response_style, self._get_url_params_page_style
            )  # Defaults to page_style url_params

        # Pagination configuration
        self.pagination_results_limit = pagination_results_limit
        self.pagination_next_page_param = pagination_next_page_param
        self.pagination_limit_per_page_param = pagination_limit_per_page_param
        self.pagination_total_limit_param = pagination_total_limit_param
        self.start_date = start_date
        self.source_search_field = source_search_field
        self.source_search_query = source_search_query
        self.pagination_page_size: Optional[int]
        self.pagination_initial_offset = pagination_initial_offset
        self.offset_records_jsonpath = offset_records_jsonpath

        # Setting Pagination Limits
        if self.pagination_request_style == "restapi_header_link_paginator":
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                if self.pagination_limit_per_page_param:
                    page_limit_param = self.pagination_limit_per_page_param
                else:
                    page_limit_param = "per_page"
                self.pagination_page_size = int(
                    self.params.get(page_limit_param, 25)
                )  # Default to requesting 25 records
        elif (
            self.pagination_request_style == "style1"
            or self.pagination_request_style == "offset_paginator"
        ):
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = (
                    self.pagination_results_limit
                )  # Will raise an exception.
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                if self.pagination_limit_per_page_param:
                    page_limit_param = self.pagination_limit_per_page_param
                else:
                    page_limit_param = "limit"
                self.pagination_page_size = int(
                    self.params.get(page_limit_param, 25)
                )  # Default to requesting 25 records
        else:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = (
                    self.pagination_results_limit
                )  # Will raise an exception.
            self.pagination_page_size = pagination_page_size

        # GitHub is missing the "since" parameter on a few endpoints
        # set this parameter to True if your stream needs to navigate data in
        # descending order
        # and try to exit early on its own.
        # This only has effect on streams whose `replication_key` is `updated_at`.
        self.use_fake_since_parameter = False

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
              A dictionary of the headers to be included in the request.

        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")

        if self.headers:
            for k, v in self.headers.items():
                headers[k] = v

        return headers

    def backoff_wait_generator(
        self,
    ) -> Generator[Union[int, float], None, None]:
        """Return a backoff generator as required to manage Rate Limited APIs.

        Supply a backoff_type in the config to indicate the style of backoff.
        If the backoff response is in a header, supply a backoff_param
        indicating what key contains the backoff delay.

        Note: If the backoff_type is message, the message is parsed for numeric
        values. It is assumed that the highest numeric value discovered is the
        backoff value in seconds.

        Returns:
            Backoff Generator with value to wait based on the API Response.

        """

        def _backoff_from_headers(exception):
            response_headers = exception.response.headers

            return (
                int(response_headers.get(self.backoff_param, 0))
                + self.backoff_time_extension
            )

        def _get_wait_time_from_response(exception):
            response_message = exception.response.json().get("message", 0)
            res = [int(i) for i in response_message.split() if i.isdigit()]

            return int(max(res)) + self.backoff_time_extension

        if self.backoff_type == "message":
            return self.backoff_runtime(value=_get_wait_time_from_response)
        elif self.backoff_type == "header":
            return self.backoff_runtime(value=_backoff_from_headers)
        else:
            # No override required. Use SDK backoff_wait_generator
            return super().backoff_wait_generator()

    def get_new_paginator(self):
        """Return the requested paginator required to retrieve all data from the API.

        Returns:
            Paginator Class.

        """
        self.logger.info(
            f"the next_page_token_jsonpath = {self.next_page_token_jsonpath}."
        )

        if (
            self.pagination_request_style == "jsonpath_paginator"
            or self.pagination_request_style == "default"
        ):
            return JSONPathPaginator(self.next_page_token_jsonpath)
        elif (
            self.pagination_request_style == "simple_header_paginator"
        ):  # Example Gitlab.com
            if self.next_page_token_jsonpath:
                return JSONPathPaginator(self.next_page_token_jsonpath)

            return SimpleHeaderPaginator("X-Next-Page")
        elif self.pagination_request_style == "header_link_paginator":
            return HeaderLinkPaginator()
        elif (
            self.pagination_request_style == "restapi_header_link_paginator"
        ):  # Example GitHub.com
            return RestAPIHeaderLinkPaginator(
                pagination_page_size=self.pagination_page_size,
                pagination_results_limit=self.pagination_results_limit,
                replication_key=self.replication_key,
            )
        elif (
            self.pagination_request_style == "style1"
            or self.pagination_request_style == "offset_paginator"
        ):
            return RestAPIOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size,
                jsonpath=self.next_page_token_jsonpath,
                pagination_total_limit_param=self.pagination_total_limit_param,
            )
        elif self.pagination_request_style == "hateoas_paginator":
            return BaseHATEOASPaginator()
        elif self.pagination_request_style == "single_page_paginator":
            return SinglePagePaginator()
        elif self.pagination_request_style == "page_number_paginator":
            return RestAPIBasePageNumberPaginator(
                start_value=self.pagination_initial_offset,
                jsonpath=self.next_page_token_jsonpath
            )
        elif self.pagination_request_style == "simple_offset_paginator":
            return SimpleOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size,
                offset_records_jsonpath=self.offset_records_jsonpath,
                pagination_page_size=self.pagination_page_size,
            )
        else:
            self.logger.error(
                f"Unknown paginator {self.pagination_request_style}. Please declare "
                f"a valid paginator."
            )
            raise ValueError(
                f"Unknown paginator {self.pagination_request_style}. Please declare "
                f"a valid paginator."
            )

    def _get_url_params_page_style(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """
        Return a dictionary of values to be used in URL parameterization.
        This version is corrected to handle cursor-based pagination.
        """
        # Initialise Starting Values
        last_run_date = get_start_date(self, context)
        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v

        # If a next_page_token (the cursor value) exists, add it to the params.
        # It uses the `pagination_next_page_param` setting from meltano.yml,
        # which we will set to 'cursor'.
        if next_page_token:
            next_page_param = self.pagination_next_page_param or "page"
            params[next_page_param] = next_page_token

        if self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                query_template = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(
                        query_template.substitute(last_run_date=last_run_date)
                    )
                else:
                    params[self.source_search_field] = query_template.substitute(
                        last_run_date=last_run_date
                    )
            else:
                params["sort"] = "asc"
                params["order_by"] = self.replication_key

        return params

    def _get_url_params_offset_style(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.

        Returns:
            An object containing the parameters to add to the request.

        """
        # Initialise Starting Values
        last_run_date = get_start_date(self, context)
        params: dict = {}

        if self.params:
            for k, v in self.params.items():
                params[k] = v
        if next_page_token:
            if self.pagination_next_page_param:
                next_page_parm = self.pagination_next_page_param
            else:
                next_page_parm = "offset"
            params[next_page_parm] = next_page_token
        if self.pagination_page_size is not None:
            if self.pagination_limit_per_page_param:
                limit_per_page_param = self.pagination_limit_per_page_param
            else:
                limit_per_page_param = "limit"
            params[limit_per_page_param] = self.pagination_page_size
        if self.replication_key:
            # Use incremental replication (if available) via a filter query being sent
            # to the API This assumes storing a replication timestamp and querying
            # records greater than that date in subsequent runs. Config the appropriate
            # source field and query template.
            if self.source_search_field and self.source_search_query and last_run_date:
                query_template = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(
                        query_template.substitute(last_run_date=last_run_date)
                    )
                else:
                    params[self.source_search_field] = query_template.substitute(
                        last_run_date=last_run_date
                    )
            else:
                params["sort"] = "asc"
                params["order_by"] = self.replication_key

        return params

    def _get_url_params_header_link(
        self, context: Optional[Dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Logic based on https://github.com/MeltanoLabs/tap-github

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.

        Returns:
            An object containing the parameters to add to the request.

        """
        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        if self.pagination_page_size:
            pagination_page_size = self.pagination_page_size
        else:
            pagination_page_size = 25  # Default to 25 per page if not set
        if self.pagination_limit_per_page_param:
            limit_per_page_param = self.pagination_limit_per_page_param
        else:
            limit_per_page_param = "per_page"
        params[limit_per_page_param] = pagination_page_size
        if next_page_token:
            request_parameters = parse_qs(str(next_page_token))
            for k, v in request_parameters.items():
                params[k] = v

        if self.replication_key == "updated_at":
            params["sort"] = "updated"
            params["direction"] = "desc" if self.use_fake_since_parameter else "asc"

        # Unfortunately the /starred, /stargazers (starred_at) and /events (created_at)
        # endpoints do not support the "since" parameter out of the box. But we use a
        # workaround in 'get_next_page_token'.
        elif self.replication_key in ["starred_at", "created_at"]:
            params["sort"] = "created"
            params["direction"] = "desc"

        # Warning: /commits endpoint accept "since" but results are ordered by
        # descending commit_timestamp
        elif self.replication_key == "commit_timestamp":
            params["direction"] = "desc"

        elif self.replication_key:
            self.logger.warning(
                f"The replication key '{self.replication_key}' is not fully supported "
                f"by this client yet."
            )

        since = self.get_starting_timestamp(context)
        since_key = "since" if not self.use_fake_since_parameter else "fake_since"
        if self.replication_key and since:
            params[since_key] = since
            # Leverage conditional requests to save API quotas
            # https://github.community/t/how-does-if-modified-since-work/139627
            self._http_headers["If-modified-since"] = email.utils.format_datetime(since)

        return params

    def _get_url_params_hateoas_body(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.

            HATEOAS stands for "Hypermedia as the Engine of Application State".
             See https://en.wikipedia.org/wiki/HATEOAS.

            Note: Under the HATEOAS model, the returned token contains all the
            required parameters for the subsequent call. The function splits the
            parameters into Dict key value pairs for subsequent requests.

        Returns:
            An object containing the parameters to add to the request.

        """
        # Initialise Starting Values
        last_run_date = get_start_date(self, context)
        params: dict = {}

        if self.params:
            for k, v in self.params.items():
                params[k] = v

        # Set Pagination Limits if required.
        if self.pagination_page_size and self.pagination_limit_per_page_param:
            params[self.pagination_limit_per_page_param] = self.pagination_page_size

        if next_page_token:
            # Parse the next_page_token for the path and parameters
            url_parsed = urlparse(next_page_token)
            if url_parsed.query:
                params.update(parse_qsl(url_parsed.query))
            else:
                params.update(parse_qsl(url_parsed.path))
            if url_parsed.path == next_page_token:
                self.path = ""
            else:
                self.path = url_parsed.path
        elif self.replication_key:
            # Use incremental replication (if available) via a filter query being sent
            # to the API This assumes storing a replication timestamp and querying
            # records greater than that date in subsequent runs. Config the appropriate
            # source field and query template.
            if self.source_search_field and self.source_search_query and last_run_date:
                query_template = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(
                        query_template.substitute(last_run_date=last_run_date)
                    )
                else:
                    params[self.source_search_field] = query_template.substitute(
                        last_run_date=last_run_date
                    )
            elif self.source_search_field and last_run_date:
                params[self.source_search_field] = "gt" + last_run_date

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: required - the requests.Response given by the api call.

        Yields:
              Parsed records.

        """
        # Check if we've hit the per-stream record limit
        if self.max_records_limit and self._records_processed >= self.max_records_limit:
            self.logger.info(
                f"Stream {self.name} reached max_records_limit of {self.max_records_limit}"
            )
            return
            
        for record in extract_jsonpath(self.records_path, input=response.json()):
            if self.max_records_limit and self._records_processed >= self.max_records_limit:
                self.logger.info(
                    f"Stream {self.name} stopping at {self.max_records_limit} records"
                )
                return
            self._records_processed += 1
            yield record

    # ----------------------------
    # Registry capture (Tweet IDs)
    # ----------------------------
    def _maybe_register_id(self, flat_record: dict, original_row: dict) -> None:
        """Optionally register a tweet ID into Singer state for downstream fan-out.

        Behavior:
        - No-ops unless `id_registry_config` is a non-empty dict with a
          `registry_key` configured.
        - Extracts the ID using `id_path` (JSONPath) against **original_row**.
          Falls back to 'id' from the flattened record if JSONPath fails.
        - Applies simple engagement-based filters on the **flattened** record
          (`min_like_count`, `min_view_count`) if provided.
        - Dedupes within the stream/run and respects `max_to_register_per_run`.

        Note:
            This does not perform any network calls. It only stores IDs into
            self._tap.state["registry"][registry_key] so other streams can read
            them later (e.g., via iteration_config.from_registry).
        """
        cfg = self.id_registry_config or {}
        registry_key = cfg.get("registry_key")
        if not registry_key:
            return  # Not enabled

        # Maximum per run for safety
        max_per_run = int(cfg.get("max_to_register_per_run", 0)) or None
        if max_per_run is not None and len(self._id_registry_cache) >= max_per_run:
            return

        # Extract ID
        id_path = cfg.get("id_path", "$.id")
        tweet_id = None
        try:
            # Prefer original row for ID extraction
            matches = list(extract_jsonpath(id_path, input=original_row))
            if matches:
                tweet_id = str(matches[0])
        except Exception:
            tweet_id = None

        if not tweet_id:
            # Fallback to flattened record
            fallback = flat_record.get("id")
            if fallback:
                tweet_id = str(fallback)

        if not tweet_id:
            return  # no ID, nothing to register

        # Simple filters on engagement (flattened fields).
        try:
            min_likes = int(cfg.get("min_like_count", 0))
        except Exception:
            min_likes = 0
        try:
            min_views = int(cfg.get("min_view_count", 0))
        except Exception:
            min_views = 0

        like_count = int(flat_record.get("likeCount", 0) or 0)
        view_count = int(flat_record.get("viewCount", 0) or 0)

        if like_count < min_likes or view_count < min_views:
            return

        # Deduplicate within this run/stream.
        if tweet_id in self._id_registry_cache:
            return

        # Write into tap state registry (persistent across streams in the same run).
        reg = self._tap.state.setdefault("registry", {})
        reg_list = reg.setdefault(registry_key, [])
        if tweet_id not in reg_list:
            reg_list.append(tweet_id)
            self._id_registry_cache.add(tweet_id)
            # Persist state immediately to be safe across process boundaries.
            self._tap.state["registry"] = reg  # explicit set for some runners
            self._tap.persist_state()

    def post_process(
        self,
        row: dict,
        context: Optional[dict] = None,  # noqa: ARG002
    ) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: required - the record for processing.
            context: optional - the singer context object.

        Returns:
              A record that has been processed.

        Behavior:
        - Flattens the record via utils.flatten_json, honoring except_keys and
          store_raw_json_message settings.
        - Converts specific date fields from Twitter's format to ISO 8601.
        - Injects constant metadata (if provided) without overwriting existing keys.
        - Optionally registers a tweet ID into Singer state for downstream fan-out
          (e.g., replies/quotes), controlled by id_registry_config.
        """
        # Keep a reference to the original row for ID extraction by JSONPath.
        original_row = row

        # Flatten first
        flat = flatten_json(row, self.except_keys, self.store_raw_json_message)
        
        # --- NEW: Convert date formats to ISO 8601 ---
        # List of fields that use Twitter's non-standard date format
        date_fields_to_convert = ["createdAt", "author_createdAt"]
        for field_name in date_fields_to_convert:
            if field_name in flat and isinstance(flat[field_name], str):
                try:
                    # Parse the input string: 'Mon Sep 15 00:12:16 +0000 2025'
                    date_obj = datetime.strptime(
                        flat[field_name], "%a %b %d %H:%M:%S %z %Y"
                    )
                    # Convert to ISO 8601 format, which the target expects
                    flat[field_name] = date_obj.isoformat()
                except ValueError:
                    # If parsing fails for any reason, log a warning but don't crash
                    self.logger.warning(
                        f"Could not parse timestamp for field '{field_name}': "
                        f"'{flat[field_name]}'"
                    )
        # --- END NEW ---

        # Inject constant metadata for provenance/partitioning (optional).
        if self.inject_metadata:
            for k, v in self.inject_metadata.items():
                flat.setdefault(k, v)

        # Optionally capture tweet IDs into state for later fan-out.
        # Only makes sense for tweet-like streams (advanced_search, last_tweets, hashtags),
        # but is safe to no-op elsewhere.
        try:
            self._maybe_register_id(flat_record=flat, original_row=original_row)
        except Exception as ex:
            # Never fail the pipeline due to registry capture; just log.
            self.logger.debug(f"ID registry capture skipped due to error: {ex}")

        return flat