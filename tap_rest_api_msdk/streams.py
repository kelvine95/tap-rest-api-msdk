"""Stream type classes for tap-rest-api-msdk."""

import email.utils
import json
from datetime import datetime, timezone
from string import Template
from typing import Any, Dict, Generator, Iterable, Optional, Union, List, Set
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


class DynamicStream(RestApiStream):
    """Define custom stream."""

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
        # Twitter-specific parameters
        twitter_mode: Optional[bool] = False,
        twitter_usernames: Optional[List[str]] = None,
        twitter_hashtags: Optional[List[str]] = None,
        twitter_stream_type: Optional[str] = None,
        twitter_parent_streams: Optional[List[str]] = None,
        twitter_max_per_run: Optional[int] = None,
        tap_instance: Optional[Any] = None,
    ) -> None:
        """Class initialization."""
        # IMPORTANT: register the stream under its own name (not the tap's)
        super().__init__(tap=tap, name=name, schema=schema)

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

        # Twitter-specific attributes
        self.twitter_mode = bool(twitter_mode)
        self.twitter_usernames = twitter_usernames or []
        self.twitter_hashtags = twitter_hashtags or []
        self.twitter_stream_type = twitter_stream_type
        self.twitter_parent_streams = twitter_parent_streams or []
        self.twitter_max_per_run = twitter_max_per_run or 20
        self.tap_instance = tap_instance
        self._twitter_records_collected = 0
        self._twitter_pending_state = {}

        if next_page_token_path:
            self.next_page_token_jsonpath = next_page_token_path
        elif (pagination_request_style == "jsonpath_paginator" or
            pagination_request_style == "default"):
            # Set default for jsonpath_paginator
            self.next_page_token_jsonpath = "$.next_page"

        get_url_params_styles = {
            "style1": self._get_url_params_offset_style,
            "offset": self._get_url_params_offset_style,
            "page": self._get_url_params_page_style,
            "header_link": self._get_url_params_header_link,
            "hateoas_body": self._get_url_params_hateoas_body,
        }

        # Parameter handling strategy
        self.use_request_body_not_params = use_request_body_not_params
        self.backoff_type = backoff_type
        self.backoff_param = backoff_param
        self.backoff_time_extension = backoff_time_extension
        self.store_raw_json_message = store_raw_json_message

        if self.use_request_body_not_params:
            self.prepare_request_payload = get_url_params_styles.get(  # type: ignore
                pagination_response_style, self._get_url_params_page_style
            )
        else:
            self.get_url_params = get_url_params_styles.get(  # type: ignore
                pagination_response_style, self._get_url_params_page_style
            )

        self.pagination_request_style = pagination_request_style
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
                page_limit_param = self.pagination_limit_per_page_param or "per_page"
                self.pagination_page_size = int(self.params.get(page_limit_param, 25))
        elif self.pagination_request_style in ("style1", "offset_paginator"):
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit  # noqa: N806
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                page_limit_param = self.pagination_limit_per_page_param or "limit"
                self.pagination_page_size = int(self.params.get(page_limit_param, 25))
        else:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit  # noqa: N806
            self.pagination_page_size = pagination_page_size

        # For GitHub-like special-casing; disabled here by default.
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
                next_page_parm = "page"
            params[next_page_parm] = next_page_token
        if self.replication_key:
            # Use incremental replication (if available) via a filter query being
            # sent to the API This assumes storing a replication timestamp and querying
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
        """Parse the response and return an iterator of result rows."""
        # Twitter mode: specialized parsing
        if self.twitter_mode:
            yield from self._parse_twitter_response(response)
            return

        # Default behavior for non-Twitter APIs with basic safety
        try:
            data = response.json()
        except Exception:
            self.logger.warning("Non-JSON or empty response; skipping page.")
            return

        yield from extract_jsonpath(self.records_path, input=data)


    def _parse_twitter_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse response in Twitter mode with special handling."""
        try:
            response_data = response.json()
        except Exception:
            self.logger.warning("Twitter mode: response is not valid JSON; skipping.")
            return

        # --- user info ---------------------------------------------------------
        if self.twitter_stream_type == "user_info":
            data = response_data.get("data")
            if isinstance(data, dict):
                yield data
            return

        # --- collections with 'tweets' array -----------------------------------
        if self.twitter_stream_type in {"user_tweets", "mentions", "hashtag_tweets"}:
            records = response_data.get("tweets") or []
            if not isinstance(records, list):
                self.logger.debug("Twitter mode: expected list at key 'tweets'.")
                return

            for record in records:
                if not isinstance(record, dict):
                    continue

                # collect tweet ID for dependent streams
                if self.tap_instance and "id" in record:
                    try:
                        self.tap_instance.add_collected_tweet_id(record["id"])
                    except Exception:
                        # never fail the stream because of state-sharing
                        pass

                # tracking metadata
                if self.twitter_stream_type == "hashtag_tweets":
                    current_hashtag = getattr(self, "_current_hashtag", None)
                    if current_hashtag:
                        record["_hashtag_tracked"] = current_hashtag
                else:
                    current_username = getattr(self, "_current_username", None)
                    if current_username:
                        record["_username_tracked"] = current_username

                record["_source_stream"] = self.twitter_stream_type

                # enforce limit
                if self._twitter_records_collected >= self.twitter_max_per_run:
                    break
                self._twitter_records_collected += 1

                yield record
            return

        # --- replies & quotes ---------------------------------------------------
        if self.twitter_stream_type in {"replies", "quotes"}:
            if self.twitter_stream_type == "replies":
                key = "replies"
            else:
                key = "tweets"  # quotes return under "tweets"

            records = response_data.get(key) or []
            if not isinstance(records, list):
                self.logger.debug("Twitter mode: expected list at key '%s'.", key)
                return

            for record in records:
                if not isinstance(record, dict):
                    continue

                # parent tweet metadata
                current_tweet_id = getattr(self, "_current_tweet_id", None)
                if current_tweet_id:
                    record["_parent_tweet_id"] = current_tweet_id

                record["_source_stream"] = self.twitter_stream_type

                # enforce limit
                if self._twitter_records_collected >= self.twitter_max_per_run:
                    break
                self._twitter_records_collected += 1

                yield record
            return

        # --- fallback -----------------------------------------------------------
        yield from extract_jsonpath(self.records_path, input=response_data)

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Override to handle Twitter-specific iteration logic."""
        # Default behavior for non-Twitter APIs
        if not self.twitter_mode:
            yield from super().get_records(context)
            return

        # Twitter mode
        self._twitter_records_collected = 0  # reset per run

        def remaining() -> int:
            return max(0, int(self.twitter_max_per_run) - int(self._twitter_records_collected))

        # helper to add sinceTime if available (integer epoch secs)
        def add_since_time(p: dict) -> dict:
            try:
                last_sync = self.get_starting_timestamp(context)
            except Exception:
                last_sync = None
            if last_sync:
                try:
                    p["sinceTime"] = int(last_sync.timestamp())
                except Exception:
                    # be conservative: skip sinceTime if conversion fails
                    pass
            return p

        # --- user info ----------------------------------------------------------
        if self.twitter_stream_type == "user_info":
            for username in self.twitter_usernames:
                if remaining() <= 0:
                    return
                self._current_username = username
                params = {"userName": username}
                yield from self._fetch_with_params(params, context)
            return

        # --- user tweets --------------------------------------------------------
        if self.twitter_stream_type == "user_tweets":
            for username in self.twitter_usernames:
                if remaining() <= 0:
                    return
                self._current_username = username
                local = {"userName": username}
                # includeReplies default False unless already provided at stream level
                if "includeReplies" not in (self.params or {}):
                    local["includeReplies"] = False
                local = add_since_time(local)
                yield from self._fetch_with_pagination(local, context)
            return

        # --- mentions -----------------------------------------------------------
        if self.twitter_stream_type == "mentions":
            for username in self.twitter_usernames:
                if remaining() <= 0:
                    return
                self._current_username = username
                local = {"userName": username}
                local = add_since_time(local)
                yield from self._fetch_with_pagination(local, context)
            return

        # --- hashtag tweets (advanced search) -----------------------------------
        if self.twitter_stream_type == "hashtag_tweets":
            for hashtag in self.twitter_hashtags:
                if remaining() <= 0:
                    return
                self._current_hashtag = hashtag

                # Build query; for incremental runs add since:YYYY-MM-DD_HH:MM:SS_UTC
                q = f"#{hashtag}"
                try:
                    last_sync = self.get_starting_timestamp(context)
                except Exception:
                    last_sync = None
                if last_sync:
                    # Ensure UTC and full timestamp with _UTC suffix per API examples
                    last_sync_utc = last_sync.astimezone(timezone.utc)
                    q += f" since:{last_sync_utc.strftime('%Y-%m-%d_%H:%M:%S')}_UTC"

                local = {"query": q, "queryType": "Latest"}
                yield from self._fetch_with_pagination(local, context)
            return

        # --- replies & quotes (dependent on collected tweet ids) ----------------
        if self.twitter_stream_type in {"replies", "quotes"}:
            operation = "replies" if self.twitter_stream_type == "replies" else "quotes"

            tweet_ids: Set[str] = set()
            if self.tap_instance:
                try:
                    tweet_ids |= set(self.tap_instance.get_collected_tweet_ids())
                except Exception:
                    pass
                try:
                    tweet_ids |= set(self.tap_instance.get_pending_tweet_ids(operation))
                except Exception:
                    pass

            for tweet_id in tweet_ids:
                if remaining() <= 0:
                    return

                self._current_tweet_id = tweet_id
                if self.twitter_stream_type == "replies":
                    local = {"tweetId": tweet_id}
                else:
                    # API defaults includeReplies=True for quotes; we won't force it unless user configured
                    local = {"tweetId": tweet_id}

                local = add_since_time(local)

                try:
                    yield from self._fetch_with_pagination(local, context)
                    if self.tap_instance:
                        try:
                            self.tap_instance.remove_pending_tweet_id(tweet_id, operation)
                        except Exception:
                            pass
                except Exception as exc:
                    # keep as pending for next run
                    if self.tap_instance:
                        try:
                            self.tap_instance.add_pending_tweet_id(tweet_id, operation)
                        except Exception:
                            pass
                    self.logger.warning("Failed to process %s for tweet %s: %s",
                                        operation, tweet_id, exc)
            return

        # --- unknown twitter stream type -> fallback to default -----------------
        self.logger.debug("Unknown twitter_stream_type=%s; using default behavior.",
                        self.twitter_stream_type)
        yield from super().get_records(context)


    def _fetch_with_params(self, params: dict, context: Optional[dict]) -> Iterable[dict]:
        """Fetch records with specific parameters."""
        # Merge base params with per-call overrides
        full_params = {**self.params, **params}

        # Build and execute request
        prepared_request = self.prepare_request(context, full_params)
        # FIX: _request requires (prepared_request, context)
        response = self.request_decorator(self._request)(prepared_request, context)

        if response.status_code == 200:
            yield from self.parse_response(response)
        else:
            self.logger.warning(
                "API request failed (%s) for %s: %s",
                response.status_code,
                getattr(self, "name", "<unknown-stream>"),
                getattr(response, "text", ""),
            )

    def _fetch_with_pagination(self, params: dict, context: Optional[dict]) -> Iterable[dict]:
        """Fetch records with pagination support."""
        cursor = ""
        has_more = True

        while has_more and self._twitter_records_collected < self.twitter_max_per_run:
            # Merge base params and page cursor
            full_params = {**self.params, **params}
            if cursor:
                full_params["cursor"] = cursor

            # Build and execute request
            prepared_request = self.prepare_request(context, full_params)
            # FIX: _request requires (prepared_request, context)
            response = self.request_decorator(self._request)(prepared_request, context)

            if response.status_code != 200:
                self.logger.warning(
                    "API request failed (%s) for %s: %s",
                    response.status_code,
                    getattr(self, "name", "<unknown-stream>"),
                    getattr(response, "text", ""),
                )
                break  # stop the loop; avoid spinning on persistent failures

            # Parse and emit records
            try:
                response_data = response.json()
            except Exception:
                self.logger.warning("Non-JSON or empty response; stopping pagination.")
                break

            yield from self.parse_response(response)

            # Paginate if possible
            next_cursor = response_data.get("next_cursor")
            has_next = response_data.get("has_next_page")

            if has_next and next_cursor:
                cursor = next_cursor
            else:
                has_more = False

    def post_process(  # noqa: PLR6301
        self,
        row: types.Record,
        context: Optional[types.Context] = None,  # noqa: ARG002
    ) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: required - the record for processing.
            context: optional - the singer context object.

        Returns:
              A record that has been processed.

        """
        return flatten_json(row, self.except_keys, self.store_raw_json_message)
    