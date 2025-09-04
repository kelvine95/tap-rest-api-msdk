"""Stream type classes for tap-rest-api-msdk with era-based incremental support."""

import email.utils
import json
from datetime import datetime
from string import Template
from typing import Any, Dict, Generator, Iterable, Optional, Union
from urllib.parse import parse_qs, parse_qsl, urlparse
import time

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


class EraBasedPageNumberPaginator(RestAPIBasePageNumberPaginator):
    """Custom paginator that tracks era_id for incremental sync."""

    def __init__(self, *args, era_field=None, max_pages_per_run=50, logger=None, **kwargs):
        """
        Initializes the paginator.
        Args:
            *args: Positional arguments for the base class.
            era_field (str, optional): The name of the era field. Defaults to None.
            max_pages_per_run (int, optional): Max pages to fetch per run. Defaults to 50.
            logger (logging.Logger, optional): The logger instance. Defaults to None.
            **kwargs: Keyword arguments for the base class.
        """
        super().__init__(*args, **kwargs)
        self.era_field = era_field
        self.max_pages_per_run = max_pages_per_run
        self.pages_fetched = 0
        self.stop_pagination = False
        self.highest_era_seen = None
        # Use the logger passed from the Stream, or create a default one
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def has_more(self, response: requests.Response) -> bool:
        """Check if more pages exist and if we should continue."""
        # Note: The SDK increments self.pages_fetched in the Stream class after
        # a successful request, so we don't need to increment it here.
        
        # Check if the run has hit the configured page limit
        if self.pages_fetched >= self.max_pages_per_run:
            self.logger.info(
                f"Reached page limit for this run ({self.max_pages_per_run} pages)."
            )
            return False

        # Check if the parse_response method signaled a stop
        if self.stop_pagination:
            self.logger.info("Stopping pagination because a known era was reached.")
            return False

        response_data = response.json()
        total_pages = response_data.get("page_count") or response_data.get("pageCount")

        if total_pages is None:
            self.logger.warning(
                "API response did not contain 'page_count' or 'pageCount'. "
                "Assuming no more pages."
            )
            return False

        # The SDK's paginator increments `current_value` to be the *next* page number
        # before this method is called. This logic checks if that next page is valid.
        has_more_pages = self.current_value <= total_pages
        
        # As a safeguard, also check if the last response returned any records.
        has_records = bool(response_data.get("data"))

        if not has_more_pages:
            self.logger.info(
                f"No more pages to fetch. Next page would be {self.current_value}, "
                f"but total pages is {total_pages}."
            )

        return has_more_pages and has_records

class DynamicStream(RestApiStream):
    """Define custom stream with era-based incremental support."""

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
        # New/modified parameters for era-based incremental
        era_based_incremental: Optional[bool] = False,
        era_field: Optional[str] = None,
        max_pages_per_run: Optional[int] = 50,
        initial_sync_era_id: Optional[int] = None, # New parameter
    ) -> None:
        """Class initialization with era-based incremental support.

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
            era_based_incremental: Enable era-based incremental sync
            era_field: Field name containing the era_id (e.g., "era_id")
            max_pages_per_run: Maximum pages to fetch per run (for API limits)
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

        # Era-based incremental settings
        self.era_based_incremental = era_based_incremental
        self.era_field = era_field or "era_id"
        self.max_pages_per_run = max_pages_per_run
        
        # New setting for the initial sync filter
        self.initial_sync_era_id = initial_sync_era_id

        # This will hold the bookmark that is built during the current run
        self._run_state_bookmark: Dict[str, Any] = {}

        if next_page_token_path:
            self.next_page_token_jsonpath = next_page_token_path
        elif (
            pagination_request_style == "jsonpath_paginator"
            or pagination_request_style == "default"
        ):
            self.next_page_token_jsonpath = "$.next_page"

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
            self.prepare_request_payload = get_url_params_styles.get(
                pagination_response_style, self._get_url_params_page_style
            )
        else:
            self.get_url_params = get_url_params_styles.get(
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
                if self.pagination_limit_per_page_param:
                    page_limit_param = self.pagination_limit_per_page_param
                else:
                    page_limit_param = "per_page"
                self.pagination_page_size = int(
                    self.params.get(page_limit_param, 25)
                )
        elif (
            self.pagination_request_style == "style1"
            or self.pagination_request_style == "offset_paginator"
        ):
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                if self.pagination_limit_per_page_param:
                    page_limit_param = self.pagination_limit_per_page_param
                else:
                    page_limit_param = "limit"
                self.pagination_page_size = int(
                    self.params.get(page_limit_param, 25)
                )
        else:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit
            self.pagination_page_size = pagination_page_size

        self.use_fake_since_parameter = False
    
    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        """
        Perform a request, logging the HTTP request and response.

        This method overrides the default to add a client-side delay,
        ensuring the tap respects the API's rate limit.

        Args:
            prepared_request: The prepared request object to send.
            context: Stream partition or context dictionary.

        Returns:
            The HTTP response object.
        """
        # Calculate the required delay to stay under 100 requests/minute.
        # 60 seconds / 100 requests = 0.6 seconds/request. We add a buffer.
        rate_limit_delay_seconds = 0.7

        # Call the parent class's _request method to execute the API call
        response = super()._request(prepared_request, context)

        # After every successful request, pause to respect the rate limit
        self.logger.info(
            f"Pausing for {rate_limit_delay_seconds} seconds to respect API rate limit."
        )
        time.sleep(rate_limit_delay_seconds)
        
        return response

    def get_starting_bookmark(self, context: Optional[dict]) -> Optional[dict]:
        """Get the last processed composite bookmark from state."""
        if not self.era_based_incremental:
            return None
        
        state = self.get_stream_or_partition_state(context)
        # The SDK stores the simple replication_key_value, we need our custom bookmark
        return state.get("bookmarks", {}).get(self.name) 
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
              A dictionary of the headers to be included in the request.

        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
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
            return super().backoff_wait_generator()

    def get_new_paginator(self):
        """Return the requested paginator with era-based support if configured."""
        self.logger.info(
            f"the next_page_token_jsonpath = {getattr(self, 'next_page_token_jsonpath', None)}."
        )

        # For era-based incremental, use our custom paginator
        if self.era_based_incremental and self.pagination_request_style == "page_number_paginator":
            return EraBasedPageNumberPaginator(
                start_value=self.pagination_initial_offset,
                jsonpath=getattr(self, 'next_page_token_jsonpath', None),
                era_field=self.era_field,
                max_pages_per_run=self.max_pages_per_run,
                logger=self.logger  # <--- This is the crucial line that passes the logger
            )
        
        # Default paginators (existing logic)
        if (
            self.pagination_request_style == "jsonpath_paginator"
            or self.pagination_request_style == "default"
        ):
            return JSONPathPaginator(self.next_page_token_jsonpath)
        elif self.pagination_request_style == "simple_header_paginator":
            if self.next_page_token_jsonpath:
                return JSONPathPaginator(self.next_page_token_jsonpath)
            return SimpleHeaderPaginator("X-Next-Page")
        elif self.pagination_request_style == "header_link_paginator":
            return HeaderLinkPaginator()
        elif self.pagination_request_style == "restapi_header_link_paginator":
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
            msg = (
                f"Unknown paginator {self.pagination_request_style}. Please declare "
                f"a valid paginator."
            )
            self.logger.error(msg)
            raise ValueError(msg)

    def get_starting_era(self, context: Optional[dict]) -> Optional[int]:
        """Get the last processed era_id from state."""
        if not self.era_based_incremental:
            return None
        
        state_value = self.get_starting_replication_key_value(context)
        if state_value and isinstance(state_value, (int, str)):
            return int(state_value)
        return None

    def _get_url_params_page_style(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return URL parameters for page-style pagination."""
        # Don't add date filtering for era-based incremental
        if self.era_based_incremental:
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
            return params
        
        # Original logic for non-era-based streams
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
        if self.replication_key and not self.era_based_incremental:
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
        """Return URL parameters for offset-style pagination."""
        # Similar modification for offset style
        if self.era_based_incremental:
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
            return params
        
        # Original logic
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
        if self.replication_key and not self.era_based_incremental:
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
        """Return URL parameters for header link pagination."""
        params: dict = {}
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        if self.pagination_page_size:
            pagination_page_size = self.pagination_page_size
        else:
            pagination_page_size = 25
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
        elif self.replication_key in ["starred_at", "created_at"]:
            params["sort"] = "created"
            params["direction"] = "desc"
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
            self._http_headers["If-modified-since"] = email.utils.format_datetime(since)

        return params

    def _get_url_params_hateoas_body(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return URL parameters for HATEOAS pagination."""
        last_run_date = get_start_date(self, context)
        params: dict = {}

        if self.params:
            for k, v in self.params.items():
                params[k] = v

        if self.pagination_page_size and self.pagination_limit_per_page_param:
            params[self.pagination_limit_per_page_param] = self.pagination_page_size

        if next_page_token:
            url_parsed = urlparse(next_page_token)
            if url_parsed.query:
                params.update(parse_qsl(url_parsed.query))
            else:
                params.update(parse_qsl(url_parsed.path))
            if url_parsed.path == next_page_token:
                self.path = ""
            else:
                self.path = url_parsed.path
        elif self.replication_key and not self.era_based_incremental:
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
        """Parse response with composite bookmarking and initial sync filtering."""
        if not self.era_based_incremental:
            yield from extract_jsonpath(self.records_path, input=response.json())
            return

        bookmark = self.get_starting_bookmark(None)
        last_era = bookmark.get("last_era_id") if bookmark else None
        seen_in_last_era = set(bookmark.get("processed_in_last_era", [])) if bookmark else set()
        
        for record in extract_jsonpath(self.records_path, input=response.json()):
            era_value = record.get(self.era_field)
            if era_value is None:
                yield record  # Yield records without an era field
                continue

            # --- Filtering Logic ---
            # 1. Initial Sync Filter: Skip records older than the starting point on a full refresh
            if not last_era and self.initial_sync_era_id and era_value < self.initial_sync_era_id:
                continue

            # 2. Incremental Sync Filter: Stop when we reach eras older than our bookmark
            if last_era and era_value < last_era:
                self.logger.info(f"Reached era {era_value}, which is older than the last bookmarked era {last_era}. Halting pagination.")
                if hasattr(self._paginator, "stop_pagination"):
                    self._paginator.stop_pagination = True
                return # Stop processing this page

            # 3. Incremental Sync Filter: Skip records within the last-seen era that were already processed
            if last_era and era_value == last_era:
                record_id = (
                    f'{record.get("validator_public_key")}_'
                    f'{record.get("timestamp")}_{era_value}'
                )
                if record_id in seen_in_last_era:
                    continue # Skip already processed record
            
            yield record

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Process records and build the composite bookmark for the current run."""
        if self.era_based_incremental and self.era_field in row:
            era = row[self.era_field]
            ts = row.get("timestamp")
            validator = row.get("validator_public_key")
            
            # Create a unique identifier for the record
            record_id = f"{validator}_{ts}_{era}"

            current_bookmark_era = self._run_state_bookmark.get("last_era_id")

            if not current_bookmark_era or era > current_bookmark_era:
                # This is the first record of the run OR the first record of a newer era
                self._run_state_bookmark["last_era_id"] = era
                self._run_state_bookmark["processed_in_last_era"] = {record_id}
            elif era == current_bookmark_era:
                # Still in the same era, add this record's ID to the set
                self._run_state_bookmark["processed_in_last_era"].add(record_id)

        return flatten_json(row, self.except_keys, self.store_raw_json_message)

    def _after_sync(self, context: Optional[dict]) -> None:
        """Write the composite bookmark to the state file at the end of a sync."""
        if not self.era_based_incremental or not self._run_state_bookmark:
            return

        # Convert set to a sorted list for deterministic STATE messages
        processed_set = self._run_state_bookmark.get("processed_in_last_era", set())
        self._run_state_bookmark["processed_in_last_era"] = sorted(list(processed_set))
        
        self.logger.info(f"Writing final bookmark to state: {self._run_state_bookmark}")
        # Use _write_state_message to emit a custom bookmark structure
        self._write_state_message({"bookmarks": {self.name: self._run_state_bookmark}})
        
