"""Stream type classes for tap-rest-api-msdk with era-based incremental support."""

import email.utils
import json
import logging
import time
from datetime import datetime
from string import Template
from typing import Any, Dict, Generator, Iterable, Optional, Union
from urllib.parse import parse_qs, parse_qsl, urlparse

import requests
from singer_sdk.helpers import types
from singer_sdk.helpers.jsonpath import extract_jsonpath
# Import added for the fix
from singer_sdk.messages import StateMessage, write_message
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


class EraBasedPageNumberPaginator(RestAPIBasePageNumberPaginator):
    """Custom paginator that tracks era_id for incremental sync."""

    def __init__(self, *args, era_field=None, max_pages_per_run=50, logger=None, **kwargs):
        """Initialize the paginator."""
        super().__init__(*args, **kwargs)
        self.era_field = era_field
        self.max_pages_per_run = max_pages_per_run
        self.pages_fetched = 0
        self.stop_pagination = False
        self.highest_era_seen = None
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def has_more(self, response: requests.Response) -> bool:
        """Check if more pages exist and if we should continue."""
        # Increment the page count
        self.pages_fetched += 1
        
        # Check if we should stop due to reaching known data
        if self.stop_pagination:
            self.logger.info("Stopping pagination - reached known era")
            return False
        
        # Check if we've hit the page limit for this run
        if self.pages_fetched >= self.max_pages_per_run:
            self.logger.info(f"Reached page limit ({self.max_pages_per_run} pages)")
            return False

        # Check the API's pagination info
        response_data = response.json()
        total_pages = response_data.get("page_count") or response_data.get("pageCount")
        
        if total_pages is None:
            self.logger.warning("No page count in response, assuming no more pages")
            return False
        
        # Check if there are more pages available
        has_more_pages = self.current_value <= total_pages
        has_records = bool(response_data.get("data"))
        
        if not has_more_pages:
            self.logger.info(f"No more pages (current: {self.current_value}, total: {total_pages})")
        
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
        # Era-based parameters
        era_based_incremental: Optional[bool] = False,
        era_field: Optional[str] = None,
        max_pages_per_run: Optional[int] = 50,
        initial_sync_era_id: Optional[int] = None,
        rate_limit_delay: Optional[float] = 0.7,
    ) -> None:
        """Initialize the stream with era-based incremental support."""
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
        self.initial_sync_era_id = initial_sync_era_id
        self.rate_limit_delay = rate_limit_delay
        
        # Composite bookmark tracking
        self._composite_bookmark: Dict[str, Any] = {}
        self._seen_in_current_run: set = set()

        # Pagination configuration
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
                page_limit_param = (
                    self.pagination_limit_per_page_param or "per_page"
                )
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
                page_limit_param = (
                    self.pagination_limit_per_page_param or "limit"
                )
                self.pagination_page_size = int(
                    self.params.get(page_limit_param, 25)
                )
        else:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit
            self.pagination_page_size = pagination_page_size

        self.use_fake_since_parameter = False

    def get_starting_composite_bookmark(self, context: Optional[dict]) -> Optional[dict]:
        """Get the composite bookmark from state."""
        if not self.era_based_incremental:
            return None
        
        # Access the tap's state properly
        state = self.get_context_state(context) or {}
        
        # Look for composite bookmark in stream metadata
        stream_state = state.get("stream_states", {}).get(self.name, {})
        composite_bookmark = stream_state.get("composite_bookmark")
        
        if composite_bookmark:
            self.logger.info(f"Found composite bookmark: {composite_bookmark}")
            return composite_bookmark
        
        # Fall back to simple replication key value if available
        if self.replication_key and stream_state.get("replication_key_value"):
            return {
                "last_era_id": stream_state.get("replication_key_value"),
                "processed_in_last_era": []
            }
        
        return None

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        """Perform a request with rate limiting."""
        # Apply rate limit delay BEFORE the request to ensure proper spacing
        if self.rate_limit_delay and self.rate_limit_delay > 0:
            self.logger.debug(f"Applying rate limit delay of {self.rate_limit_delay}s")
            time.sleep(self.rate_limit_delay)
        
        # Execute the request
        response = super()._request(prepared_request, context)
        
        return response

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
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
        """Return a backoff generator for rate-limited APIs."""
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
        """Return the appropriate paginator."""
        # Use custom era-based paginator if configured
        if self.era_based_incremental and self.pagination_request_style == "page_number_paginator":
            return EraBasedPageNumberPaginator(
                start_value=self.pagination_initial_offset,
                jsonpath=getattr(self, 'next_page_token_jsonpath', None),
                era_field=self.era_field,
                max_pages_per_run=self.max_pages_per_run,
                logger=self.logger
            )
        
        # Default paginators
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
            msg = f"Unknown paginator {self.pagination_request_style}"
            self.logger.error(msg)
            raise ValueError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse response with era-based filtering and deduplication."""
        if not self.era_based_incremental:
            # Standard parsing for non-era streams
            yield from extract_jsonpath(self.records_path, input=response.json())
            return

        # Get the composite bookmark from state
        bookmark = self.get_starting_composite_bookmark(None)
        last_era = bookmark.get("last_era_id") if bookmark else None
        processed_in_last_era = set(bookmark.get("processed_in_last_era", [])) if bookmark else set()
        
        # Track if we should stop pagination
        should_stop = False
        
        for record in extract_jsonpath(self.records_path, input=response.json()):
            era_value = record.get(self.era_field)
            
            # Skip records without era field
            if era_value is None:
                self.logger.warning(f"Record missing era field '{self.era_field}': {record}")
                continue
            
            # Filter 1: Initial sync - skip records older than starting point
            if not last_era and self.initial_sync_era_id and era_value < self.initial_sync_era_id:
                self.logger.debug(f"Skipping era {era_value} < initial_sync_era_id {self.initial_sync_era_id}")
                continue
            
            # Filter 2: Incremental sync - stop when reaching older eras
            if last_era and era_value < last_era:
                self.logger.info(f"Reached era {era_value} < last_era {last_era}, stopping pagination")
                should_stop = True
                break
            
            # Filter 3: Deduplicate within the last processed era
            if last_era and era_value == last_era:
                # Create unique record identifier
                record_id = self._get_record_id(record, era_value)
                if record_id in processed_in_last_era:
                    self.logger.debug(f"Skipping duplicate record: {record_id}")
                    continue
            
            # Track this record for the current run
            self._track_record(record, era_value)
            
            yield record
        
        # Signal paginator to stop if needed
        if should_stop and hasattr(self._paginator, 'stop_pagination'):
            self._paginator.stop_pagination = True

    def _get_record_id(self, record: dict, era_value: Any) -> str:
        """Generate unique identifier for a record."""
        validator = record.get("validator_public_key", "")
        timestamp = record.get("timestamp", "")
        return f"{validator}_{timestamp}_{era_value}"

    def _track_record(self, record: dict, era_value: Any) -> None:
        """Track record in the current run's composite bookmark."""
        record_id = self._get_record_id(record, era_value)
        
        # Update composite bookmark
        if not self._composite_bookmark or era_value > self._composite_bookmark.get("last_era_id", -1):
            # New highest era
            self._composite_bookmark = {
                "last_era_id": era_value,
                "processed_in_last_era": {record_id}
            }
        elif era_value == self._composite_bookmark.get("last_era_id"):
            # Same era, add to set
            self._composite_bookmark["processed_in_last_era"].add(record_id)

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Process and flatten records."""
        # Update standard replication key value for SDK compatibility
        if self.era_based_incremental and self.replication_key and self.era_field in row:
            # This ensures the SDK tracks the era_id properly
            self._increment_stream_state(
                {self.replication_key: row[self.era_field]}, 
                context=context
            )
        
        # Flatten the record
        return flatten_json(row, self.except_keys, self.store_raw_json_message)

    def _sync_records(
        self, context: Optional[dict] = None
    ) -> Generator[dict, None, None]:
        """Sync records and manage composite bookmark state."""
        # Reset tracking for new sync run
        self._composite_bookmark = {}
        
        # Call parent sync_records
        yield from super()._sync_records(context=context)
        
        # Save composite bookmark after sync
        if self.era_based_incremental and self._composite_bookmark:
            self._write_composite_bookmark(context)

    def _write_composite_bookmark(self, context: Optional[dict]) -> None:
        """Write the composite bookmark to state."""
        if not self._composite_bookmark:
            return
        
        # Convert set to sorted list for consistent state
        processed_set = self._composite_bookmark.get("processed_in_last_era", set())
        self._composite_bookmark["processed_in_last_era"] = sorted(list(processed_set))
        
        self.logger.info(f"Writing composite bookmark: {self._composite_bookmark}")
        
        # Get current state and update it
        state = self.get_context_state(context) or {}
        
        # Ensure stream_states exists
        if "stream_states" not in state:
            state["stream_states"] = {}
        
        if self.name not in state["stream_states"]:
            state["stream_states"][self.name] = {}
        
        # Store composite bookmark
        state["stream_states"][self.name]["composite_bookmark"] = self._composite_bookmark
        
        # Also update the simple replication_key_value for compatibility
        if self.replication_key:
            state["stream_states"][self.name]["replication_key_value"] = (
                self._composite_bookmark.get("last_era_id")
            )
        
        # FIX: Write the state using the modern SDK method
        write_message(StateMessage(value=state))

    def _get_url_params_page_style(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return URL parameters for page-style pagination."""
        params: dict = {}
        
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        
        if next_page_token:
            next_page_param = self.pagination_next_page_param or "page"
            params[next_page_param] = next_page_token
        
        # Don't add date filtering for era-based incremental
        if not self.era_based_incremental and self.replication_key:
            last_run_date = get_start_date(self, context)
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
        params: dict = {}
        
        if self.params:
            for k, v in self.params.items():
                params[k] = v
        
        if next_page_token:
            next_page_param = self.pagination_next_page_param or "offset"
            params[next_page_param] = next_page_token
        
        if self.pagination_page_size is not None:
            limit_param = self.pagination_limit_per_page_param or "limit"
            params[limit_param] = self.pagination_page_size
        
        # Don't add date filtering for era-based incremental
        if not self.era_based_incremental and self.replication_key:
            last_run_date = get_start_date(self, context)
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
        
        pagination_page_size = self.pagination_page_size or 25
        limit_param = self.pagination_limit_per_page_param or "per_page"
        params[limit_param] = pagination_page_size
        
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
                f"The replication key '{self.replication_key}' is not fully supported"
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
        elif not self.era_based_incremental and self.replication_key:
            last_run_date = get_start_date(self, context)
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
    