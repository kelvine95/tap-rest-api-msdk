"""Stream type classes for tap-rest-api-msdk with iteration support."""

import email.utils
import json
from datetime import datetime
from string import Template
from typing import Any, Dict, Generator, Iterable, Optional, Union, List
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


class IterativeDynamicStream(RestApiStream):
    """Dynamic stream with iteration support for multiple values."""

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
        # New iteration parameters
        iteration_config: Optional[dict] = None,
    ) -> None:
        """Initialize stream with iteration support.
        
        Args:
            iteration_config: Configuration for iteration, containing:
                - iteration_type: Type of iteration ('usernames', 'keywords', 'custom')
                - values: List of values to iterate over
                - api_param_key: The API parameter key to modify (e.g., 'query', 'userName')
                - api_param_template: Template for the parameter value (e.g., 'from:{value}')
                - path_template: Optional template for path modification
                - metadata_key: Key to store the iteration value in records (default: '_source_{iteration_type}')
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
        
        # Iteration configuration
        self.iteration_config = iteration_config or {}
        self.iteration_values = self.iteration_config.get('values', [None])
        self.iteration_type = self.iteration_config.get('iteration_type', 'none')
        self.api_param_key = self.iteration_config.get('api_param_key')
        self.api_param_template = self.iteration_config.get('api_param_template', '{value}')
        self.path_template = self.iteration_config.get('path_template')
        self.metadata_key = self.iteration_config.get('metadata_key', f'_source_{self.iteration_type}')
        
        # Store original path and params for iteration
        self.original_path = path
        self.original_params = params.copy() if params else {}

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
        self.pagination_page_size = pagination_page_size
        self.pagination_initial_offset = pagination_initial_offset
        self.offset_records_jsonpath = offset_records_jsonpath

        # Set pagination limits
        if self.pagination_request_style == "restapi_header_link_paginator":
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                page_limit_param = self.pagination_limit_per_page_param or "per_page"
                self.pagination_page_size = int(self.params.get(page_limit_param, 25))
        elif self.pagination_request_style in ["style1", "offset_paginator"]:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                page_limit_param = self.pagination_limit_per_page_param or "limit"
                self.pagination_page_size = int(self.params.get(page_limit_param, 25))
        else:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit
            self.pagination_page_size = pagination_page_size

        self.use_fake_since_parameter = False
        
        # Track current iteration value
        self.current_iteration_value = None

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Get records for all iteration values.
        
        This method iterates over all configured values (usernames, keywords, etc.)
        and yields records from each API call.
        """
        if not self.iteration_values or self.iteration_values == [None]:
            # No iteration configured, use original behavior
            yield from super().get_records(context)
        else:
            # Iterate over all configured values
            for value in self.iteration_values:
                self.current_iteration_value = value
                self.logger.info(f"Processing {self.iteration_type}: {value}")
                
                # Update path and params for this iteration
                self._update_request_for_iteration(value)
                
                # Get records for this specific value
                try:
                    for record in self._get_records_for_value(context, value):
                        yield record
                except Exception as e:
                    self.logger.error(f"Error processing {self.iteration_type} '{value}': {e}")
                    if self.iteration_config.get('continue_on_error', True):
                        continue
                    else:
                        raise
                
                # Reset for next iteration
                self._reset_after_iteration()

    def _update_request_for_iteration(self, value: str) -> None:
        """Update request parameters for current iteration value."""
        # Reset to original values
        self.path = self.original_path
        self.params = self.original_params.copy()
        
        # Apply path template if provided
        if self.path_template:
            self.path = self.path_template.format(value=value)
        
        # Apply parameter template
        if self.api_param_key:
            param_value = self.api_param_template.format(value=value)
            
            # Handle special cases for complex queries
            if self.api_param_key == 'query' and self.iteration_type == 'usernames':
                # For Twitter timeline queries
                existing_query = self.params.get('query', '')
                if existing_query:
                    # Merge with existing query
                    self.params['query'] = f"{param_value} {existing_query}"
                else:
                    self.params['query'] = param_value
            else:
                self.params[self.api_param_key] = param_value

    def _reset_after_iteration(self) -> None:
        """Reset stream state after processing an iteration value."""
        self.path = self.original_path
        self.params = self.original_params.copy()
        self.current_iteration_value = None

    def _get_records_for_value(self, context: Optional[dict], value: str) -> Iterable[dict]:
        """Get records for a specific iteration value using parent's logic."""
        # Use the parent class's get_records method for actual API calls
        # This ensures pagination and other features work correctly
        for record in super().get_records(context):
            yield record

    def post_process(
        self,
        row: types.Record,
        context: Optional[types.Context] = None,
    ) -> Optional[dict]:
        """Add iteration metadata to records after processing."""
        # First apply parent processing (flattening, etc.)
        processed = flatten_json(row, self.except_keys, self.store_raw_json_message)
        
        # Add iteration metadata if we're iterating
        if self.current_iteration_value and self.iteration_type != 'none':
            processed[self.metadata_key] = self.current_iteration_value
            processed['_source_kind'] = self.iteration_type
            
            # Add additional metadata if configured
            if self.iteration_config.get('add_extracted_at', True):
                processed['_extracted_at'] = datetime.utcnow().isoformat()
        
        return processed

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

    def backoff_wait_generator(self) -> Generator[Union[int, float], None, None]:
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
        """Return the appropriate paginator for the stream."""
        self.logger.info(f"Using paginator: {self.pagination_request_style}")

        if self.pagination_request_style in ["jsonpath_paginator", "default"]:
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
        elif self.pagination_request_style in ["style1", "offset_paginator"]:
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
            raise ValueError(f"Unknown paginator {self.pagination_request_style}")

    def _get_url_params_page_style(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return URL parameters for page-style pagination."""
        last_run_date = get_start_date(self, context)
        params: dict = {}
        
        if self.params:
            for k, v in self.params.items():
                params[k] = v
                
        if next_page_token:
            next_page_parm = self.pagination_next_page_param or "page"
            params[next_page_parm] = next_page_token
            
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
        """Return URL parameters for offset-style pagination."""
        last_run_date = get_start_date(self, context)
        params: dict = {}

        if self.params:
            for k, v in self.params.items():
                params[k] = v
                
        if next_page_token:
            next_page_parm = self.pagination_next_page_param or "offset"
            params[next_page_parm] = next_page_token
            
        if self.pagination_page_size is not None:
            limit_per_page_param = self.pagination_limit_per_page_param or "limit"
            params[limit_per_page_param] = self.pagination_page_size
            
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

    def _get_url_params_header_link(
        self, context: Optional[Dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return URL parameters for header-link pagination."""
        params: dict = {}
        
        if self.params:
            for k, v in self.params.items():
                params[k] = v
                
        pagination_page_size = self.pagination_page_size or 25
        limit_per_page_param = self.pagination_limit_per_page_param or "per_page"
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
        """Return URL parameters for HATEOAS-style pagination."""
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
        elif self.replication_key:
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
        yield from extract_jsonpath(self.records_path, input=response.json())


# Keep original DynamicStream for backward compatibility
DynamicStream = IterativeDynamicStream
