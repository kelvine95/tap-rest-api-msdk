# streams.py
"""Stream class with iteration, partitioned state, incremental time filters and run caps."""

import email.utils
import json
from datetime import datetime, timezone
from string import Template
from typing import Any, Dict, Generator, Iterable, Optional, Union, List
from urllib.parse import parse_qs, parse_qsl, urlparse

import requests
from dateutil import parser as dtparse
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
    """Dynamic stream with iteration support and strict cost controls."""

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
        iteration_config: Optional[dict] = None,
    ) -> None:
        super().__init__(tap=tap, name=tap.name, schema=schema)

        self.name = name
        self.path = path
        self.params = params.copy() if params else {}
        self.headers = headers
        self.assigned_authenticator = authenticator
        self._authenticator = authenticator
        self.primary_keys = primary_keys or []
        self.replication_key = replication_key
        self.except_keys = except_keys
        self.records_path = records_path

        # Iteration
        self.iteration_config = iteration_config or {}
        self.iteration_values = self.iteration_config.get("values", [None])
        self.iteration_type = self.iteration_config.get("iteration_type", "none")
        self.api_param_key = self.iteration_config.get("api_param_key")
        self.api_param_template = self.iteration_config.get("api_param_template", "{value}")
        self.path_template = self.iteration_config.get("path_template")
        self.metadata_key = self.iteration_config.get("metadata_key", f"_source_{self.iteration_type}")

        self.original_path = path
        self.original_params = self.params.copy()

        # Next page token path
        if next_page_token_path:
            self.next_page_token_jsonpath = next_page_token_path
        elif (pagination_request_style in ("jsonpath_paginator", "default")):
            self.next_page_token_jsonpath = "$.next_page"

        # Pagination styles
        self.use_request_body_not_params = use_request_body_not_params
        self.backoff_type = backoff_type
        self.backoff_param = backoff_param
        self.backoff_time_extension = backoff_time_extension
        self.store_raw_json_message = store_raw_json_message

        get_url_params_styles = {
            "style1": self._get_url_params_offset_style,
            "offset": self._get_url_params_offset_style,
            "page": self._get_url_params_page_style,
            "header_link": self._get_url_params_header_link,
            "hateoas_body": self._get_url_params_hateoas_body,
        }

        if self.use_request_body_not_params:
            self.prepare_request_payload = get_url_params_styles.get(pagination_response_style, self._get_url_params_page_style)
        else:
            self.get_url_params = get_url_params_styles.get(pagination_response_style, self._get_url_params_page_style)

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

        # Page size & caps
        if self.pagination_request_style == "restapi_header_link_paginator":
            self.pagination_page_size = pagination_page_size or int(self.params.get(self.pagination_limit_per_page_param or "per_page", 25))
        elif self.pagination_request_style in ["style1", "offset_paginator"]:
            if pagination_page_size:
                self.pagination_page_size = pagination_page_size
            else:
                self.pagination_page_size = int(self.params.get(self.pagination_limit_per_page_param or "limit", 25))
        else:
            self.pagination_page_size = pagination_page_size

        # Global/stream run caps for cost control
        self.max_records_per_stream = self.config.get("max_records_per_stream")
        self.max_records_total = self.config.get("max_records_total")
        self._emitted_in_stream = 0
        if not hasattr(self.tap, "_emitted_total"):
            self.tap._emitted_total = 0

        # Identify endpoints
        p = (self.original_path or "").strip()
        self._is_advanced_search = p == "/twitter/tweet/advanced_search"
        self._is_mentions = p == "/twitter/user/mentions"
        self._is_last_tweets = p == "/twitter/user/last_tweets"

        # Track current iteration
        self.current_iteration_value = None

        # In-run seen IDs cache (cheap dedupe safeguard)
        self._seen_ids_this_run: set[str] = set()

    # ---------- helpers ----------
    def _ctx(self, value: str) -> dict:
        # Partitioned state keys → per iteration value bookmark
        return {"iteration_type": self.iteration_type, "iteration_value": value}

    def _iso_for_query(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    def _unix_secs(self, dt: datetime) -> int:
        return int(dt.replace(tzinfo=timezone.utc).timestamp())

    def _created_at_to_dt(self, record: dict) -> Optional[datetime]:
        val = record.get("createdAt")
        if not val:
            return None
        try:
            return dtparse.parse(val)
        except Exception:
            return None

    def _stop_for_caps(self) -> bool:
        if self.max_records_total is not None and self.tap._emitted_total >= self.max_records_total:
            return True
        if self.max_records_per_stream is not None and self._emitted_in_stream >= self.max_records_per_stream:
            return True
        return False

    # ---------- iteration entry ----------
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if not self.iteration_values or self.iteration_values == [None]:
            yield from super().get_records(context)
            return

        for value in self.iteration_values:
            if self._stop_for_caps():
                break

            iter_ctx = self._ctx(value)
            self.current_iteration_value = value
            self.logger.info(f"Processing {self.iteration_type}: {value}")

            # Update request per iteration
            self._update_request_for_iteration(value)

            # Inject incremental filters from bookmark BEFORE requesting first page
            last_dt = self.get_starting_timestamp(iter_ctx)
            if last_dt:
                if self._is_advanced_search:
                    base_q = self.params.get("query", "")
                    since_q = f"since:{self._iso_for_query(last_dt)}"
                    self.params["query"] = (base_q + " " + since_q).strip()
                elif self._is_mentions:
                    self.params["sinceTime"] = str(self._unix_secs(last_dt))
                # last_tweets: no param → we will early-stop below

            stop_iteration = False
            try:
                for record in super().get_records(iter_ctx):
                    if self._stop_for_caps():
                        stop_iteration = True
                        break

                    # Early-stop for last_tweets when we reach <= bookmark
                    if self._is_last_tweets and last_dt:
                        rec_dt = self._created_at_to_dt(record)
                        if rec_dt and rec_dt <= last_dt:
                            stop_iteration = True
                            break

                    # In-run dedupe safeguard by tweet id
                    rid = record.get("id")
                    if rid and rid in self._seen_ids_this_run:
                        continue
                    if rid:
                        self._seen_ids_this_run.add(rid)

                    yield record
            except Exception as e:
                self.logger.error(f"Error processing {self.iteration_type} '{value}': {e}")
                if not self.iteration_config.get("continue_on_error", True):
                    raise

            # Reset & go next
            self._reset_after_iteration()
            if stop_iteration:
                continue

    def _update_request_for_iteration(self, value: str) -> None:
        self.path = self.original_path
        self.params = self.original_params.copy()

        if self.path_template:
            try:
                self.path = self.path_template.format(value=value)
            except Exception:
                pass

        if self.api_param_key:
            param_value = self.api_param_template.format(value=value)
            if self.api_param_key == "query" and self.iteration_type == "usernames":
                existing = self.params.get("query", "")
                self.params["query"] = f"{param_value} {existing}".strip() if existing else param_value
            else:
                self.params[self.api_param_key] = param_value

    def _reset_after_iteration(self) -> None:
        self.path = self.original_path
        self.params = self.original_params.copy()
        self.current_iteration_value = None

    # ---------- base request plumbing ----------
    def post_process(self, row: types.Record, context: Optional[types.Context] = None) -> Optional[dict]:
        processed = flatten_json(row, self.except_keys, self.store_raw_json_message)

        if context:
            processed["_source_kind"] = context.get("iteration_type")
            processed[self.metadata_key] = context.get("iteration_value")
        if self.iteration_config.get("add_extracted_at", True):
            processed["_extracted_at"] = datetime.utcnow().isoformat()

        self._emitted_in_stream += 1
        self.tap._emitted_total += 1

        return processed

    @property
    def http_headers(self) -> dict:
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        if self.headers:
            headers.update(self.headers)
        return headers

    def backoff_wait_generator(self) -> Generator[Union[int, float], None, None]:
        def _backoff_from_headers(exception):
            return int(exception.response.headers.get(self.backoff_param, 0)) + self.backoff_time_extension

        def _get_wait_time_from_response(exception):
            response_message = exception.response.json().get("message", 0)
            res = [int(i) for i in response_message.split() if i.isdigit()]
            return int(max(res)) + self.backoff_time_extension

        if self.backoff_type == "message":
            return self.backoff_runtime(value=_get_wait_time_from_response)
        elif self.backoff_type == "header":
            return self.backoff_runtime(value=_backoff_from_headers)
        return super().backoff_wait_generator()

    def get_new_paginator(self):
        self.logger.info(f"Using paginator: {self.pagination_request_style}")

        if self.pagination_request_style in ["jsonpath_paginator", "default"]:
            return JSONPathPaginator(self.next_page_token_jsonpath)
        if self.pagination_request_style == "simple_header_paginator":
            return JSONPathPaginator(self.next_page_token_jsonpath) if self.next_page_token_jsonpath else SimpleHeaderPaginator("X-Next-Page")
        if self.pagination_request_style == "header_link_paginator":
            return HeaderLinkPaginator()
        if self.pagination_request_style == "restapi_header_link_paginator":
            return RestAPIHeaderLinkPaginator(
                pagination_page_size=self.pagination_page_size or 25,
                pagination_results_limit=self.pagination_results_limit,
                replication_key=self.replication_key,
            )
        if self.pagination_request_style in ["style1", "offset_paginator"]:
            return RestAPIOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size or 25,
                jsonpath=self.next_page_token_jsonpath,
                pagination_total_limit_param=self.pagination_total_limit_param or "total",
            )
        if self.pagination_request_style == "hateoas_paginator":
            return BaseHATEOASPaginator()
        if self.pagination_request_style == "single_page_paginator":
            return SinglePagePaginator()
        if self.pagination_request_style == "page_number_paginator":
            return RestAPIBasePageNumberPaginator(
                start_value=self.pagination_initial_offset,
                jsonpath=self.next_page_token_jsonpath,
            )
        if self.pagination_request_style == "simple_offset_paginator":
            return SimpleOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size or 25,
                offset_records_jsonpath=self.offset_records_jsonpath,
                pagination_page_size=self.pagination_page_size or 25,
            )
        raise ValueError(f"Unknown paginator {self.pagination_request_style}")

    # ---------- URL param builders (respect partitioned state) ----------
    def _get_url_params_page_style(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: dict = {**self.params} if self.params else {}

        if next_page_token:
            params[self.pagination_next_page_param or "page"] = next_page_token

        if self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                qtpl = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(qtpl.substitute(last_run_date=last_run_date))
                else:
                    params[self.source_search_field] = qtpl.substitute(last_run_date=last_run_date)
            else:
                params["sort"] = "asc"
                params["order_by"] = self.replication_key
        return params

    def _get_url_params_offset_style(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: dict = {**self.params} if self.params else {}

        if next_page_token:
            params[self.pagination_next_page_param or "offset"] = next_page_token

        if self.pagination_page_size is not None:
            params[self.pagination_limit_per_page_param or "limit"] = self.pagination_page_size

        if self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                qtpl = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(qtpl.substitute(last_run_date=last_run_date))
                else:
                    params[self.source_search_field] = qtpl.substitute(last_run_date=last_run_date)
            else:
                params["sort"] = "asc"
                params["order_by"] = self.replication_key
        return params

    def _get_url_params_header_link(self, context: Optional[Dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        params: dict = {**self.params} if self.params else {}
        pagination_page_size = self.pagination_page_size or 25
        params[self.pagination_limit_per_page_param or "per_page"] = pagination_page_size

        if next_page_token:
            request_parameters = parse_qs(str(next_page_token))
            for k, v in request_parameters.items():
                params[k] = v

        if self.replication_key == "updated_at":
            params["sort"] = "updated"
            params["direction"] = "asc"
        elif self.replication_key in ["starred_at","created_at"]:
            params["sort"] = "created"
            params["direction"] = "desc"
        elif self.replication_key == "commit_timestamp":
            params["direction"] = "desc"
        return params

    def _get_url_params_hateoas_body(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: dict = {**self.params} if self.params else {}

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
        elif self.replication_key and self.source_search_field and self.source_search_query and last_run_date:
            qtpl = Template(self.source_search_query)
            if self.use_request_body_not_params:
                params[self.source_search_field] = json.loads(qtpl.substitute(last_run_date=last_run_date))
            else:
                params[self.source_search_field] = qtpl.substitute(last_run_date=last_run_date)
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_path, input=response.json())
