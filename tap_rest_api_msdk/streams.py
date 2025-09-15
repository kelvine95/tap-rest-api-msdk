"""Stream type classes for tap-rest-api-msdk (generic, Twitter-compatible)."""

from __future__ import annotations

import json
from datetime import datetime
from string import Template
from typing import Any, Dict, Generator, Iterable, Optional, Union

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
from singer_sdk.helpers._typing import TypeConformanceLevel

from tap_rest_api_msdk.client import RestApiStream
from tap_rest_api_msdk.pagination import (
    RestAPIBasePageNumberPaginator,
    RestAPIHeaderLinkPaginator,
    RestAPIOffsetPaginator,
    SimpleOffsetPaginator,
)
from tap_rest_api_msdk.utils import flatten_json, get_start_date


class DynamicStream(RestApiStream):
    """Config-driven, API-agnostic stream."""

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

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
        # New / optional:
        replication_request_adapter: Optional[dict] = None,
        disable_discovery_probe: bool = False,
    ) -> None:
        super().__init__(tap=tap, name=name, schema=schema or {"type": "object", "properties": {}})
        self.path = path
        self.params = dict(params or {})
        self.headers = dict(headers or {})
        self.assigned_authenticator = authenticator
        self._authenticator = authenticator
        self.primary_keys = primary_keys or []
        self.replication_key = replication_key
        self.except_keys = except_keys or []
        self.records_path = records_path

        self.next_page_token_jsonpath = next_page_token_path or (
            "$.next_page" if pagination_request_style in ("jsonpath_paginator", "default") else None
        )

        # param sending route
        self.use_request_body_not_params = bool(use_request_body_not_params)

        # backoff tuning (SDK honors our generator)
        self.backoff_type = backoff_type
        self.backoff_param = backoff_param
        self.backoff_time_extension = int(backoff_time_extension or 0)

        self.store_raw_json_message = bool(store_raw_json_message)

        self.pagination_request_style = pagination_request_style
        self.pagination_response_style = pagination_response_style
        self.pagination_results_limit = pagination_results_limit
        self.pagination_next_page_param = pagination_next_page_param
        self.pagination_limit_per_page_param = pagination_limit_per_page_param
        self.pagination_total_limit_param = pagination_total_limit_param
        self.pagination_initial_offset = int(pagination_initial_offset or 1)
        self.offset_records_jsonpath = offset_records_jsonpath

        self.start_date = start_date
        self.source_search_field = source_search_field
        self.source_search_query = source_search_query

        # max emission budgets (soft stop)
        cfg = tap.config or {}
        self._max_per_stream = int(cfg.get("max_records_per_stream") or 0) or None
        self._max_total = int(cfg.get("max_records_total") or 0) or None
        self._emitted_total = 0

        # adapter to translate bookmarks into request params (Twitter or others)
        self.replication_request_adapter = replication_request_adapter or {}
        self.disable_discovery_probe = bool(disable_discovery_probe)

        # Page size defaulting (do not throw if missing)
        if pagination_request_style == "restapi_header_link_paginator":
            # infer from params or default
            limit_key = pagination_limit_per_page_param or "per_page"
            self.pagination_page_size = int(self.params.get(limit_key, pagination_page_size or 25))
        elif pagination_request_style in ("style1", "offset_paginator"):
            limit_key = pagination_limit_per_page_param or "limit"
            self.pagination_page_size = int(self.params.get(limit_key, pagination_page_size or 25))
        else:
            self.pagination_page_size = pagination_page_size

    # ---------- Headers ----------
    @property
    def http_headers(self) -> dict:
        hdrs = {}
        if "user_agent" in self.config:
            hdrs["User-Agent"] = self.config.get("user_agent")
        hdrs.update(self.headers or {})
        return hdrs

    # ---------- Backoff ----------
    def backoff_wait_generator(self) -> Generator[Union[int, float], None, None]:
        # Optional override: compute retry after from header or JSON message
        def _backoff_from_headers(exc):
            return int(exc.response.headers.get(self.backoff_param, 0)) + self.backoff_time_extension

        def _backoff_from_message(exc):
            try:
                msg = (exc.response.json() or {}).get("message") or ""
            except Exception:
                msg = exc.response.text or ""
            nums = [int(x) for x in msg.split() if x.isdigit()]
            return (max(nums) if nums else 0) + self.backoff_time_extension

        if self.backoff_type == "header":
            return self.backoff_runtime(value=_backoff_from_headers)
        if self.backoff_type == "message":
            return self.backoff_runtime(value=_backoff_from_message)
        return super().backoff_wait_generator()

    # ---------- Pagination ----------
    def get_new_paginator(self):
        if self.pagination_request_style in ("jsonpath_paginator", "default"):
            return JSONPathPaginator(self.next_page_token_jsonpath or "$.next_page")
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
        if self.pagination_request_style in ("style1", "offset_paginator"):
            return RestAPIOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size,
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
        raise ValueError(f"Unknown paginator '{self.pagination_request_style}'.")

    # ---------- Param construction ----------
    def _apply_replication_adapter(self, params: Dict[str, Any], context: Optional[dict]) -> Dict[str, Any]:
        """Translate bookmarks into request params using a small, declarative adapter.

        Examples:
          {"mode":"param", "key":"sinceTime", "transform":"epoch_seconds"}
          {"mode":"add_query_suffix", "key":"query", "template":" since:${iso_utc}"}
        """
        if not self.replication_key:
            return params

        # figure out the bookmark
        since = self.get_starting_timestamp(context)  # returns datetime or None
        if not since:
            return params

        mode = (self.replication_request_adapter or {}).get("mode")
        if not mode:
            # default, reasonable generic behavior: sort asc + filter if template provided
            return params

        if mode == "param":
            key = self.replication_request_adapter.get("key")
            transform = self.replication_request_adapter.get("transform", "iso_utc")
            if key:
                if transform == "epoch_seconds":
                    params[key] = int(since.timestamp())
                elif transform == "iso_date":
                    params[key] = since.strftime("%Y-%m-%d")
                else:
                    # iso_utc default
                    params[key] = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            return params

        if mode == "add_query_suffix":
            key = self.replication_request_adapter.get("key", "query")
            template = self.replication_request_adapter.get("template", " since:${iso_utc}")
            iso_utc = since.strftime("%Y-%m-%d_%H:%M:%S_UTC")
            t = Template(template)
            suffix = t.safe_substitute(iso_utc=iso_utc, iso=iso_utc)
            base_q = params.get(key, "")
            params[key] = (base_q + suffix).strip()
            return params

        return params

    def _get_url_params_page_style(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: Dict[str, Any] = dict(self.params or {})
        if next_page_token:
            params[self.pagination_next_page_param or "page"] = next_page_token
        if self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                tmpl = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(tmpl.substitute(last_run_date=last_run_date))
                else:
                    params[self.source_search_field] = tmpl.substitute(last_run_date=last_run_date)
            else:
                params.setdefault("sort", "asc")
                params.setdefault("order_by", self.replication_key)
        return self._apply_replication_adapter(params, context)

    def _get_url_params_offset_style(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: Dict[str, Any] = dict(self.params or {})
        if next_page_token:
            params[self.pagination_next_page_param or "offset"] = next_page_token
        if self.pagination_page_size is not None:
            params[self.pagination_limit_per_page_param or "limit"] = self.pagination_page_size
        if self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                tmpl = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(tmpl.substitute(last_run_date=last_run_date))
                else:
                    params[self.source_search_field] = tmpl.substitute(last_run_date=last_run_date)
            else:
                params.setdefault("sort", "asc")
                params.setdefault("order_by", self.replication_key)
        return self._apply_replication_adapter(params, context)

    def _get_url_params_header_link(self, context: Optional[Dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = dict(self.params or {})
        size = (self.pagination_page_size or 25)
        params[self.pagination_limit_per_page_param or "per_page"] = size

        if next_page_token:
            # next_page_token from HeaderLinkPaginator is full query-string
            from urllib.parse import parse_qs
            for k, v in (parse_qs(str(next_page_token)) or {}).items():
                params[k] = v

        if self.replication_key:
            params.setdefault("sort", "asc")
        return self._apply_replication_adapter(params, context)

    def _get_url_params_hateoas_body(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: Dict[str, Any] = dict(self.params or {})
        if self.pagination_page_size and self.pagination_limit_per_page_param:
            params[self.pagination_limit_per_page_param] = self.pagination_page_size

        if next_page_token:
            from urllib.parse import urlparse, parse_qsl
            parsed = urlparse(next_page_token)
            if parsed.query:
                params.update(parse_qsl(parsed.query))
            else:
                params.update(parse_qsl(parsed.path))
            # update path if present in token
            if parsed.path and parsed.geturl() != parsed.path:
                self.path = parsed.path
        elif self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                tmpl = Template(self.source_search_query)
                if self.use_request_body_not_params:
                    params[self.source_search_field] = json.loads(tmpl.substitute(last_run_date=last_run_date))
                else:
                    params[self.source_search_field] = tmpl.substitute(last_run_date=last_run_date)
        return self._apply_replication_adapter(params, context)

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        dispatch = {
            "style1": self._get_url_params_offset_style,
            "offset": self._get_url_params_offset_style,
            "offset_paginator": self._get_url_params_offset_style,
            "page": self._get_url_params_page_style,
            "default": self._get_url_params_page_style,
            "header_link": self._get_url_params_header_link,
            "hateoas_body": self._get_url_params_hateoas_body,
        }
        fn = dispatch.get(self.pagination_response_style, self._get_url_params_page_style)
        return fn(context, next_page_token)

    # ---------- Response parsing & budgets ----------
    def _can_emit_more(self) -> bool:
        if self._max_per_stream is not None and self._emitted_total >= self._max_per_stream:
            return False
        g = getattr(self._tap, "_global_emitted_total", 0)
        if self._max_total is not None and g >= self._max_total:
            return False
        return True

    def _increment_emitted(self, n: int = 1) -> None:
        self._emitted_total += n
        prev = getattr(self._tap, "_global_emitted_total", 0)
        setattr(self._tap, "_global_emitted_total", prev + n)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield records from records_path, honoring budgets."""
        # If validate_response soft-failed, response may be 304/4xx; return nothing.
        if self.soft_fail_status_codes and response.status_code in self.soft_fail_status_codes:
            return []
        # Extract array/objects from JSONPath
        for rec in extract_jsonpath(self.records_path, input=response.json()):
            if not self._can_emit_more():
                break
            yield rec

    def post_process(
        self,
        row: types.Record,
        context: Optional[types.Context] = None,
    ) -> Optional[dict]:
        flat = flatten_json(row, self.except_keys, self.store_raw_json_message)
        # budget counting
        self._increment_emitted(1)
        return flat
