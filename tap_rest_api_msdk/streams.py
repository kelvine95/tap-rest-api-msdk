"""Generic REST stream with run budget, dupes guard, iteration, field pruning, and rich logging."""

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


class DynamicStream(RestApiStream):
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
        # NEW:
        iteration_config: Optional[dict] = None,
        keep_fields: Optional[list] = None,
        bookmark_target_param: Optional[str] = None,
        bookmark_query_template: Optional[str] = None,
    ) -> None:
        # IMPORTANT: stream name must be the stream's name (not tap name)
        super().__init__(tap=tap, name=name, schema=schema)

        self.name = name
        self.path = path
        self.params = params or {}
        self.headers = headers
        self.assigned_authenticator = authenticator
        self._authenticator = authenticator
        self.primary_keys = primary_keys or []
        self.replication_key = replication_key
        self.except_keys = except_keys
        self.records_path = records_path

        # iteration support
        self.iteration_config = iteration_config or {}
        self.keep_fields = keep_fields or []
        self.bookmark_target_param = bookmark_target_param
        self.bookmark_query_template = bookmark_query_template

        # page tokens
        if next_page_token_path:
            self.next_page_token_jsonpath = next_page_token_path
        elif pagination_request_style in {"jsonpath_paginator", "default"}:
            self.next_page_token_jsonpath = "$.next_page"

        # param strategy
        get_param_styles = {
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
            self.prepare_request_payload = get_param_styles.get(  # type: ignore
                pagination_response_style, self._get_url_params_page_style
            )
        else:
            self.get_url_params = get_param_styles.get(  # type: ignore
                pagination_response_style, self._get_url_params_page_style
            )

        # pagination config
        self.pagination_request_style = pagination_request_style
        self.pagination_results_limit = pagination_results_limit
        self.pagination_next_page_param = pagination_next_page_param
        self.pagination_limit_per_page_param = pagination_limit_per_page_param
        self.pagination_total_limit_param = pagination_total_limit_param
        self.start_date = start_date
        self.source_search_field = source_search_field
        self.source_search_query = source_search_query
        self.pagination_initial_offset = pagination_initial_offset
        self.offset_records_jsonpath = offset_records_jsonpath

        # compute page size
        if self.pagination_request_style == "restapi_header_link_paginator":
            page_limit_param = self.pagination_limit_per_page_param or "per_page"
            self.pagination_page_size = int((pagination_page_size or self.params.get(page_limit_param, 25)))
        elif self.pagination_request_style in {"style1", "offset_paginator"}:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit
            page_limit_param = self.pagination_limit_per_page_param or "limit"
            self.pagination_page_size = int((pagination_page_size or self.params.get(page_limit_param, 25)))
        else:
            if self.pagination_results_limit:
                self.ABORT_AT_RECORD_COUNT = self.pagination_results_limit
            self.pagination_page_size = pagination_page_size

        # GitHub-only behavior retained but disabled by default
        self.use_fake_since_parameter = False

        # run budget & duplicate guard
        self._max_total = int(self.config.get("max_records_total") or 5000)
        self._max_per_stream = int(self.config.get("max_records_per_stream") or 0) or None
        self._drop_on_bookmark_le = bool(self.config.get("drop_on_bookmark_le", True))
        self._log_body_preview = int(self.config.get("log_response_body_preview") or 1000)

        self._emitted_total = 0

    # ---------- Iteration: run once per value ----------
    def sync(self, context: Optional[dict] = None) -> None:
        it = self.iteration_config or {}
        values = it.get("values") or [None]
        for val in values:
            part_ctx = dict(context or {})
            part_ctx.update({"iteration_value": val, "iteration_type": it.get("iteration_type")})
            super().sync(context=part_ctx)

    # ---------- HTTP ----------
    @property
    def http_headers(self) -> dict:
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        if self.headers:
            headers.update(self.headers)
        return headers

    def backoff_wait_generator(self) -> Generator[Union[int, float], None, None]:
        def _header_delay(exc):
            return int(exc.response.headers.get(self.backoff_param, 0)) + self.backoff_time_extension

        def _message_delay(exc):
            msg = exc.response.json().get("message", "")
            nums = [int(i) for i in str(msg).split() if str(i).isdigit()]
            return (max(nums) if nums else 0) + self.backoff_time_extension

        if self.backoff_type == "message":
            return self.backoff_runtime(value=_message_delay)
        if self.backoff_type == "header":
            return self.backoff_runtime(value=_header_delay)
        return super().backoff_wait_generator()

    # ---------- Pagination ----------
    def get_new_paginator(self):
        self.logger.info(
            "Paginator=%s jsonpath=%s", self.pagination_request_style, getattr(self, "next_page_token_jsonpath", None)
        )
        prs = self.pagination_request_style
        if prs in {"jsonpath_paginator", "default"}:
            return JSONPathPaginator(self.next_page_token_jsonpath)
        if prs == "simple_header_paginator":
            return JSONPathPaginator(self.next_page_token_jsonpath) if self.next_page_token_jsonpath else SimpleHeaderPaginator("X-Next-Page")
        if prs == "header_link_paginator":
            return HeaderLinkPaginator()
        if prs == "restapi_header_link_paginator":
            return RestAPIHeaderLinkPaginator(
                pagination_page_size=self.pagination_page_size,
                pagination_results_limit=self.pagination_results_limit,
                replication_key=self.replication_key,
            )
        if prs in {"style1", "offset_paginator"}:
            return RestAPIOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size,
                jsonpath=self.next_page_token_jsonpath,
                pagination_total_limit_param=self.pagination_total_limit_param,
            )
        if prs == "hateoas_paginator":
            return BaseHATEOASPaginator()
        if prs == "single_page_paginator":
            return SinglePagePaginator()
        if prs == "page_number_paginator":
            return RestAPIBasePageNumberPaginator(
                start_value=self.pagination_initial_offset, jsonpath=self.next_page_token_jsonpath
            )
        if prs == "simple_offset_paginator":
            return SimpleOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size,
                offset_records_jsonpath=self.offset_records_jsonpath,
                pagination_page_size=self.pagination_page_size,
            )
        self.logger.error("Unknown paginator '%s'.", prs)
        raise ValueError(f"Unknown paginator {prs}.")

    # ---------- URL Param Builders ----------
    def _inject_iteration_params(self, params: Dict[str, Any], context: Optional[dict]) -> None:
        it = self.iteration_config or {}
        key = it.get("api_param_key")
        templ = it.get("api_param_template")
        if key and templ and context and "iteration_value" in context:
            params[key] = templ.format(value=context["iteration_value"])

    @staticmethod
    def _fmt_twitter_since(last_run_date: str) -> str:
        # Expecting 'YYYY-mm-ddTHH:MM:SS' or RFC; convert to 'YYYY-mm-dd_HH:MM:SS_UTC'
        try:
            dtobj = datetime.fromisoformat(str(last_run_date).replace("Z", "+00:00"))
        except Exception:
            try:
                dtobj = email.utils.parsedate_to_datetime(str(last_run_date))
            except Exception:
                return str(last_run_date)
        return dtobj.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    def _inject_bookmark_template(self, params: Dict[str, Any], context: Optional[dict]) -> None:
        if not (self.bookmark_target_param and self.bookmark_query_template and self.replication_key):
            return
        last_run_date = get_start_date(self, context)
        if not last_run_date:
            return
        existing = params.get(self.bookmark_target_param, "")
        params[self.bookmark_target_param] = Template(self.bookmark_query_template).safe_substitute(
            existing=str(existing),
            last_run_date=str(last_run_date),
            last_run_date_utc_twitter=self._fmt_twitter_since(last_run_date),
        )

    def _get_url_params_page_style(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: dict = dict(self.params or {})

        if next_page_token:
            params[self.pagination_next_page_param or "page"] = next_page_token

        self._inject_iteration_params(params, context)

        if self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                q = Template(self.source_search_query)
                params[self.source_search_field] = q.substitute(last_run_date=last_run_date)
            else:
                params.setdefault("sort", "asc")
                params.setdefault("order_by", self.replication_key)

        self._inject_bookmark_template(params, context)
        return params

    def _get_url_params_offset_style(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: dict = dict(self.params or {})

        if next_page_token:
            params[self.pagination_next_page_param or "offset"] = next_page_token

        if self.pagination_page_size is not None:
            params[self.pagination_limit_per_page_param or "limit"] = self.pagination_page_size

        self._inject_iteration_params(params, context)

        if self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                q = Template(self.source_search_query)
                params[self.source_search_field] = q.substitute(last_run_date=last_run_date)
            else:
                params.setdefault("sort", "asc")
                params.setdefault("order_by", self.replication_key)

        self._inject_bookmark_template(params, context)
        return params

    def _get_url_params_header_link(self, context: Optional[Dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        params: dict = dict(self.params or {})
        params[self.pagination_limit_per_page_param or "per_page"] = self.pagination_page_size or 25

        if next_page_token:
            for k, v in parse_qs(str(next_page_token)).items():
                params[k] = v

        self._inject_iteration_params(params, context)

        if self.replication_key == "updated_at":
            params["sort"] = "updated"
            params["direction"] = "desc" if self.use_fake_since_parameter else "asc"
        elif self.replication_key in ["starred_at", "created_at"]:
            params["sort"] = "created"
            params["direction"] = "desc"
        elif self.replication_key == "commit_timestamp":
            params["direction"] = "desc"

        self._inject_bookmark_template(params, context)
        return params

    def _get_url_params_hateoas_body(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        last_run_date = get_start_date(self, context)
        params: dict = dict(self.params or {})

        if self.pagination_page_size and self.pagination_limit_per_page_param:
            params[self.pagination_limit_per_page_param] = self.pagination_page_size

        if next_page_token:
            url_parsed = urlparse(next_page_token)
            params.update(parse_qsl(url_parsed.query or url_parsed.path))
            self.path = "" if url_parsed.path == next_page_token else url_parsed.path
        elif self.replication_key:
            if self.source_search_field and self.source_search_query and last_run_date:
                q = Template(self.source_search_query)
                params[self.source_search_field] = q.substitute(last_run_date=last_run_date)

        self._inject_iteration_params(params, context)
        self._inject_bookmark_template(params, context)
        return params

    # ---------- Response / errors ----------
    def validate_response(self, response: requests.Response) -> None:
        if response.ok:
            try:
                body = response.json()
                if isinstance(body, dict) and str(body.get("status", "")).lower() == "error":
                    preview = json.dumps(body)[: self._log_body_preview]
                    self.logger.error(
                        "API business error: path=%s url=%s req_headers=%s params/body=%s body~=%s",
                        self.path,
                        response.request.url,
                        dict(response.request.headers or {}),
                        getattr(response.request, "body", None),
                        preview,
                    )
                    response.raise_for_status()
            except Exception:
                pass
            return

        try:
            raw = response.text
        except Exception:
            raw = "<unreadable>"
        preview = (raw or "")[: self._log_body_preview]
        self.logger.error(
            "HTTP error %s %s | path=%s | url=%s | req_headers=%s | params/body=%s | resp_preview=%s",
            response.status_code,
            response.reason,
            self.path,
            getattr(response.request, "url", ""),
            dict(getattr(response.request, "headers", {}) or {}),
            getattr(response.request, "body", None),
            preview,
        )
        response.raise_for_status()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_path, input=response.json())

    # ---------- Budget + de-dupe + field pruning ----------
    def _compare_replication_values(self, left: Any, right: Any) -> Optional[int]:
        if left is None or right is None:
            return None
        # try ISO/RFC datetimes
        for fn in (self._parse_iso_datetime, self._parse_rfc_datetime, self._parse_epoch):
            try:
                l = fn(left)
                r = fn(right)
                if l is not None and r is not None:
                    return -1 if l < r else (1 if l > r else 0)
            except Exception:
                pass
        # fallback lexicographic
        try:
            l, r = str(left), str(right)
            return -1 if l < r else (1 if l > r else 0)
        except Exception:
            return None

    @staticmethod
    def _parse_iso_datetime(v: Any) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def _parse_rfc_datetime(v: Any) -> Optional[datetime]:
        try:
            return email.utils.parsedate_to_datetime(str(v))
        except Exception:
            return None

    @staticmethod
    def _parse_epoch(v: Any) -> Optional[datetime]:
        try:
            iv = int(v)
            if iv > 10_000_000_000:
                iv = iv / 1000.0
            return datetime.utcfromtimestamp(iv)
        except Exception:
            return None

    def _can_emit_more(self) -> bool:
        g = getattr(self._tap, "_global_emitted_total", 0)
        if self._max_total is not None and g >= self._max_total:
            return False
        if self._max_per_stream is not None and self._emitted_total >= self._max_per_stream:
            return False
        return True

    def _increment_emitted(self, n: int = 1) -> None:
        self._emitted_total += n
        prev = getattr(self._tap, "_global_emitted_total", 0)
        setattr(self._tap, "_global_emitted_total", prev + n)

    def post_process(self, row: types.Record, context: Optional[types.Context] = None) -> Optional[dict]:
        if not self._can_emit_more():
            return None

        # drop duplicates at/before bookmark
        if self.replication_key and self._drop_on_bookmark_le:
            bookmark_val = self.get_starting_replication_key_value(context)
            if bookmark_val is not None:
                row_val = row.get(self.replication_key)
                cmp = self._compare_replication_values(row_val, bookmark_val)
                if cmp is not None and cmp <= 0:
                    return None

        # stamp iteration metadata, if requested
        meta_key = (self.iteration_config or {}).get("metadata_key")
        if meta_key and context and "iteration_value" in context:
            row[meta_key] = context["iteration_value"]

        # flatten
        out = flatten_json(row, self.except_keys, self.store_raw_json_message)

        # field pruning
        if self.keep_fields:
            must_keep = set(self.keep_fields) | set(self.primary_keys or [])
            if self.replication_key:
                must_keep.add(self.replication_key)
            if meta_key:
                must_keep.add(meta_key)
            out = {k: v for k, v in out.items() if k in must_keep}

        self._increment_emitted(1)
        return out
