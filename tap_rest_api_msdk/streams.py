# streams.py
"""Dynamic REST stream with precise TwitterAPI handling and better error messages."""

from __future__ import annotations

import email.utils
import json
from datetime import datetime, timezone
from string import Template
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Union

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
from singer_sdk.streams import RESTStream as RestApiStream

from tap_rest_api_msdk.pagination import (
    RestAPIHeaderLinkPaginator,
    RestAPIOffsetPaginator,
    RestAPIPageNumberPaginator,
    SimpleOffsetPaginator,
)
from tap_rest_api_msdk.utils import flatten_json, get_start_date


class DynamicStream(RestApiStream):
    """One class to serve both generic REST and TwitterAPI-specific flows."""

    # SDK fields
    name = "dynamic"

    # ---------------- Init ----------------
    def __init__(  # noqa: PLR0913
        self,
        tap: Any,
        name: str,
        records_path: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        primary_keys: Optional[List[str]] = None,
        replication_key: Optional[str] = None,
        except_keys: Optional[List[str]] = None,
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
        use_request_body_not_params: bool = False,
        backoff_type: Optional[str] = None,
        backoff_param: str = "Retry-After",
        backoff_time_extension: int = 0,
        store_raw_json_message: bool = False,
        authenticator: Optional[object] = None,
        # Twitter extras (optional)
        twitter_stream_type: Optional[str] = None,
        twitter_usernames: Optional[List[str]] = None,
        twitter_hashtags: Optional[List[str]] = None,
        twitter_parent_streams: Optional[List[str]] = None,
        twitter_max_per_run: int = 20,
        tap_instance: Optional[Any] = None,
    ) -> None:
        super().__init__(tap=tap, name=name, schema=schema)

        # Generic config
        self.path = path
        self.records_path = records_path or "$[*]"
        self.params = params or {}
        self._extra_headers = headers or {}
        self.primary_keys = primary_keys or []
        self.replication_key = replication_key
        self.except_keys = except_keys or []
        self.next_page_token_jsonpath = next_page_token_path
        self.pagination_request_style = pagination_request_style or "default"
        self.pagination_response_style = pagination_response_style or "default"
        self.pagination_page_size = pagination_page_size
        self.pagination_results_limit = pagination_results_limit
        self.pagination_next_page_param = pagination_next_page_param
        self.pagination_limit_per_page_param = pagination_limit_per_page_param
        self.pagination_total_limit_param = pagination_total_limit_param or "total"
        self.pagination_initial_offset = pagination_initial_offset
        self.offset_records_jsonpath = offset_records_jsonpath
        self.start_date = start_date
        self.source_search_field = source_search_field
        self.source_search_query = source_search_query
        self.use_request_body_not_params = use_request_body_not_params
        self.backoff_type = backoff_type
        self.backoff_param = backoff_param
        self.backoff_time_extension = backoff_time_extension
        self.store_raw_json_message = store_raw_json_message

        # Twitter
        self.twitter_stream_type = twitter_stream_type
        self.twitter_usernames = twitter_usernames or []
        self.twitter_hashtags = twitter_hashtags or []
        self.twitter_parent_streams = twitter_parent_streams or []
        self.twitter_max_per_run = int(twitter_max_per_run)
        self.tap_instance = tap_instance
        self._twitter_records = 0

        # sensible default for JSONPath paginator
        if not self.next_page_token_jsonpath and self.pagination_request_style in {"default", "jsonpath_paginator"}:
            self.next_page_token_jsonpath = "$.next_page"

    # ---------------- Headers ----------------
    @property
    def http_headers(self) -> Dict[str, Any]:
        out = {}
        # merge authenticator-driven headers (SDK manages self.authenticator)
        out.update(self._extra_headers or {})
        return out

    # ---------------- Backoff ----------------
    def backoff_wait_generator(self) -> Generator[Union[int, float], None, None]:
        def _from_headers(exception):
            return int(exception.response.headers.get(self.backoff_param, 0)) + self.backoff_time_extension

        def _from_message(exception):
            try:
                msg = exception.response.json().get("message", "")
            except Exception:
                msg = exception.response.text or ""
            nums = [int(x) for x in msg.split() if x.isdigit()]
            return (max(nums) if nums else 0) + self.backoff_time_extension

        if self.backoff_type == "header":
            return self.backoff_runtime(value=_from_headers)
        if self.backoff_type == "message":
            return self.backoff_runtime(value=_from_message)
        return super().backoff_wait_generator()

    # ---------------- Pagination factory ----------------
    def get_new_paginator(self):
        if self.pagination_request_style in {"default", "jsonpath_paginator"}:
            return JSONPathPaginator(self.next_page_token_jsonpath or "$.next_page")
        if self.pagination_request_style == "simple_header_paginator":
            return SimpleHeaderPaginator("X-Next-Page")
        if self.pagination_request_style == "header_link_paginator":
            return HeaderLinkPaginator()
        if self.pagination_request_style == "restapi_header_link_paginator":
            return RestAPIHeaderLinkPaginator(
                pagination_page_size=self.pagination_page_size,
                pagination_results_limit=self.pagination_results_limit,
                replication_key=self.replication_key,
            )
        if self.pagination_request_style in {"offset_paginator", "style1"}:
            return RestAPIOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size or 25,
                jsonpath=self.next_page_token_jsonpath,
                pagination_total_limit_param=self.pagination_total_limit_param,
            )
        if self.pagination_request_style == "simple_offset_paginator":
            return SimpleOffsetPaginator(
                start_value=self.pagination_initial_offset,
                page_size=self.pagination_page_size or 25,
                offset_records_jsonpath=self.offset_records_jsonpath,
                pagination_page_size=self.pagination_page_size or 25,
            )
        if self.pagination_request_style == "hateoas_paginator":
            return BaseHATEOASPaginator()
        if self.pagination_request_style == "single_page_paginator":
            return SinglePagePaginator()
        if self.pagination_request_style == "page_number_paginator":
            return RestAPIPageNumberPaginator(
                start_value=self.pagination_initial_offset,
                jsonpath=self.next_page_token_jsonpath,
            )
        raise ValueError(f"Unknown paginator: {self.pagination_request_style}")

    # ---------------- Request param helpers ----------------
    def _page_style_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {**self.params}
        if next_page_token:
            params[self.pagination_next_page_param or "page"] = next_page_token
        if self.replication_key:
            last = get_start_date(self, context)
            if self.source_search_field and self.source_search_query and last:
                q = Template(self.source_search_query).substitute(last_run_date=last)
                params[self.source_search_field] = json.loads(q) if self.use_request_body_not_params else q
            else:
                params["sort"] = "asc"
                params["order_by"] = self.replication_key
        return params

    def _offset_style_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {**self.params}
        if next_page_token:
            params[self.pagination_next_page_param or "offset"] = next_page_token
        if self.pagination_page_size:
            params[self.pagination_limit_per_page_param or "limit"] = self.pagination_page_size
        if self.replication_key:
            last = get_start_date(self, context)
            if self.source_search_field and self.source_search_query and last:
                q = Template(self.source_search_query).substitute(last_run_date=last)
                params[self.source_search_field] = json.loads(q) if self.use_request_body_not_params else q
            else:
                params["sort"] = "asc"
                params["order_by"] = self.replication_key
        return params

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        # generic param builder
        if self.pagination_response_style in {"default", "page"}:
            return self._page_style_params(context, next_page_token)
        if self.pagination_response_style in {"offset", "style1"}:
            return self._offset_style_params(context, next_page_token)
        return {**self.params}

    # ---------------- Response validation with real error bodies ----------------
    def validate_response(self, response: requests.Response) -> None:
        if response.ok:
            return
        # include server response text for visibility
        body = ""
        try:
            body = response.text or ""
        except Exception:
            body = ""
        self.logger.error("HTTP %s on %s: %s", response.status_code, self.path, body[:2000])
        super().validate_response(response)

    # ---------------- Default parsing ----------------
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if self.twitter_stream_type:
            yield from self._parse_twitter_response(response)
            return
        try:
            data = response.json()
        except Exception:
            return
        yield from extract_jsonpath(self.records_path, input=data)

    # ---------------- Twitter: parsing ----------------
    def _parse_twitter_response(self, response: requests.Response) -> Iterable[dict]:
        try:
            payload = response.json()
        except Exception:
            return

        ttype = (self.twitter_stream_type or "").lower()

        if ttype == "user_info":
            row = payload.get("data")
            if isinstance(row, dict):
                yield row
            return

        if ttype in {"user_tweets", "mentions", "hashtag_tweets", "quotes"}:
            key = "tweets"
            for rec in payload.get(key, []) or []:
                if isinstance(rec, dict):
                    yield rec
            return

        if ttype == "replies":
            for rec in payload.get("replies", []) or []:
                if isinstance(rec, dict):
                    yield rec
            return

        # fallback
        yield from extract_jsonpath(self.records_path, input=payload)

    # ---------------- Twitter: driving requests ----------------
    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if not self.twitter_stream_type:
            # generic: delegate to SDK (handles pagination choice)
            yield from super().get_records(context)
            return

        # twitter mode
        self._twitter_records = 0

        def remaining() -> int:
            return max(0, self.twitter_max_per_run - self._twitter_records)

        # helpers
        def _add_since_time(p: Dict[str, Any]) -> Dict[str, Any]:
            try:
                last = self.get_starting_timestamp(context)
            except Exception:
                last = None
            if last:
                # endpoints expecting epoch seconds: mentions, replies, quotes (optional)
                p.setdefault("sinceTime", int(last.timestamp()))
            return p

        def _loop_cursor(call_params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
            cursor = ""
            while remaining() > 0:
                qp = {**self.params, **call_params}
                if cursor:
                    qp["cursor"] = cursor
                req = self.prepare_request(context, qp)
                resp = self.request_decorator(self._request)(req, context)
                if not resp.ok:
                    break
                # parse & count
                for rec in self.parse_response(resp):
                    yield rec
                    self._twitter_records += 1
                    if remaining() <= 0:
                        break
                # next page?
                try:
                    data = resp.json()
                except Exception:
                    break
                if data.get("has_next_page") and data.get("next_cursor"):
                    cursor = data["next_cursor"]
                else:
                    break

        ttype = (self.twitter_stream_type or "").lower()

        # --- routes matching the docs you provided ---
        if ttype == "user_info":
            for uname in self.twitter_usernames:
                par = {"userName": uname}
                req = self.prepare_request(context, {**self.params, **par})
                resp = self.request_decorator(self._request)(req, context)
                if not resp.ok:
                    continue
                for rec in self.parse_response(resp):
                    yield rec
            return

        if ttype == "user_tweets":
            # use /twitter/tweet/advanced_search for efficiency + since: filter
            for uname in self.twitter_usernames:
                since_txt = None
                try:
                    last = self.get_starting_timestamp(context)
                    if last:
                        since_txt = last.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass
                q = f"from:{uname}" + (f" since:{since_txt}" if since_txt else "")
                par = {"query": q, "queryType": "Latest"}
                for rec in _loop_cursor(par):
                    # capture IDs for dependent streams
                    if self.tap_instance and "id" in rec:
                        try:
                            self.tap_instance.add_collected_tweet_id(rec["id"])
                        except Exception:
                            pass
                    yield rec
                    if remaining() <= 0:
                        return
            return

        if ttype == "mentions":
            for uname in self.twitter_usernames:
                par = _add_since_time({"userName": uname})
                for rec in _loop_cursor(par):
                    yield rec
                    if remaining() <= 0:
                        return
            return

        if ttype == "hashtag_tweets":
            for h in self.twitter_hashtags:
                since_txt = None
                try:
                    last = self.get_starting_timestamp(context)
                    if last:
                        since_txt = last.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass
                q = f"#{h}" + (f" since:{since_txt}" if since_txt else "")
                par = {"query": q, "queryType": "Latest"}
                for rec in _loop_cursor(par):
                    yield rec
                    if remaining() <= 0:
                        return
            return

        if ttype in {"replies", "quotes"}:
            op = "replies" if ttype == "replies" else "quotes"
            tweet_ids: Set[str] = set()
            if self.tap_instance:
                try:
                    tweet_ids |= set(self.tap_instance.get_collected_tweet_ids())
                except Exception:
                    pass
                try:
                    tweet_ids |= set(self.tap_instance.get_pending_tweet_ids(op))
                except Exception:
                    pass

            for tid in tweet_ids:
                par = _add_since_time({"tweetId": tid})
                for rec in _loop_cursor(par):
                    yield rec
                    if remaining() <= 0:
                        return
            return

        # unknown type: fallback
        yield from super().get_records(context)

    # ---------------- post_process ----------------
    def post_process(
        self,
        row: types.Record,
        context: Optional[types.Context] = None,
    ) -> Optional[dict]:
        return flatten_json(row, self.except_keys, self.store_raw_json_message)
