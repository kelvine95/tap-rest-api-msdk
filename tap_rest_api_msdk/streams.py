# tap_rest_api_msdk/streams.py
from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Iterable, List, Optional

import requests
from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.helpers._typing import TypeConformanceLevel


TWITTER_DATE_FMT = "%a %b %d %H:%M:%S %z %Y"  # e.g. Tue Dec 10 07:00:30 +0000 2024


def parse_created_at(val: Optional[str]) -> Optional[int]:
    """Return epoch seconds for the Twitter-style createdAt string; None if not parsable."""
    if not val or not isinstance(val, str):
        return None
    try:
        return int(dt.datetime.strptime(val, TWITTER_DATE_FMT).timestamp())
    except Exception:
        return None


class CursorPaginator(BaseAPIPaginator):
    """Simple cursor paginator using 'next_cursor' from response JSON."""
    def __init__(self) -> None:
        super().__init__(None)

    def has_more(self, response: requests.Response) -> bool:
        data = response.json() if response.content else {}
        has_next = data.get("has_next_page")
        next_cur = data.get("next_cursor")
        return bool(next_cur) or bool(has_next)

    def get_next(self, response: requests.Response) -> Optional[str]:
        data = response.json() if response.content else {}
        return data.get("next_cursor") or None


class BaseTwitterStream(RESTStream):
    """Shared behavior across all Twitter streams."""

    # Let Singer trust our records as-is; fields not in schema -> allowed if selected
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    # Provided by Tap
    path: str
    records_path: Optional[str] = None
    next_page_token_jsonpath: Optional[str] = None

    primary_keys: List[str] = []
    replication_key: Optional[str] = None

    # Custom
    iteration_config: Dict[str, Any] = {}
    pagination_results_limit: Optional[int] = None

    @property
    def url_base(self) -> str:
        return self.config["api_url"].rstrip("/")

    @property
    def http_headers(self) -> Dict[str, Any]:
        hdrs = dict(self.config.get("headers") or {})
        api_keys = self.config.get("api_keys") or {}
        hdrs.update(api_keys)
        return hdrs

    def __init__(
        self,
        tap,
        name: str,
        path: str,
        records_path: Optional[str],
        primary_keys: List[str],
        replication_key: Optional[str],
        next_page_token_path: Optional[str],
        params: Dict[str, Any],
        iteration_config: Dict[str, Any],
        pagination_results_limit: Optional[int],
    ):
        super().__init__(tap=tap, name=name)
        # keep attribute names
        self.path = path
        self.records_path = records_path
        self.primary_keys = primary_keys or []
        self.replication_key = replication_key
        self.next_page_token_jsonpath = next_page_token_path
        self._base_params = params or {}
        self.iteration_config = iteration_config or {}
        self.pagination_results_limit = pagination_results_limit
        self._http_request_count = 0
        self._emitted_total = 0

        # cap controls
        self._max_per_stream = int(self.config.get("max_records_per_stream") or 0) or None
        self._max_total = int(self.config.get("max_records_total") or 0) or None

    # ---------- Pagination ----------
    def get_new_paginator(self) -> BaseAPIPaginator:
        return CursorPaginator()

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[str]) -> Optional[str]:
        if self.next_page_token_jsonpath:
            matches = list(extract_jsonpath(self.next_page_token_jsonpath, response.json() or {}))
            if matches:
                return matches[0]
        return None  # our CursorPaginator also looks at body

    # ---------- Params ----------
    def get_url_params(self, context: Optional[dict], next_page_token: Optional[str]) -> Dict[str, Any]:
        params = dict(self._base_params)

        # Pagination
        if next_page_token:
            params["cursor"] = next_page_token

        # Iteration-level param injection (e.g., userName or query)
        iter_cfg = self.iteration_config or {}
        api_param_key = iter_cfg.get("api_param_key")
        if api_param_key:
            # value is provided in context under 'iteration_value'
            if context and "iteration_value" in context:
                templ = iter_cfg.get("api_param_template", "{value}")
                params[api_param_key] = templ.format(value=context["iteration_value"])

        # For Advanced Search, we inject 'since:...._UTC' based on bookmark to avoid duplicates
        if self.path == "/twitter/tweet/advanced_search":
            bookmark = self._get_partition_bookmark(context)
            if bookmark:
                # twitter expects 'since:YYYY-MM-DD_HH:MM:SS_UTC' inside 'query'
                since_utc = dt.datetime.utcfromtimestamp(bookmark).strftime("%Y-%m-%d_%H:%M:%S_UTC")
                q = params.get("query", "")
                # Remove any existing since:..._UTC to avoid duplication
                q = re.sub(r"\s+since:\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}_UTC", "", q).strip()
                params["query"] = (q + f" since:{since_utc}").strip()

        # For Mentions, we pass sinceTime in epoch seconds
        if self.path == "/twitter/user/mentions":
            bookmark = self._get_partition_bookmark(context)
            if bookmark:
                params["sinceTime"] = int(bookmark + 1)  # +1s to avoid boundary dupes

        return params

    # ---------- Request ----------
    def prepare_request(self, context: Optional[dict], next_page_token: Optional[str]) -> requests.PreparedRequest:
        req = requests.Request(
            method="GET",
            url=f"{self.url_base}{self.path}",
            headers=self.http_headers,
            params=self.get_url_params(context, next_page_token),
        )
        return req.prepare()

    # ---------- Parsing ----------
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        body = response.json() or {}

        # Count HTTP calls (used for budget visibility)
        self._http_request_count += 1

        if self.records_path:
            for rec in extract_jsonpath(self.records_path, body):
                yield rec
        else:
            # Whole body is the record if no path provided
            yield body

    # ---------- State / bookmarks ----------
    def _get_partition_bookmark(self, context: Optional[dict]) -> Optional[int]:
        """Return epoch seconds for last seen replication_key per-partition."""
        if not self.replication_key:
            return None
        return parse_created_at(self.get_starting_timestamp(context))

    def get_starting_timestamp(self, context: Optional[dict]) -> Optional[str]:
        """Return the tap/bookmark value for replication_key as raw string (createdAt)."""
        if not self.replication_key:
            return None
        state = self.get_context_state(context) or {}
        return state.get("replication_key_value")  # raw createdAt string

    def _should_stop_on_bookmark(self, record: dict, context: Optional[dict]) -> bool:
        """For endpoints without server-side since filtering, stop when we hit already-synced rows."""
        if not self.replication_key:
            return False
        prior_raw = self.get_starting_timestamp(context)
        if not prior_raw:
            return False
        prior_sec = parse_created_at(prior_raw)
        now_sec = parse_created_at(record.get(self.replication_key))
        return (prior_sec is not None and now_sec is not None and now_sec <= prior_sec)

    # ---------- Emission / caps ----------
    def _can_emit_more(self) -> bool:
        if self._max_per_stream is not None and self._emitted_total >= self._max_per_stream:
            return False
        # Cross-stream total cap is enforced in Tap runner typically; add soft guard here
        if self._max_total is not None and self._tap and getattr(self._tap, "_global_emitted_total", 0) >= self._max_total:
            return False
        return True

    def _increment_emitted(self, n: int = 1) -> None:
        self._emitted_total += n
        if self._tap:
            prev = getattr(self._tap, "_global_emitted_total", 0)
            setattr(self._tap, "_global_emitted_total", prev + n)

    # ---------- Sync ----------
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """Attach iteration metadata for lineage/partitioned state."""
        meta_key = (self.iteration_config or {}).get("metadata_key")
        iter_type = (self.iteration_config or {}).get("iteration_type")
        if meta_key and context and "iteration_value" in context:
            row[meta_key] = context["iteration_value"]
        if iter_type:
            row["_iteration_type"] = iter_type
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> Optional[dict]:
        return None

    def validate_response(self, response: requests.Response) -> None:
        # Treat 200 only as success (lib already raises for bad status)
        response.raise_for_status()

    def _yield_records_with_budget(self, response: requests.Response, context: Optional[dict]) -> Iterable[dict]:
        for rec in self.parse_response(response):
            # Early stop for endpoints without server-side since filtering
            if self.path == "/twitter/user/last_tweets" and self._should_stop_on_bookmark(rec, context):
                return  # stop the stream cleanly

            if not self._can_emit_more():
                return

            yield self.post_process(rec, context)
            self._increment_emitted(1)

    # Singer SDK calls .request_records(); we keep logic in .sync() for fine control.
    def sync(self) -> None:
        iter_cfg = self.iteration_config or {}
        values = iter_cfg.get("values") or [None]
        # Ensure we track partitioned bookmarks
        for val in values:
            context = {
                "iteration_value": val,
                "iteration_type": iter_cfg.get("iteration_type"),
            }
            super().sync(context=context)


class TwitterAdvancedSearchStream(BaseTwitterStream):
    name = "twitter_advanced_search"  # internal name; catalog name is provided by Tap
    path = "/twitter/tweet/advanced_search"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row = super().post_process(row, context)
        # Ensure createdAt exists for replication
        if "createdAt" not in row and "created_at" in row:
            row["createdAt"] = row["created_at"]
        return row


class TwitterMentionsStream(BaseTwitterStream):
    name = "twitter_mentions"
    path = "/twitter/user/mentions"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row = super().post_process(row, context)
        return row


class TwitterLatestStream(BaseTwitterStream):
    name = "twitter_latest"
    path = "/twitter/user/last_tweets"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row = super().post_process(row, context)
        return row


class TwitterUsersStream(BaseTwitterStream):
    name = "twitter_users"
    path = "/twitter/user/info"
    records_path = "$.data"
    primary_keys = ["id"]
    replication_key = None
