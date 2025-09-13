# tap_rest_api_msdk/streams.py
from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Iterable, List, Optional

import requests
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.helpers._typing import TypeConformanceLevel

TWITTER_DATE_FMT = "%a %b %d %H:%M:%S %z %Y"  # Tue Dec 10 07:00:30 +0000 2024


def parse_created_at(val: Optional[str]) -> Optional[int]:
    if not val or not isinstance(val, str):
        return None
    try:
        return int(dt.datetime.strptime(val, TWITTER_DATE_FMT).timestamp())
    except Exception:
        return None


class CursorPaginator(BaseAPIPaginator):
    def __init__(self) -> None:
        super().__init__(None)

    def has_more(self, response: requests.Response) -> bool:
        if not response.content:
            return False
        data = response.json()
        return bool(data.get("next_cursor")) or bool(data.get("has_next_page"))

    def get_next(self, response: requests.Response) -> Optional[str]:
        if not response.content:
            return None
        return (response.json() or {}).get("next_cursor")


def tweet_schema_dict() -> Dict[str, Any]:
    """Minimal, static schema for tweet-like records (per docs)."""
    return th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("source", th.StringType),
        th.Property("retweetCount", th.IntegerType),
        th.Property("replyCount", th.IntegerType),
        th.Property("likeCount", th.IntegerType),
        th.Property("quoteCount", th.IntegerType),
        th.Property("viewCount", th.IntegerType),
        th.Property("createdAt", th.StringType),  # replication_key
        th.Property("lang", th.StringType),
        th.Property("bookmarkCount", th.IntegerType),
        th.Property("isReply", th.BooleanType),
        th.Property("inReplyToId", th.StringType),
        th.Property("conversationId", th.StringType),
        th.Property("displayTextRange", th.ArrayType(th.IntegerType)),
        th.Property("inReplyToUserId", th.StringType),
        th.Property("inReplyToUsername", th.StringType),
        th.Property("author", th.ObjectType()),
        th.Property("entities", th.ObjectType()),
        th.Property("quoted_tweet", th.ObjectType(nullable=True)),
        th.Property("retweeted_tweet", th.ObjectType(nullable=True)),
        # Metadata we add:
        th.Property("_source_handle", th.StringType),
        th.Property("_source_keyword", th.StringType),
        th.Property("_source_hashtag", th.StringType),
        th.Property("_iteration_type", th.StringType),
        th.Property("_raw_message", th.ObjectType()),  # if store_raw_json_message=true
    ).to_dict()


def user_schema_dict() -> Dict[str, Any]:
    return th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("userName", th.StringType),
        th.Property("url", th.StringType),
        th.Property("name", th.StringType),
        th.Property("isBlueVerified", th.BooleanType),
        th.Property("verifiedType", th.StringType),
        th.Property("profilePicture", th.StringType),
        th.Property("coverPicture", th.StringType),
        th.Property("description", th.StringType),
        th.Property("location", th.StringType),
        th.Property("followers", th.IntegerType),
        th.Property("following", th.IntegerType),
        th.Property("canDm", th.BooleanType),
        th.Property("createdAt", th.StringType),
        th.Property("favouritesCount", th.IntegerType),
        th.Property("hasCustomTimelines", th.BooleanType),
        th.Property("isTranslator", th.BooleanType),
        th.Property("mediaCount", th.IntegerType),
        th.Property("statusesCount", th.IntegerType),
        th.Property("withheldInCountries", th.ArrayType(th.StringType)),
        th.Property("affiliatesHighlightedLabel", th.ObjectType()),
        th.Property("possiblySensitive", th.BooleanType),
        th.Property("pinnedTweetIds", th.ArrayType(th.StringType)),
        th.Property("isAutomated", th.BooleanType),
        th.Property("automatedBy", th.StringType),
        th.Property("unavailable", th.BooleanType),
        th.Property("message", th.StringType),
        th.Property("unavailableReason", th.StringType),
        th.Property("profile_bio", th.ObjectType()),
        # Metadata
        th.Property("_source_handle", th.StringType),
        th.Property("_iteration_type", th.StringType),
    ).to_dict()


class BaseTwitterStream(RESTStream):
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    # provided at construction
    path: str
    records_path: Optional[str] = None
    next_page_token_jsonpath: Optional[str] = None
    primary_keys: List[str] = []
    replication_key: Optional[str] = None
    iteration_config: Dict[str, Any] = {}
    pagination_results_limit: Optional[int] = None

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
        # Build schema FIRST so discovery has it
        if path == "/twitter/user/info":
            _schema = user_schema_dict()
        else:
            _schema = tweet_schema_dict()

        super().__init__(tap=tap, name=name, schema=_schema)

        # retain config
        self.path = path
        self.records_path = records_path
        self.primary_keys = primary_keys or []
        self.replication_key = replication_key
        self.next_page_token_jsonpath = next_page_token_path
        self._base_params = params or {}
        self.iteration_config = iteration_config or {}
        self.pagination_results_limit = pagination_results_limit

        # counters & caps
        self._http_request_count = 0
        self._emitted_total = 0
        self._max_per_stream = int(self.config.get("max_records_per_stream") or 0) or None
        self._max_total = int(self.config.get("max_records_total") or 0) or None

    @property
    def url_base(self) -> str:
        return self.config["api_url"].rstrip("/")

    @property
    def http_headers(self) -> Dict[str, Any]:
        hdrs = dict(self.config.get("headers") or {})
        api_keys = self.config.get("api_keys") or {}
        hdrs.update(api_keys)
        return hdrs

    # ---------- Pagination ----------
    def get_new_paginator(self) -> BaseAPIPaginator:
        return CursorPaginator()

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[str]) -> Optional[str]:
        if self.next_page_token_jsonpath:
            matches = list(extract_jsonpath(self.next_page_token_jsonpath, response.json() or {}))
            if matches:
                return matches[0]
        return None

    # ---------- Params ----------
    def get_url_params(self, context: Optional[dict], next_page_token: Optional[str]) -> Dict[str, Any]:
        params = dict(self._base_params)

        if next_page_token:
            params["cursor"] = next_page_token

        iter_cfg = self.iteration_config or {}
        api_param_key = iter_cfg.get("api_param_key")
        if api_param_key and context and "iteration_value" in context:
            templ = iter_cfg.get("api_param_template", "{value}")
            params[api_param_key] = templ.format(value=context["iteration_value"])

        # Advanced search -> use since: to avoid duplicates
        if self.path == "/twitter/tweet/advanced_search":
            bookmark = self._get_partition_bookmark(context)
            if bookmark:
                since_utc = dt.datetime.utcfromtimestamp(bookmark).strftime("%Y-%m-%d_%H:%M:%S_UTC")
                q = params.get("query", "")
                q = re.sub(r"\s+since:\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}_UTC", "", q).strip()
                params["query"] = (q + f" since:{since_utc}").strip()

        # Mentions -> sinceTime
        if self.path == "/twitter/user/mentions":
            bookmark = self._get_partition_bookmark(context)
            if bookmark:
                params["sinceTime"] = int(bookmark + 1)  # +1s to skip boundary

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
        self._http_request_count += 1

        if self.records_path:
            for rec in extract_jsonpath(self.records_path, body):
                yield rec
        else:
            yield body

    # ---------- State / bookmarks ----------
    def _get_partition_bookmark(self, context: Optional[dict]) -> Optional[int]:
        if not self.replication_key:
            return None
        raw = self.get_starting_timestamp(context)
        return parse_created_at(raw) if raw else None

    def get_starting_timestamp(self, context: Optional[dict]) -> Optional[str]:
        if not self.replication_key:
            return None
        state = self.get_context_state(context) or {}
        return state.get("replication_key_value")

    def _should_stop_on_bookmark(self, record: dict, context: Optional[dict]) -> bool:
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
        # Cross-stream soft cap using tap accumulator
        g = getattr(self._tap, "_global_emitted_total", 0)
        if self._max_total is not None and g >= self._max_total:
            return False
        return True

    def _increment_emitted(self, n: int = 1) -> None:
        self._emitted_total += n
        prev = getattr(self._tap, "_global_emitted_total", 0)
        setattr(self._tap, "_global_emitted_total", prev + n)

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
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
        response.raise_for_status()

    def _yield_records_with_budget(self, response: requests.Response, context: Optional[dict]) -> Iterable[dict]:
        for rec in self.parse_response(response):
            if self.path == "/twitter/user/last_tweets" and self._should_stop_on_bookmark(rec, context):
                return
            if not self._can_emit_more():
                return
            yield self.post_process(rec, context)
            self._increment_emitted(1)

    def sync(self, context: Optional[dict] = None) -> None:
        # Partition by iteration values
        iter_cfg = self.iteration_config or {}
        values = iter_cfg.get("values") or [None]
        for val in values:
            part_ctx = {"iteration_value": val, "iteration_type": iter_cfg.get("iteration_type")}
            super().sync(context=part_ctx)


class TwitterAdvancedSearchStream(BaseTwitterStream):
    name = "twitter_advanced_search"
    path = "/twitter/tweet/advanced_search"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row = super().post_process(row, context)
        if "createdAt" not in row and "created_at" in row:
            row["createdAt"] = row["created_at"]
        return row


class TwitterMentionsStream(BaseTwitterStream):
    name = "twitter_mentions"
    path = "/twitter/user/mentions"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"


class TwitterLatestStream(BaseTwitterStream):
    name = "twitter_latest"
    path = "/twitter/user/last_tweets"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"


class TwitterUsersStream(BaseTwitterStream):
    name = "twitter_users"
    path = "/twitter/user/info"
    records_path = "$.data"
    primary_keys = ["id"]
    replication_key = None
