from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Iterable, List, Optional

import requests
from singer_sdk import typing as th
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

TWITTER_DATE_FMT = "%a %b %d %H:%M:%S %z %Y"  # e.g. Tue Dec 10 07:00:30 +0000 2024


def parse_created_at_to_epoch(val: Optional[str]) -> Optional[int]:
    if not val or not isinstance(val, str):
        return None
    try:
        return int(dt.datetime.strptime(val, TWITTER_DATE_FMT).timestamp())
    except Exception:
        return None


def iso_start_to_epoch(val: Optional[str]) -> Optional[int]:
    if not val:
        return None
    try:
        s = val.replace("Z", "+00:00")
        return int(dt.datetime.fromisoformat(s).timestamp())
    except Exception:
        return None


def epoch_to_since_utc(epoch: int) -> str:
    return dt.datetime.utcfromtimestamp(epoch).strftime("%Y-%m-%d_%H:%M:%S_UTC")


def tweet_schema_dict() -> Dict[str, Any]:
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
        th.Property("createdAt", th.StringType),  # replication key
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
        # Use JSON-Schema unions for nullable objects (SDK-agnostic):
        th.Property("quoted_tweet", th.CustomType({"type": ["object", "null"]})),
        th.Property("retweeted_tweet", th.CustomType({"type": ["object", "null"]})),
        # Metadata
        th.Property("_source_handle", th.StringType),
        th.Property("_source_keyword", th.StringType),
        th.Property("_source_hashtag", th.StringType),
        th.Property("_iteration_type", th.StringType),
        th.Property("_raw_message", th.CustomType({"type": ["object", "array", "null"]})),
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
        # Build schema first for discovery
        schema = user_schema_dict() if path == "/twitter/user/info" else tweet_schema_dict()

        # Optional per-stream overrides
        overrides_all = tap.config.get("schema_overrides") or {}
        overrides = overrides_all.get(name) or {}
        if isinstance(overrides.get("properties"), dict):
            schema.setdefault("properties", {}).update(overrides["properties"])

        super().__init__(tap=tap, name=name, schema=schema)

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
        self._max_per_stream = int(self.config.get("max_records_per_stream") or 0) or None
        self._max_total = int(self.config.get("max_records_total") or 0) or None

    @property
    def url_base(self) -> str:
        return self.config["api_url"].rstrip("/")

    @property
    def http_headers(self) -> Dict[str, Any]:
        hdrs = dict(self.config.get("headers") or {})
        hdrs.update(self.config.get("api_keys") or {})  # e.g. {"X-API-Key": "..."}
        return hdrs

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[str]) -> Optional[str]:
        body = response.json() or {}
        if self.next_page_token_jsonpath:
            matches = list(extract_jsonpath(self.next_page_token_jsonpath, body))
            return matches[0] if matches else None
        return body.get("next_cursor")

    def _partition_bookmark_epoch(self, context: Optional[dict]) -> Optional[int]:
        if not self.replication_key:
            return None
        state = self.get_context_state(context) or {}
        prior_raw = state.get("replication_key_value")
        if prior_raw:
            ep = parse_created_at_to_epoch(prior_raw)
            if ep is not None:
                return ep
        return iso_start_to_epoch(self.config.get("start_date"))

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[str]) -> Dict[str, Any]:
        params = dict(self._base_params)
        if next_page_token:
            params["cursor"] = next_page_token

        iter_cfg = self.iteration_config or {}
        api_param_key = iter_cfg.get("api_param_key")
        if api_param_key and context and "iteration_value" in context:
            templ = iter_cfg.get("api_param_template", "{value}")
            params[api_param_key] = templ.format(value=context["iteration_value"])

        if self.path == "/twitter/tweet/advanced_search":
            mark_epoch = self._partition_bookmark_epoch(context)
            if mark_epoch is not None:
                since_utc = epoch_to_since_utc(mark_epoch)
                q = params.get("query", "")
                q = re.sub(r"\s+since:\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}_UTC", "", q).strip()
                params["query"] = (q + f" since:{since_utc}").strip()

        if self.path == "/twitter/user/mentions":
            mark_epoch = self._partition_bookmark_epoch(context)
            if mark_epoch is not None:
                params["sinceTime"] = int(mark_epoch + 1)  # skip boundary

        return params

    def prepare_request(self, context: Optional[dict], next_page_token: Optional[str]) -> requests.PreparedRequest:
        req = requests.Request(
            method="GET",
            url=f"{self.url_base}{self.path}",
            headers=self.http_headers,
            params=self.get_url_params(context, next_page_token),
        )
        return req.prepare()

    def validate_response(self, response: requests.Response) -> None:
        response.raise_for_status()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        body = response.json() or {}
        self._http_request_count += 1
        if self.records_path:
            for rec in extract_jsonpath(self.records_path, body):
                yield rec
        else:
            yield body

    def _can_emit_more(self) -> bool:
        if self._max_per_stream is not None and self._emitted_total >= self._max_per_stream:
            return False
        global_emitted = getattr(self._tap, "_global_emitted_total", 0)
        if self._max_total is not None and global_emitted >= self._max_total:
            return False
        return True

    def _increment_emitted(self, n: int = 1) -> None:
        self._emitted_total += n
        setattr(self._tap, "_global_emitted_total", getattr(self._tap, "_global_emitted_total", 0) + n)

    def _should_stop_on_bookmark(self, record: dict, context: Optional[dict]) -> bool:
        if self.path != "/twitter/user/last_tweets" or not self.replication_key:
            return False
        prior_epoch = self._partition_bookmark_epoch(context)
        if prior_epoch is None:
            return False
        now_epoch = parse_created_at_to_epoch(record.get(self.replication_key))
        return (now_epoch is not None) and (now_epoch <= prior_epoch)

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        page_count = 0
        token: Optional[str] = None

        while True:
            if not self._can_emit_more():
                return

            resp = self.requests_session.send(self.prepare_request(context, token), timeout=30)
            self.validate_response(resp)

            emitted_this_page = 0
            stop_early = False

            for rec in self.parse_response(resp):
                if self._should_stop_on_bookmark(rec, context):
                    stop_early = True
                    break
                if not self._can_emit_more():
                    stop_early = True
                    break

                meta_key = (self.iteration_config or {}).get("metadata_key")
                iter_type = (self.iteration_config or {}).get("iteration_type")
                if meta_key and context and "iteration_value" in context:
                    rec[meta_key] = context["iteration_value"]
                if iter_type:
                    rec["_iteration_type"] = iter_type

                if self.config.get("store_raw_json_message"):
                    rec.setdefault("_raw_message", rec)

                yield rec
                self._increment_emitted(1)
                emitted_this_page += 1

            page_count += 1
            if stop_early:
                return

            if self.pagination_results_limit is not None and page_count >= int(self.pagination_results_limit):
                return

            token = self.get_next_page_token(resp, token)
            if not token:
                return

    def sync(self, context: Optional[dict] = None) -> None:
        iter_cfg = self.iteration_config or {}
        values = iter_cfg.get("values") or [None]
        for val in values:
            part_ctx = {"iteration_value": val, "iteration_type": iter_cfg.get("iteration_type")}
            super().sync(context=part_ctx)


class TwitterAdvancedSearchStream(BaseTwitterStream):
    path = "/twitter/tweet/advanced_search"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"


class TwitterMentionsStream(BaseTwitterStream):
    path = "/twitter/user/mentions"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"


class TwitterLatestStream(BaseTwitterStream):
    path = "/twitter/user/last_tweets"
    records_path = "$.tweets[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"


class TwitterUsersStream(BaseTwitterStream):
    path = "/twitter/user/info"
    records_path = "$.data"
    primary_keys = ["id"]
    replication_key = None
