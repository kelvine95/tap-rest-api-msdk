"""Stream type classes for the Twitter API tap."""

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator
from tap_rest_api_msdk.utils import flatten_json

def _epoch_seconds(dt_obj: datetime) -> int:
    """Convert a datetime (naive treated as UTC) to epoch seconds."""
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return int(dt_obj.timestamp())

class DynamicStream(RESTStream):
    """A dynamic stream that handles pagination, limits, and incremental loading."""
    url_base = "https://api.twitterapi.io"
    
    def __init__(self, tap: Any, name: str, schema: dict, config: dict) -> None:
        """Initialize the stream."""
        super().__init__(tap=tap, name=name, schema=schema)
        self._stream_config = config
        self._records_processed = 0

    @property
    def path(self) -> str:
        """Return the API path for the stream."""
        return self._stream_config.get("path", "")

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="X-API-Key",
            value=self.config.get("api_keys", {}).get("X-API-Key", ""),
            location="header",
        )

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = self._stream_config.get("params", {}).copy()

        if next_page_token:
            params["cursor"] = next_page_token

        start_date = self.get_starting_timestamp(context)
        rra_config = self._stream_config.get("replication_request_adapter")
        
        if self.replication_key and start_date and rra_config:
            mode = rra_config.get("mode")
            key = rra_config.get("key")
            
            if mode == "add_query_suffix":
                template = rra_config.get("template", "")
                if "${start_date_iso}" in template:
                    iso_date = start_date.strftime("%Y-%m-%d_%H:%M:%S_UTC")
                    suffix = template.replace("${start_date_iso}", iso_date)
                    if key in params and isinstance(params[key], str):
                        params[key] += suffix
                    else:
                        params[key] = suffix
            
            elif mode == "param":
                transform = rra_config.get("transform")
                if transform == "epoch_seconds":
                    params[key] = _epoch_seconds(start_date)
        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return the next page token."""
        return response.json().get("next_cursor")

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and yield records."""
        records_path = self._stream_config.get("records_path", "$[*]")
        for record in extract_jsonpath(records_path, input=response.json()):
            if self._stream_config.get("max_records_limit") and self._records_processed >= self._stream_config["max_records_limit"]:
                self.logger.info(f"Stream '{self.name}' reached its record limit of {self._stream_config['max_records_limit']}.")
                break
            
            if self._tap.max_ingestion_limit and self._tap.total_records_processed >= self._tap.max_ingestion_limit:
                if not self._tap.reached_max_limit:
                    self._tap.logger.info(f"Tap reached its global ingestion limit of {self._tap.max_ingestion_limit}.")
                    self._tap.reached_max_limit = True
                break

            yield record
            self._records_processed += 1
            if hasattr(self._tap, 'total_records_processed'):
                self._tap.total_records_processed += 1
    
    def _maybe_register_id(self, flat_record: dict, original_row: dict) -> None:
        """Register a tweet ID into state if configured."""
        cfg = self._stream_config.get("id_registry_config", {})
        registry_key = cfg.get("registry_key")
        if not registry_key: return

        id_path = cfg.get("id_path", "$.id")
        tweet_id = None
        try:
            matches = list(extract_jsonpath(id_path, input=original_row))
            if matches: tweet_id = str(matches[0])
        except Exception: tweet_id = None
        
        if not tweet_id:
            fallback = flat_record.get("id")
            if fallback: tweet_id = str(fallback)
        
        if not tweet_id: return
        
        min_likes = int(cfg.get("min_like_count", 0))
        like_count = int(flat_record.get("likeCount", 0) or 0)
        if like_count < min_likes: return

        reg = self._tap.state.setdefault("registry", {})
        reg_list = reg.setdefault(registry_key, [])
        if tweet_id not in reg_list:
            reg_list.append(tweet_id)
            self._tap.state["registry"] = reg
            self._tap.persist_state()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Transform record data, format dates, and register IDs."""
        flat_record = flatten_json(row, self._stream_config.get("except_keys", []))
        
        date_fields = ["createdAt", "author_createdAt"]
        for field in date_fields:
            if field in flat_record and isinstance(flat_record[field], str):
                try:
                    date_obj = datetime.strptime(flat_record[field], "%a %b %d %H:%M:%S %z %Y")
                    flat_record[field] = date_obj.isoformat()
                except ValueError:
                    self.logger.warning(f"Could not parse timestamp for '{field}': {flat_record[field]}")
        
        if "inject_metadata" in self._stream_config:
            for k, v in self._stream_config["inject_metadata"].items():
                flat_record[k] = v
        
        self._maybe_register_id(flat_record=flat_record, original_row=row)
        
        return flat_record
    