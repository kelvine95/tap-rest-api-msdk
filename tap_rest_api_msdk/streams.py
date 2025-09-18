"""Stream type classes for the Twitter API tap."""

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_rest_api_msdk.client import TwitterApiStream
from tap_rest_api_msdk.utils import flatten_json

def _epoch_seconds(dt_obj: datetime) -> int:
    """Convert a datetime (naive treated as UTC) to epoch seconds."""
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return int(dt_obj.timestamp())

class DynamicTwitterStream(TwitterApiStream):
    """A dynamic stream that handles pagination, limits, and incremental loading."""

    def __init__(self, tap: Any, name: str, schema: dict, config: dict) -> None:
        """Initialize the stream."""
        super().__init__(tap=tap, name=name, schema=schema)
        self.config = config
        self._records_processed = 0

    @property
    def path(self) -> str:
        """Return the API path for the stream."""
        return self.config.get("path", "")

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = self.config.get("params", {}).copy()

        if next_page_token:
            params["cursor"] = next_page_token

        start_date = self.get_starting_timestamp(context)
        rra_config = self.config.get("replication_request_adapter")
        
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
        records_path = self.config.get("records_path", "$[*]")
        for record in extract_jsonpath(records_path, input=response.json()):
            if self.config.get("max_records_limit") and self._records_processed >= self.config["max_records_limit"]:
                self.logger.info(f"Stream '{self.name}' reached its record limit of {self.config['max_records_limit']}.")
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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Transform record data and handle date formatting."""
        flat_record = flatten_json(row, self.config.get("except_keys", []))
        
        date_fields = ["createdAt", "author_createdAt"]
        for field in date_fields:
            if field in flat_record and isinstance(flat_record[field], str):
                try:
                    date_obj = datetime.strptime(flat_record[field], "%a %b %d %H:%M:%S %z %Y")
                    flat_record[field] = date_obj.isoformat()
                except ValueError:
                    self.logger.warning(f"Could not parse timestamp for '{field}': {flat_record[field]}")
        
        if "inject_metadata" in self.config:
            for k, v in self.config["inject_metadata"].items():
                flat_record[k] = v
        
        return flat_record
    