"""REST client handling, including RestApiStream base class."""

from pathlib import Path
from typing import Any, Optional
import json
import urllib.parse

from singer_sdk.streams import RESTStream
from tap_rest_api_msdk.auth import get_authenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class RestApiStream(RESTStream):
    """rest-api stream class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_auth = None
        self._authenticator = getattr(self, "assigned_authenticator", None)

        # new: soft-fail & logging controls (config-based)
        cfg = getattr(self, "config", {}) or {}
        self._soft_fail_status_codes = set(cfg.get("soft_fail_status_codes") or [400, 404, 422])
        self._log_body_preview = int(cfg.get("log_response_body_preview") or 1000)

    @property
    def url_base(self) -> Any:
        return self.config["api_url"]

    @property
    def authenticator(self) -> Any:
        get_authenticator(self)
        return self._authenticator

    # NEW: friendlier error handler that can soft-fail certain codes
    def validate_response(self, response) -> None:
        if response.ok:
            return
        # preview request context
        try:
            req = response.request
            full_url = req.url
            method = req.method
            # log parsed query (helps find missing required params)
            parsed = urllib.parse.urlparse(full_url)
            q = urllib.parse.parse_qs(parsed.query)
        except Exception:
            full_url, method, q = ("<unknown>", "<unknown>", {})

        # preview body
        try:
            body_preview = response.text[: self._log_body_preview]
        except Exception:
            body_preview = "<non-text-response>"

        msg = (
            f"[{self.name}] HTTP {response.status_code} on {method} {full_url} "
            f"params={q} body~{self._log_body_preview}='{body_preview}'"
        )

        if response.status_code in self._soft_fail_status_codes:
            self.logger.warning("Soft-failing stream due to non-critical error: " + msg)
            # Treat as empty page: downstream code will emit nothing for this page/partition
            # by skipping parse/iteration when we return without raising.
            return

        # hard fail for everything else
        self.logger.error("Hard error: " + msg)
        response.raise_for_status()