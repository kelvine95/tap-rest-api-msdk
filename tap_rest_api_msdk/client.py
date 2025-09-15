"""REST client base stream."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from singer_sdk.streams import RESTStream
from tap_rest_api_msdk.auth import get_authenticator

SCHEMAS_DIR = Path(__file__).parent / "schemas"


class RestApiStream(RESTStream):
    """Generic RESTStream with unified auth handling and soft-fail support."""

    # Populated from tap config at runtime
    soft_fail_status_codes: Optional[list[int]] = None
    request_timeout_secs: Optional[int] = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.http_auth = None
        self._authenticator = getattr(self, "assigned_authenticator", None)

        # pull soft-fail and timeout from tap config, if set
        cfg = getattr(self, "config", {}) or {}
        self.soft_fail_status_codes = cfg.get("soft_fail_status_codes") or []
        self.request_timeout_secs = cfg.get("request_timeout_secs") or None

    @property
    def url_base(self) -> str:
        base = self.config["api_url"]
        return base[:-1] if base.endswith("/") else base

    @property
    def authenticator(self) -> Any:
        # Lazily create/authenticate as needed
        return get_authenticator(self)

    def prepare_request_kwargs(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        kwargs = super().prepare_request_kwargs(context, next_page_token)
        if self.request_timeout_secs:
            kwargs["timeout"] = float(self.request_timeout_secs)
        return kwargs

    def validate_response(self, response) -> None:
        if self.soft_fail_status_codes and response.status_code in self.soft_fail_status_codes:
            self.logger.warning(
                "Soft-failing response: %s %s -> %s (%s)",
                response.request.method,
                response.request.url,
                response.status_code,
                (response.text or "")[:500],
            )
            # Raise StopIteration for this page to be treated as empty by SDK.
            # We do this by returning without raising; parse_response should yield nothing.
            return
        try:
            response.raise_for_status()
        except Exception:
            # emit rich context for troubleshooting
            self.logger.exception(
                "HTTP error for %s %s -> %s. Body(head): %s",
                response.request.method,
                response.request.url,
                response.status_code,
                (response.text or "")[:1000],
            )
            raise
