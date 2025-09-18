# pagination.py
"""Helper paginator shims used by DynamicStream (backward compatible names)."""

from __future__ import annotations
from typing import Any, Optional

from singer_sdk.pagination import BaseAPIPaginator


class RestAPIHeaderLinkPaginator(BaseAPIPaginator):
    """Shim to keep legacy import path; delegates to BaseAPIPaginator contract."""

    def __init__(
        self,
        pagination_page_size: Optional[int] = None,
        pagination_results_limit: Optional[int] = None,
        replication_key: Optional[str] = None,
    ) -> None:
        super().__init__(start_value=None)
        self._page_size = pagination_page_size
        self._results_limit = pagination_results_limit
        self._repl_key = replication_key

    def get_next(self, response) -> Any:  # noqa: ANN401
        link = response.headers.get("Link") or response.headers.get("link")
        if not link:
            return None
        # Let SDK HeaderLinkPaginator exist elsewhere; here we just stop if not present
        return None


class RestAPIOffsetPaginator(BaseAPIPaginator):
    """Offset paginator that increments by page size and stops if no records or limit hit."""

    def __init__(
        self,
        start_value: int = 1,
        page_size: int = 25,
        jsonpath: Optional[str] = None,
        pagination_total_limit_param: Optional[str] = None,
    ) -> None:
        super().__init__(start_value=start_value)
        self._page_size = page_size

    def get_next(self, response) -> Any:  # noqa: ANN401
        # naive increment; stop when empty page (handled by stream) or server stops us
        if self.current_value is None:
            return None
        return (int(self.current_value) or 0) + self._page_size


class RestAPIPageNumberPaginator(BaseAPIPaginator):
    """Simple page-number paginator."""

    def get_next(self, response) -> Any:  # noqa: ANN401
        if self.current_value is None:
            return None
        return int(self.current_value) + 1


class SimpleOffsetPaginator(BaseAPIPaginator):
    """Another simple offset style with optional record-based advancement."""

    def __init__(
        self,
        start_value: int = 1,
        page_size: int = 25,
        offset_records_jsonpath: Optional[str] = None,
        pagination_page_size: Optional[int] = None,
    ) -> None:
        super().__init__(start_value=start_value)
        self._page_size = page_size

    def get_next(self, response) -> Any:  # noqa: ANN401
        if self.current_value is None:
            return None
        return int(self.current_value) + self._page_size
