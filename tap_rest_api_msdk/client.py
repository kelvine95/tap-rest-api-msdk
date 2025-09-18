"""A simplified API client for the Twitter API."""
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator

class TwitterApiStream(RESTStream):
    """Base stream class for the Twitter API."""
    url_base = "https://api.twitterapi.io"

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="X-API-Key",
            value=self.config.get("api_keys", {}).get("X-API-Key", ""),
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {"Accept": "application/json"}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config["user_agent"]
        return headers