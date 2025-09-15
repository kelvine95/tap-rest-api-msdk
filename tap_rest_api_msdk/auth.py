"""REST authentication handling (generic)."""

from __future__ import annotations

import os
from typing import Any, Optional, Dict

import boto3
from requests_aws4auth import AWS4Auth
from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    APIKeyAuthenticator,
    BasicAuthenticator,
    BearerTokenAuthenticator,
    OAuthAuthenticator,
)


class AWSConnectClient:
    """Light wrapper creating AWS4Auth from either explicit keys or profile."""

    def __init__(self, connection_config: Dict[str, Any], create_signed_credentials: bool = True) -> None:
        self.connection_config = connection_config or {}
        self.create_signed_credentials = bool(self.connection_config.get("create_signed_credentials", create_signed_credentials))
        self.aws_auth: Optional[AWS4Auth] = None
        self.region: Optional[str] = None
        self.credentials = None
        self.aws_service: Optional[str] = None
        self.session: Optional[boto3.session.Session] = None

        self._create_session_and_store_auth()

    def _create_session_and_store_auth(self) -> None:
        cfg = self.connection_config
        profile = cfg.get("aws_profile") or os.environ.get("AWS_PROFILE")
        access_key = cfg.get("aws_access_key_id") or os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = cfg.get("aws_secret_access_key") or os.environ.get("AWS_SECRET_ACCESS_KEY")
        session_token = cfg.get("aws_session_token") or os.environ.get("AWS_SESSION_TOKEN")
        region = cfg.get("aws_region") or os.environ.get("AWS_REGION")
        self.aws_service = cfg.get("aws_service") or os.environ.get("AWS_SERVICE")

        if access_key and secret_key:
            self.session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=region,
            )
        elif profile:
            self.session = boto3.session.Session(profile_name=profile, region_name=region)
        else:
            # let default chain try
            self.session = boto3.session.Session(region_name=region)

        self.region = self.session.region_name
        self.credentials = self.session.get_credentials() if self.session else None

        if self.create_signed_credentials and self.credentials and self.aws_service and self.region:
            # NB: requests-aws4auth uses 'session_token' (not aws_session)
            self.aws_auth = AWS4Auth(
                self.credentials.access_key,
                self.credentials.secret_key,
                self.region,
                self.aws_service,
                session_token=self.credentials.token,
            )

    def get_awsauth(self) -> Optional[AWS4Auth]:
        return self.aws_auth


class ConfigurableOAuthAuthenticator(OAuthAuthenticator):
    """OAuth2 with flexible payload construction."""

    def get_initial_oauth_token(self) -> None:
        if not self.is_token_valid():
            self.update_access_token()
        self.auth_headers["Authorization"] = f"Bearer {self.access_token}"

    @property
    def oauth_request_body(self) -> dict:
        # accept both tap- and stream-level config
        my_config = getattr(self, "config", None) or getattr(self, "_config", {}) or {}
        required = my_config.get("grant_type")
        if not required:
            raise ValueError("Missing grant_type for OAuth token request.")

        # optional fields
        body = {"grant_type": required}
        for k in (
            "client_id", "client_secret", "username", "password",
            "refresh_token", "scope", "redirect_uri"
        ):
            v = my_config.get(k)
            if v:
                body[k] = v

        extras = my_config.get("oauth_extras") or {}
        body.update(extras)
        return body


def _resolve_config(self) -> dict:
    return getattr(self, "config", None) or getattr(self, "_config", {}) or {}


def select_authenticator(self) -> Any:
    cfg = _resolve_config(self)
    method = cfg.get("auth_method", "no_auth")
    api_keys = cfg.get("api_keys") or {}
    auth_headers = cfg.get("headers") or {}

    if method == "api_key":
        # Use the first key/value as header auth via SDK helper
        if not api_keys:
            raise ValueError("auth_method=api_key but no api_keys provided.")
        (key, value), *_ = api_keys.items()
        return APIKeyAuthenticator(stream=self, key=key, value=value)

    if method == "basic":
        return BasicAuthenticator(stream=self, username=cfg.get("username", ""), password=cfg.get("password", ""))

    if method == "oauth":
        return ConfigurableOAuthAuthenticator(
            stream=self,
            auth_endpoint=cfg.get("access_token_url", ""),
            oauth_scopes=cfg.get("scope", ""),
            default_expiration=cfg.get("oauth_expiration_secs"),
            oauth_headers=auth_headers or None,
        )

    if method == "bearer_token":
        return BearerTokenAuthenticator(stream=self, token=cfg.get("bearer_token", ""))

    if method == "aws":
        client = AWSConnectClient(cfg.get("aws_credentials") or {})
        self.http_auth = client.get_awsauth()
        return self.http_auth

    if method == "no_auth":
        return APIAuthenticatorBase(stream=self)

    raise ValueError(
        f"Unknown auth_method '{method}'. Use one of: no_auth, api_key, basic, oauth, bearer_token, aws."
    )


def get_authenticator(self) -> Any:
    cfg = _resolve_config(self)
    method = cfg.get("auth_method", "no_auth")
    if not getattr(self, "_authenticator", None):
        self._authenticator = select_authenticator(self)
    elif method == "oauth" and hasattr(self._authenticator, "is_token_valid") and not self._authenticator.is_token_valid():
        # refresh token
        self._authenticator = select_authenticator(self)
    if method == "aws":
        # http_auth is used directly on requests (not SDK headers)
        self.http_auth = self._authenticator
    return self._authenticator
