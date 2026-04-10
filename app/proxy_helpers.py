from __future__ import annotations

from typing import Mapping, Optional
from urllib.parse import quote


_DEFAULT_SCOPE_ALIASES = {
    "telegram": "telegram",
    "tg": "telegram",
    "telethon": "telegram",
    "service": "service",
    "hdhive": "service",
    "tmdb": "service",
    "hdhive_tmdb": "service",
    "downloader": "downloader",
    "download": "downloader",
    "yt": "downloader",
}

_PROXY_SCHEME_ALIASES = {
    "http": "http",
    "https": "https",
    "socks": "socks5",
    "socks5": "socks5",
    "socks5h": "socks5",
    "socks4": "socks4",
    "socks4a": "socks4",
}


def _parse_proxy_addr(addr: str, default_scheme: str) -> tuple[str, str]:
    """
    Parse optional proxy scheme prefix from address field.

    Examples:
      - "127.0.0.1" -> ("http", "127.0.0.1") for default http
      - "socks5://127.0.0.1" -> ("socks5", "127.0.0.1")
      - "http://proxy.example.com" -> ("http", "proxy.example.com")
    """
    raw = str(addr or "").strip()
    scheme = default_scheme
    if "://" in raw:
        candidate, _, host = raw.partition("://")
        mapped = _PROXY_SCHEME_ALIASES.get(str(candidate or "").strip().lower())
        if mapped:
            scheme = mapped
        raw = host.strip()
    return scheme, raw


def normalize_proxy_scope(scope: Optional[str]) -> str:
    value = str(scope or "service").strip().lower()
    return _DEFAULT_SCOPE_ALIASES.get(value, "service")


def sanitize_proxy_scope_config(scope_cfg: object) -> dict[str, str]:
    if not isinstance(scope_cfg, Mapping):
        return {}
    return {
        "addr": str(scope_cfg.get("addr") or "").strip(),
        "port": str(scope_cfg.get("port") or "").strip(),
        "username": str(scope_cfg.get("username") or "").strip(),
        "password": str(scope_cfg.get("password") or "").strip(),
    }


def extract_proxy_scope_config(
    config: Optional[dict],
    scope: Optional[str],
    *,
    allow_legacy_proxy: bool = True,
) -> dict[str, str]:
    proxy_cfg = config.get("proxy", {}) if isinstance(config, dict) else {}
    if not isinstance(proxy_cfg, Mapping):
        return {}

    normalized_scope = normalize_proxy_scope(scope)
    legacy_keys = {"addr", "port", "username", "password"}
    if allow_legacy_proxy and legacy_keys & set(proxy_cfg.keys()):
        if normalized_scope in ("telegram", "service"):
            return sanitize_proxy_scope_config(proxy_cfg)
        return {}

    return sanitize_proxy_scope_config(proxy_cfg.get(normalized_scope))


def build_proxy_url_from_scope_config(scope_cfg: Mapping[str, str]) -> Optional[str]:
    scheme, addr = _parse_proxy_addr(scope_cfg.get("addr") or "", "http")
    port = str(scope_cfg.get("port") or "").strip()
    if not addr or not port:
        return None

    username = str(scope_cfg.get("username") or "").strip()
    password = str(scope_cfg.get("password") or "").strip()
    auth = ""
    if username:
        auth = quote(username, safe="")
        if password:
            auth += ":" + quote(password, safe="")
        auth += "@"
    return f"{scheme}://{auth}{addr}:{port}"


def build_requests_proxies_from_scope_config(scope_cfg: Mapping[str, str]) -> Optional[dict[str, str]]:
    proxy_url = build_proxy_url_from_scope_config(scope_cfg)
    if not proxy_url:
        return None
    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def build_telethon_proxy_from_scope_config(scope_cfg: Mapping[str, str]) -> Optional[tuple]:
    protocol, addr = _parse_proxy_addr(scope_cfg.get("addr") or "", "socks5")
    port = str(scope_cfg.get("port") or "").strip()
    if not addr or not port:
        return None
    # Telethon only accepts socks5/socks4/http proxy kinds.
    if protocol not in {"socks5", "socks4", "http"}:
        protocol = "http"
    try:
        return (
            protocol,
            addr,
            int(port),
            True,
            scope_cfg.get("username") or None,
            scope_cfg.get("password") or None,
        )
    except Exception:
        return None
