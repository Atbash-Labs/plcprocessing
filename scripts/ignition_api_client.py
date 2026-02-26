#!/usr/bin/env python3
"""
HTTP client wrapping the Ignition Gateway REST API.

Provides read-only access to:
- Projects and entity browsing
- Gateway overview and connections
- Tag providers and OPC connections
- Tag reads (via WebDev /system/webdev/Axilon/getTags endpoint)
- Alarm pipeline states

Configuration via environment variables:
    IGNITION_API_URL    Base URL (e.g., http://localhost:9074)
    IGNITION_API_TOKEN  Optional Bearer token for authentication
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from urllib.parse import urljoin, quote

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class TagValue:
    """Result from a tag read."""
    path: str
    value: Any = None
    quality: str = "Unknown"
    timestamp: Optional[str] = None
    data_type: Optional[str] = None
    config: Optional[Dict] = None
    error: Optional[str] = None


@dataclass
class GatewayOverview:
    """Gateway status information."""
    version: Optional[str] = None
    state: Optional[str] = None
    platform: Optional[str] = None
    uptime_ms: Optional[int] = None
    edition: Optional[str] = None
    extra: Dict = field(default_factory=dict)


@dataclass
class OpcConnection:
    """OPC connection status."""
    name: str = ""
    status: str = "Unknown"
    server_type: Optional[str] = None
    extra: Dict = field(default_factory=dict)


class IgnitionApiClient:
    """
    Read-only HTTP client for the Ignition Gateway REST API.

    All methods are GET-only; no mutations are made to the live system.
    Methods return typed dataclasses or dicts and gracefully handle errors.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
        timeout: float = 15.0,
    ):
        self.base_url = (base_url or os.getenv("IGNITION_API_URL", "")).rstrip("/")
        self.api_token = api_token or os.getenv("IGNITION_API_TOKEN", "")
        self.timeout = timeout

        self._session = requests.Session()
        if self.api_token:
            self._session.headers["Authorization"] = f"Bearer {self.api_token}"
        self._session.headers["Accept"] = "application/json"

    @property
    def is_configured(self) -> bool:
        return bool(self.base_url)

    def _url(self, path: str) -> str:
        """Build full URL from a relative path."""
        return f"{self.base_url}/{path.lstrip('/')}"

    def _get(self, path: str, params: Optional[Dict] = None) -> Optional[Any]:
        """Execute a GET request and return parsed JSON (or None on error)."""
        if not self.is_configured:
            return None

        url = self._url(path)
        try:
            resp = self._session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.warning("Ignition API request failed: %s %s – %s", "GET", url, exc)
            return None
        except (ValueError, json.JSONDecodeError) as exc:
            logger.warning("Ignition API returned non-JSON: %s – %s", url, exc)
            return None

    # --------------------------------------------------------------------- #
    #  Projects
    # --------------------------------------------------------------------- #

    def list_projects(self) -> List[Dict]:
        """List all projects on the gateway."""
        data = self._get("data/api/v1/projects/list")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("projects", data.get("items", [data]))
        return []

    def get_project(self, name: str) -> Optional[Dict]:
        """Get a single project by name."""
        return self._get(f"data/api/v1/projects/find/{quote(name, safe='')}")

    # --------------------------------------------------------------------- #
    #  Entity browsing
    # --------------------------------------------------------------------- #

    def browse_entities(
        self, path: str = "", depth: int = 1
    ) -> List[Dict]:
        """Browse the entity tree from a starting path."""
        params: Dict[str, Any] = {"depth": depth}
        if path:
            params["path"] = path
        data = self._get("data/api/v1/entity/browse", params=params)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("entities", data.get("children", []))
        return []

    # --------------------------------------------------------------------- #
    #  Gateway overview
    # --------------------------------------------------------------------- #

    def get_gateway_overview(self) -> Optional[GatewayOverview]:
        """Get gateway status, version, uptime, etc."""
        data = self._get("data/api/v1/overview")
        if not data:
            return None

        return GatewayOverview(
            version=data.get("version"),
            state=data.get("state"),
            platform=data.get("platform"),
            uptime_ms=data.get("uptimeMs") or data.get("uptime_ms"),
            edition=data.get("edition"),
            extra={k: v for k, v in data.items()
                   if k not in ("version", "state", "platform", "uptimeMs", "uptime_ms", "edition")},
        )

    def get_connections(self) -> List[OpcConnection]:
        """Get OPC / database connection status from the gateway."""
        data = self._get("data/api/v1/overview/connections")
        if not data:
            return []

        items = data if isinstance(data, list) else data.get("connections", [])
        results: List[OpcConnection] = []
        for item in items:
            if isinstance(item, dict):
                results.append(OpcConnection(
                    name=item.get("name", ""),
                    status=item.get("status", "Unknown"),
                    server_type=item.get("type") or item.get("serverType"),
                    extra={k: v for k, v in item.items()
                           if k not in ("name", "status", "type", "serverType")},
                ))
        return results

    # --------------------------------------------------------------------- #
    #  Tag providers & OPC connections (resource config API)
    # --------------------------------------------------------------------- #

    def get_tag_providers(self) -> List[Dict]:
        """Get configured tag providers."""
        data = self._get("data/api/v1/resource/config/tagproviders")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("providers", data.get("items", []))
        return []

    def get_opc_connections(self) -> List[Dict]:
        """Get configured OPC server connections."""
        data = self._get("data/api/v1/resource/config/opcconnections")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("connections", data.get("items", []))
        return []

    # --------------------------------------------------------------------- #
    #  Tags – WebDev module endpoint
    # --------------------------------------------------------------------- #

    def read_tag(self, path: str) -> TagValue:
        """Read a single tag's current value, quality, config, and timestamp."""
        return self.read_tags([path])[0]

    def read_tags(self, paths: List[str]) -> List[TagValue]:
        """Read multiple tags in a single call via the WebDev getTags endpoint.

        Endpoint: /system/webdev/Axilon/getTags?tagPaths=path1,path2,...
        Paths should include the provider prefix, e.g. [default]Folder/Tag.
        """
        if not paths:
            return []

        normalised = [self._ensure_provider_prefix(p) for p in paths]
        tag_paths_param = ",".join(normalised)

        data = self._get(
            f"system/webdev/Axilon/getTags?tagPaths={tag_paths_param}",
        )

        if data is None:
            return [
                TagValue(path=p, error="API request failed or not configured")
                for p in normalised
            ]

        return self._parse_tags_response(normalised, data)

    # --------------------------------------------------------------------- #
    #  Tag history – WebDev module endpoint
    # --------------------------------------------------------------------- #

    @staticmethod
    def _local_iso_to_utc(dt_str: str) -> str:
        """Convert a bare ISO datetime string (assumed local) to UTC.

        If the string already has a timezone indicator (Z, +, -)
        or looks like epoch milliseconds, it is returned unchanged.
        """
        from datetime import datetime, timezone

        s = str(dt_str).strip()

        # Epoch ms – pass through
        if s.isdigit():
            return s

        # Already has TZ info – pass through
        if s.endswith("Z") or "+" in s[10:] or s[10:].count("-") > 0:
            return s

        try:
            naive = datetime.fromisoformat(s)
            local_dt = naive.astimezone()          # attach local TZ
            utc_dt = local_dt.astimezone(timezone.utc)
            return utc_dt.strftime("%Y-%m-%dT%H:%M:%S")
        except (ValueError, TypeError):
            return s

    def query_tag_history(
        self,
        tag_paths: List[str],
        start_date: str,
        end_date: str,
        return_size: int = 100,
        aggregation_mode: str = "Average",
        return_format: str = "Wide",
        interval_minutes: Optional[int] = None,
        include_bounding_values: bool = False,
    ) -> Optional[Any]:
        """Query historical tag values via the WebDev queryTagHistory endpoint.

        Bare ISO datetime strings (no timezone suffix) are assumed to be in
        the server's local timezone and are converted to UTC before sending
        to the gateway (which interprets all times as UTC).

        Args:
            tag_paths: Tag paths with provider prefix, e.g. ['[default]Folder/Tag'].
            start_date: ISO datetime string (local) or epoch ms.
            end_date: ISO datetime string (local) or epoch ms.
            return_size: Max rows to return (default 100).
            aggregation_mode: Average, MinMax, LastValue, Sum, Minimum, Maximum.
            return_format: Wide or Tall.
            interval_minutes: Aggregation interval in minutes.
            include_bounding_values: Include values at boundaries.
        """
        normalised = [self._ensure_provider_prefix(p) for p in tag_paths]

        utc_start = self._local_iso_to_utc(start_date)
        utc_end = self._local_iso_to_utc(end_date)

        params: Dict[str, Any] = {
            "tagPaths": ",".join(normalised),
            "startDate": utc_start,
            "endDate": utc_end,
            "returnSize": return_size,
            "aggregationMode": aggregation_mode,
            "returnFormat": return_format,
            "includeBoundingValues": str(include_bounding_values).lower(),
        }
        if interval_minutes is not None:
            params["intervalMinutes"] = interval_minutes

        data = self._get("system/webdev/Axilon/queryTagHistory", params=params)

        if data is None:
            return {"error": "API request failed or not configured", "tagPaths": normalised}

        return data

    # --------------------------------------------------------------------- #
    #  Alarm pipelines
    # --------------------------------------------------------------------- #

    def get_alarm_pipelines(self) -> List[Dict]:
        """Get alarm notification pipeline states."""
        data = self._get("data/alarm-notification/api/v1/pipelines")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("pipelines", data.get("items", []))
        return []

    # --------------------------------------------------------------------- #
    #  Helpers
    # --------------------------------------------------------------------- #

    @staticmethod
    def _ensure_provider_prefix(path: str) -> str:
        """Ensure the path has a provider prefix like [default].

        The WebDev getTags endpoint expects paths with the provider prefix.
        If a caller passes a bare path, prepend [default].
        """
        if path.startswith("["):
            return path
        return f"[default]{path}"

    _TAG_ITEM_KNOWN_KEYS = {"value", "quality", "tagPath", "isGood",
                             "timestamp", "t", "dataType", "data_type"}

    @staticmethod
    def _parse_tags_response(paths: List[str], data: Any) -> List["TagValue"]:
        """Parse the response from the WebDev getTags endpoint.

        Expected shape: {"allGood": bool, "success": bool, "count": N,
                         "tags": [{tagPath, value, quality, isGood}, ...]}
        """
        if isinstance(data, dict):
            items = (data.get("tags")
                     or data.get("results")
                     or data.get("items")
                     or [])
            if not items and "value" in data:
                items = [data]
        elif isinstance(data, list):
            items = data
        else:
            return [TagValue(path=p, value=data, quality="Unknown") for p in paths]

        by_path: Dict[str, dict] = {}
        for item in items:
            if isinstance(item, dict) and "tagPath" in item:
                by_path[item["tagPath"]] = item

        results: List[TagValue] = []
        for i, path in enumerate(paths):
            item = by_path.get(path)
            if item is None and i < len(items) and isinstance(items[i], dict):
                item = items[i]

            if item is None:
                results.append(TagValue(path=path, error="No data returned for this path"))
            elif isinstance(item, dict):
                extra = {k: v for k, v in item.items()
                         if k not in IgnitionApiClient._TAG_ITEM_KNOWN_KEYS} or None
                results.append(TagValue(
                    path=item.get("tagPath", path),
                    value=item.get("value"),
                    quality=str(item.get("quality", "Good" if item.get("isGood") else "Unknown")),
                    timestamp=item.get("timestamp") or item.get("t"),
                    data_type=item.get("dataType") or item.get("data_type"),
                    config=extra,
                ))
            else:
                results.append(TagValue(path=path, value=item, quality="Unknown"))

        return results

    def close(self):
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ------------------------------------------------------------------ #
#  Quick CLI for manual testing
# ------------------------------------------------------------------ #

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Test Ignition API client")
    parser.add_argument("--url", help="Ignition gateway URL (or set IGNITION_API_URL)")
    parser.add_argument("--token", help="API token (or set IGNITION_API_TOKEN)")
    parser.add_argument("--overview", action="store_true", help="Show gateway overview")
    parser.add_argument("--connections", action="store_true", help="Show connections")
    parser.add_argument("--projects", action="store_true", help="List projects")
    parser.add_argument("--browse", metavar="PATH", nargs="?", const="", help="Browse entity tree")
    parser.add_argument("--tag-providers", action="store_true", help="List tag providers")
    parser.add_argument("--opc-connections", action="store_true", help="List OPC connections")
    parser.add_argument("--read-tag", metavar="PATH", help="Read a single tag")
    parser.add_argument("--alarms", action="store_true", help="Show alarm pipelines")

    args = parser.parse_args()

    client = IgnitionApiClient(base_url=args.url, api_token=args.token)

    if not client.is_configured:
        print("[ERROR] No API URL configured. Pass --url or set IGNITION_API_URL.")
        return

    if args.overview:
        overview = client.get_gateway_overview()
        if overview:
            print(json.dumps({
                "version": overview.version,
                "state": overview.state,
                "platform": overview.platform,
                "uptime_ms": overview.uptime_ms,
                "edition": overview.edition,
                **overview.extra,
            }, indent=2, default=str))
        else:
            print("[WARN] Could not fetch gateway overview")

    elif args.connections:
        conns = client.get_connections()
        for c in conns:
            print(f"  {c.name}: {c.status} ({c.server_type or 'N/A'})")

    elif args.projects:
        projects = client.list_projects()
        print(json.dumps(projects, indent=2, default=str))

    elif args.browse is not None:
        entities = client.browse_entities(args.browse)
        print(json.dumps(entities, indent=2, default=str))

    elif args.tag_providers:
        providers = client.get_tag_providers()
        print(json.dumps(providers, indent=2, default=str))

    elif args.opc_connections:
        conns = client.get_opc_connections()
        print(json.dumps(conns, indent=2, default=str))

    elif args.read_tag:
        tv = client.read_tag(args.read_tag)
        print(json.dumps({
            "path": tv.path,
            "value": tv.value,
            "quality": tv.quality,
            "timestamp": tv.timestamp,
            "data_type": tv.data_type,
            "error": tv.error,
        }, indent=2, default=str))

    elif args.alarms:
        pipelines = client.get_alarm_pipelines()
        print(json.dumps(pipelines, indent=2, default=str))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
