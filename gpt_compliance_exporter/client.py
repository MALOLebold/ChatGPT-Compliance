from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener


API_BASE = "https://api.chatgpt.com/v1/compliance"


class ComplianceAPIError(RuntimeError):
    """Raised for Compliance API HTTP or transport failures."""

    def __init__(self, message: str, *, status: Optional[int] = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


def scope_segment_for_principal(principal_id: str) -> str:
    return "organizations" if principal_id.startswith("org-") else "workspaces"


def scope_type_for_principal(principal_id: str) -> str:
    return "organization" if principal_id.startswith("org-") else "workspace"


@dataclass(frozen=True)
class ComplianceClient:
    api_key: str
    api_base: str = API_BASE
    timeout_seconds: int = 60

    def build_url(
        self,
        principal_id: str,
        path: str,
        query: Optional[Dict[str, Any]] = None,
    ) -> str:
        scope_segment = scope_segment_for_principal(principal_id)
        url = f"{self.api_base.rstrip('/')}/{scope_segment}/{principal_id}/{path.lstrip('/')}"
        if query:
            cleaned = {
                key: str(value)
                for key, value in query.items()
                if value is not None
            }
            if cleaned:
                url = f"{url}?{urlencode(cleaned)}"
        return url

    def list_logs(
        self,
        *,
        principal_id: str,
        event_type: str,
        limit: int,
        after: str,
    ) -> Dict[str, Any]:
        url = self.build_url(
            principal_id,
            "logs",
            {
                "limit": limit,
                "event_type": event_type,
                "after": after,
            },
        )
        response = self._request_text(url, description="listing Compliance Logs")
        try:
            parsed = json.loads(response)
        except json.JSONDecodeError as exc:
            raise ComplianceAPIError("Compliance Logs listing returned invalid JSON") from exc
        if not isinstance(parsed, dict):
            raise ComplianceAPIError("Compliance Logs listing did not return a JSON object")
        return parsed

    def download_log(self, *, principal_id: str, log_id: str) -> str:
        url = self.build_url(principal_id, f"logs/{log_id}")
        return self._request_text(url, description=f"downloading Compliance Log {log_id}")

    def _request_text(self, url: str, *, description: str) -> str:
        request = Request(
            url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
                "User-Agent": "gpt-compliance-exporter/0.1.0",
            },
            method="GET",
        )
        opener = build_opener()
        try:
            with opener.open(request, timeout=self.timeout_seconds) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ComplianceAPIError(
                f"HTTP {exc.code} while {description}",
                status=exc.code,
                body=body,
            ) from exc
        except URLError as exc:
            raise ComplianceAPIError(f"Network error while {description}: {exc.reason}") from exc
