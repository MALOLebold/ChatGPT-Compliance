from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, build_opener


GRAPH_BASE = "https://graph.microsoft.com/v1.0"
POWER_BI_BASE = "https://api.powerbi.com/v1.0/myorg"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
GRAPH_SIMPLE_UPLOAD_MAX_BYTES = 250 * 1024 * 1024
XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Dict[str, str]


HttpRequest = Callable[[str, str], HttpResponse]


class MicrosoftCloudError(RuntimeError):
    """Raised for Microsoft identity, Graph, or Power BI API failures."""

    def __init__(self, message: str, *, status: Optional[int] = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass(frozen=True)
class ClientCredentials:
    tenant_id: str
    client_id: str
    client_secret: str


@dataclass(frozen=True)
class SharePointUploadConfig:
    site_id: Optional[str] = None
    site_url: Optional[str] = None
    drive_id: Optional[str] = None
    drive_name: str = "Documents"
    folder_path: str = ""
    filename: str = "compliance_findings.xlsx"


@dataclass(frozen=True)
class PowerBIRefreshConfig:
    workspace_id: str
    dataset_id: str
    notify_option: Optional[str] = None


class MicrosoftCloudClient:
    def __init__(
        self,
        credentials: ClientCredentials,
        *,
        graph_base: str = GRAPH_BASE,
        power_bi_base: str = POWER_BI_BASE,
        timeout_seconds: int = 60,
        http_request: Optional[Callable[..., HttpResponse]] = None,
    ) -> None:
        self.credentials = credentials
        self.graph_base = graph_base.rstrip("/")
        self.power_bi_base = power_bi_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._http_request = http_request or http_request_urllib

    def upload_xlsx_to_sharepoint(self, config: SharePointUploadConfig, xlsx_path: Path) -> Dict[str, Any]:
        if not xlsx_path.exists():
            raise FileNotFoundError(f"SharePoint upload source file not found: {xlsx_path}")
        file_size = xlsx_path.stat().st_size
        if file_size > GRAPH_SIMPLE_UPLOAD_MAX_BYTES:
            raise MicrosoftCloudError("Microsoft Graph simple upload supports files up to 250 MB.")

        token = self._access_token(GRAPH_SCOPE)
        site_id = config.site_id or self._resolve_site_id(config.site_url, token)
        drive_id = config.drive_id or self._resolve_drive_id(site_id, config.drive_name, token)
        upload_path = _sharepoint_upload_path(config.folder_path, config.filename)
        url = f"{self.graph_base}/drives/{quote(drive_id, safe='')}/root:/{upload_path}:/content"

        response = self._request(
            "PUT",
            url,
            token=token,
            headers={"Content-Type": XLSX_CONTENT_TYPE},
            data=xlsx_path.read_bytes(),
            expected_statuses={200, 201},
            description="uploading workbook to SharePoint",
        )
        return _decode_json_response(response, "SharePoint upload")

    def trigger_powerbi_refresh(self, config: PowerBIRefreshConfig) -> Dict[str, Any]:
        token = self._access_token(POWER_BI_SCOPE)
        url = (
            f"{self.power_bi_base}/groups/{quote(config.workspace_id, safe='')}"
            f"/datasets/{quote(config.dataset_id, safe='')}/refreshes"
        )
        payload: Dict[str, Any] = {}
        if config.notify_option:
            payload["notifyOption"] = config.notify_option

        response = self._request(
            "POST",
            url,
            token=token,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload).encode("utf-8"),
            expected_statuses={200, 202},
            description="triggering Power BI refresh",
        )
        return {
            "status": response.status,
            "location": response.headers.get("Location") or response.headers.get("location"),
            "request_id": response.headers.get("x-ms-request-id") or response.headers.get("X-Ms-Request-Id"),
        }

    def _access_token(self, scope: str) -> str:
        token_url = f"https://login.microsoftonline.com/{quote(self.credentials.tenant_id, safe='')}/oauth2/v2.0/token"
        body = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.credentials.client_id,
                "client_secret": self.credentials.client_secret,
                "scope": scope,
            }
        ).encode("utf-8")
        response = self._request(
            "POST",
            token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=body,
            expected_statuses={200},
            description=f"acquiring token for {scope}",
        )
        parsed = _decode_json_response(response, "Microsoft identity token")
        token = parsed.get("access_token")
        if not isinstance(token, str) or not token:
            raise MicrosoftCloudError("Microsoft identity token response did not include access_token.")
        return token

    def _resolve_site_id(self, site_url: Optional[str], token: str) -> str:
        if not site_url:
            raise MicrosoftCloudError("SharePoint site ID or site URL is required.")
        parsed = urlparse(site_url)
        if not parsed.netloc or not parsed.path:
            raise MicrosoftCloudError("SharePoint site URL must look like https://tenant.sharepoint.com/sites/site-name.")
        site_path = quote(parsed.path.rstrip("/"), safe="/")
        url = f"{self.graph_base}/sites/{parsed.netloc}:{site_path}"
        response = self._request("GET", url, token=token, expected_statuses={200}, description="resolving SharePoint site")
        parsed_site = _decode_json_response(response, "SharePoint site")
        site_id = parsed_site.get("id")
        if not isinstance(site_id, str) or not site_id:
            raise MicrosoftCloudError("SharePoint site response did not include id.")
        return site_id

    def _resolve_drive_id(self, site_id: str, drive_name: str, token: str) -> str:
        if not drive_name:
            raise MicrosoftCloudError("SharePoint drive ID or drive name is required.")
        url = f"{self.graph_base}/sites/{quote(site_id, safe='')}/drives"
        response = self._request("GET", url, token=token, expected_statuses={200}, description="resolving SharePoint drive")
        parsed = _decode_json_response(response, "SharePoint drives")
        drives = parsed.get("value")
        if not isinstance(drives, list):
            raise MicrosoftCloudError("SharePoint drives response did not include a value list.")
        for drive in drives:
            if isinstance(drive, dict) and str(drive.get("name", "")).lower() == drive_name.lower():
                drive_id = drive.get("id")
                if isinstance(drive_id, str) and drive_id:
                    return drive_id
        raise MicrosoftCloudError(f"Could not find SharePoint drive named {drive_name!r}.")

    def _request(
        self,
        method: str,
        url: str,
        *,
        token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[bytes] = None,
        expected_statuses: set[int],
        description: str,
    ) -> HttpResponse:
        request_headers = dict(headers or {})
        request_headers.setdefault("Accept", "application/json")
        if token:
            request_headers["Authorization"] = f"Bearer {token}"
        response = self._http_request(
            method,
            url,
            headers=request_headers,
            data=data,
            timeout_seconds=self.timeout_seconds,
        )
        if response.status not in expected_statuses:
            body = response.body.decode("utf-8", errors="replace")
            raise MicrosoftCloudError(
                f"HTTP {response.status} while {description}",
                status=response.status,
                body=body,
            )
        return response


def http_request_urllib(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    timeout_seconds: int = 60,
) -> HttpResponse:
    request = Request(url, data=data, headers=headers or {}, method=method)
    opener = build_opener()
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return HttpResponse(
                status=response.status,
                body=response.read(),
                headers=dict(response.headers.items()),
            )
    except HTTPError as exc:
        return HttpResponse(
            status=exc.code,
            body=exc.read(),
            headers=dict(exc.headers.items()),
        )
    except URLError as exc:
        raise MicrosoftCloudError(f"Network error calling Microsoft API: {exc.reason}") from exc


def _decode_json_response(response: HttpResponse, description: str) -> Dict[str, Any]:
    if not response.body:
        return {}
    try:
        parsed = json.loads(response.body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise MicrosoftCloudError(f"{description} response returned invalid JSON.") from exc
    if not isinstance(parsed, dict):
        raise MicrosoftCloudError(f"{description} response did not return a JSON object.")
    return parsed


def _sharepoint_upload_path(folder_path: str, filename: str) -> str:
    if not filename or "/" in filename or "\\" in filename:
        raise MicrosoftCloudError("SharePoint filename must be a non-empty file name, not a path.")
    cleaned_folder = folder_path.strip().strip("/\\")
    raw_path = f"{cleaned_folder}/{filename}" if cleaned_folder else filename
    return quote(raw_path.replace("\\", "/"), safe="/")
