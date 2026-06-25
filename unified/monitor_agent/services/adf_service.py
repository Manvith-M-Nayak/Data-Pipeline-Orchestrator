import os
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import httpx

TENANT_ID       = os.getenv("AZURE_TENANT_ID", "")
CLIENT_ID       = os.getenv("AZURE_CLIENT_ID", "")
CLIENT_SECRET   = os.getenv("AZURE_CLIENT_SECRET", "")
SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "")
RESOURCE_GROUP  = os.getenv("AZURE_RESOURCE_GROUP", "")
FACTORY_NAME    = os.getenv("ADF_FACTORY_NAME", "")

ADF_API_VERSION = "2018-06-01"
MGMT_BASE       = "https://management.azure.com"


class ADFService:
    def __init__(self):
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0

    async def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token
        token_url = f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID', TENANT_ID)}/oauth2/token"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                token_url,
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     os.getenv("AZURE_CLIENT_ID", CLIENT_ID),
                    "client_secret": os.getenv("AZURE_CLIENT_SECRET", CLIENT_SECRET),
                    "resource":      "https://management.azure.com/",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        self._token = data["access_token"]
        self._token_expiry = time.time() + int(data.get("expires_in", 3600))
        return self._token

    def _factory_url(self, path: str) -> str:
        sub  = os.getenv("AZURE_SUBSCRIPTION_ID", SUBSCRIPTION_ID)
        rg   = os.getenv("AZURE_RESOURCE_GROUP", RESOURCE_GROUP)
        name = os.getenv("ADF_FACTORY_NAME", FACTORY_NAME)
        return (
            f"{MGMT_BASE}/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.DataFactory/factories/{name}"
            f"{path}?api-version={ADF_API_VERSION}"
        )

    async def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {await self._get_token()}", "Content-Type": "application/json"}

    async def get_active_pipeline_runs(self) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        body = {
            "lastUpdatedAfter":  (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lastUpdatedBefore": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "filters": [{"operand": "Status", "operator": "Equals", "values": ["InProgress"]}],
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self._factory_url("/queryPipelineRuns"),
                headers=await self._headers(), json=body, timeout=20,
            )
            resp.raise_for_status()
        return resp.json().get("value", [])

    async def get_recent_pipeline_runs(self, hours: int = 48) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        body = {
            "lastUpdatedAfter":  (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lastUpdatedBefore": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self._factory_url("/queryPipelineRuns"),
                headers=await self._headers(), json=body, timeout=20,
            )
            resp.raise_for_status()
        return resp.json().get("value", [])

    async def get_pipeline_run(self, run_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                self._factory_url(f"/pipelineRuns/{run_id}"),
                headers=await self._headers(), timeout=15,
            )
            resp.raise_for_status()
        return resp.json()

    async def get_activity_runs(self, run_id: str) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        body = {
            "lastUpdatedAfter":  (now - timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lastUpdatedBefore": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self._factory_url(f"/pipelineRuns/{run_id}/queryActivityruns"),
                headers=await self._headers(), json=body, timeout=20,
            )
            resp.raise_for_status()
        return resp.json().get("value", [])

    async def cancel_pipeline_run(self, run_id: str) -> bool:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self._factory_url(f"/pipelineRuns/{run_id}/cancel"),
                headers=await self._headers(), json={}, timeout=15,
            )
        return resp.status_code in (200, 202)

    async def get_all_pipelines(self) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                self._factory_url("/pipelines"),
                headers=await self._headers(), timeout=20,
            )
            resp.raise_for_status()
        return resp.json().get("value", [])
