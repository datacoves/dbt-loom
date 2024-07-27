import os
import typing as t

import requests
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note
from pydantic import BaseModel

# from dbt_datacoves_mesh.constants import UNSET
UNSET = "unset"

class DatacovesReferenceConfig(BaseModel):
    """Configuration for a Datacoves reference."""

    project_name: str
    api_token: str = os.getenv(
        "DATACOVES__DBT_API_TOKEN", os.getenv("DATACOVES__SECRETS_TOKEN", UNSET)
    )
    api_endpoint: str = os.getenv(
        "DATACOVES__DBT_API_ENDPOINT",
        "http://core-dbt-api-svc.core.svc.cluster.local:80/api/internal",
    )


class Datacoves:
    """API Client for datacoves. Fetches latest manifest for a given dbt job."""

    def __init__(self, project_name: str, api_token: str, api_endpoint: str) -> None:
        self.project_name = project_name
        self.api_endpoint = api_endpoint
        self._headers = {
            "Authorization": "Bearer " + api_token,
            "Content-Type": "application/json",
        }

    def _query(self, endpoint: str, **kwargs) -> t.Dict:
        """Query the datacoves Jade API."""
        uri = f"{self.api_endpoint}/{endpoint}"
        fire_event(Note(msg=f"Querying datacoves API at {uri}"))
        response = requests.get(uri, headers=self._headers, **kwargs)
        return response.json()

    def _get_manifest(self) -> t.Dict[str, t.Any]:
        """Get the latest manifest json for a given project."""
        data = self._query(f"projects/{self.project_name}/latest-manifest?trimmed=true")
        return data

    def get_models(self) -> t.Dict[str, t.Any]:
        """Get the latest state of all models."""
        return self._get_manifest()
