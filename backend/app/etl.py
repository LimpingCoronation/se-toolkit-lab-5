"""ETL pipeline: fetch data from the autochecker API and load it into the database.
The autochecker dashboard API provides two endpoints:
GET /api/items — lab/task catalog
GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)
Both require HTTP Basic Auth (email + password from settings).
"""
from datetime import datetime
from sqlmodel.ext.asyncio.session import AsyncSession
from app.settings import settings
import httpx  # Added import for HTTP requests

# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------

async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.
    
    - Uses httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Passes HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Returns the parsed list of dicts
    - Raises an exception if the response status is not 200
    """
    # We use AsyncClient to avoid blocking the event loop while waiting for the API
    async with httpx.AsyncClient() as client:
        # Construct the URL from settings
        url = f"{settings.autochecker_api_url}/api/items"
        
        # HTTP Basic Auth expects a tuple of (username, password)
        auth = (settings.autochecker_email, settings.autochecker_password)
        
        # Perform the GET request
        response = await client.get(url, auth=auth)
        
        # raise_for_status() will raise an HTTPError if status code is 4xx or 5xx
        # This satisfies the requirement to raise an exception if not 200
        response.raise_for_status()
        
        # Parse and return the JSON content
        return response.json()

async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.
    TODO: Implement this function.
    """
    raise NotImplementedError

# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------

async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.
    TODO: Implement this function.
    """
    raise NotImplementedError

async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.
    TODO: Implement this function.
    """
    raise NotImplementedError

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.
    TODO: Implement this function.
    """
    raise NotImplementedError