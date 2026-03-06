"""ETL pipeline: fetch data from the autochecker API and load it into the database.
The autochecker dashboard API provides two endpoints:
GET /api/items — lab/task catalog
GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)
Both require HTTP Basic Auth (email + password from settings).
"""
from datetime import datetime, timezone
from typing import Any
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog
import httpx


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------

async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.
    
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        url = f"{settings.autochecker_api_url}/api/items"
        auth = (settings.autochecker_email, settings.autochecker_password)
        
        response = await client.get(url, auth=auth)
        response.raise_for_status()
        
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.
    
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs = []
    
    async with httpx.AsyncClient() as client:
        url = f"{settings.autochecker_api_url}/api/logs"
        auth = (settings.autochecker_email, settings.autochecker_password)
        
        # Build initial query parameters
        params = {"limit": 500}
        if since is not None:
            # Convert datetime to ISO format string for the API
            params["since"] = since.isoformat()
        
        while True:
            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            
            data = response.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)
            
            # Check if there are more pages to fetch
            has_more = data.get("has_more", False)
            if not has_more or not logs:
                break
            
            # Use the last log's submitted_at as the new "since" value
            # This ensures we get the next batch without duplicates
            last_log = logs[-1]
            params["since"] = last_log.get("submitted_at", params["since"])
        
        return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------

async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.
    
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
         "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
         "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    new_items_count = 0
    
    # Build a mapping from lab short ID (e.g., "lab-01") to ItemRecord
    # This is crucial for tasks to find their parent lab
    lab_id_to_record: dict[str, ItemRecord] = {}
    
    # Process labs FIRST (parent items must exist before children)
    for item in items:
        if item.get("type") != "lab":
            continue
        
        lab_short_id = item.get("lab", "")
        title = item.get("title", "")
        
        # Check if this lab already exists in the database
        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == title
        )
        result = await session.exec(statement)
        existing_lab = result.first()
        
        if existing_lab is None:
            # Create new lab record
            new_lab = ItemRecord(
                type="lab",
                title=title,
                description=item.get("description", ""),
                attributes={"lab_id": lab_short_id}  # Store short ID in attributes
            )
            session.add(new_lab)
            await session.flush()  # Get the ID before committing
            existing_lab = new_lab
            new_items_count += 1
        
        # Map the short ID to the database record for task lookup
        lab_id_to_record[lab_short_id] = existing_lab
    
    # Process tasks SECOND (after labs are created)
    for item in items:
        if item.get("type") != "task":
            continue
        
        lab_short_id = item.get("lab", "")
        task_title = item.get("title", "")
        
        # Find the parent lab using the short ID mapping
        parent_lab = lab_id_to_record.get(lab_short_id)
        if parent_lab is None:
            # Skip task if parent lab doesn't exist (shouldn't happen)
            continue
        
        # Check if this task already exists
        statement = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == task_title,
            ItemRecord.parent_id == parent_lab.id
        )
        result = await session.exec(statement)
        existing_task = result.first()
        
        if existing_task is None:
            # Create new task record with parent_id
            new_task = ItemRecord(
                type="task",
                title=task_title,
                description=item.get("description", ""),
                parent_id=parent_lab.id,
                attributes={"lab_id": lab_short_id, "task_id": item.get("task", "")}
            )
            session.add(new_task)
            new_items_count += 1
    
    # Commit all changes
    await session.commit()
    
    return new_items_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.
    
    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.
    
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
          (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    new_interactions_count = 0
    
    # Build lookup: (lab_short_id, task_short_id) -> item title
    # This maps API short IDs to database titles
    short_id_to_title: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_id = item.get("lab", "")
        task_id = item.get("task")  # Can be None for labs
        title = item.get("title", "")
        short_id_to_title[(lab_id, task_id)] = title
    
    for log in logs:
        # Step 1: Find or create learner (find-or-create pattern)
        student_id = str(log.get("student_id", ""))
        student_group = log.get("group", "")
        
        statement = select(Learner).where(Learner.external_id == student_id)
        result = await session.exec(statement)
        learner = result.first()
        
        if learner is None:
            # Create new learner
            learner = Learner(
                external_id=student_id,
                student_group=student_group,
                enrolled_at=datetime.now(timezone.utc).replace(tzinfo=None)
            )
            session.add(learner)
            await session.flush()  # Get the ID
        
        # Step 2: Find the matching item in the database
        lab_short_id = log.get("lab", "")
        task_short_id = log.get("task")  # Can be None
        
        # Get the title from our lookup table
        item_title = short_id_to_title.get((lab_short_id, task_short_id))
        
        if item_title is None:
            # Skip this log if we can't find the corresponding item
            continue
        
        # Query the DB for the ItemRecord with this title
        statement = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.exec(statement)
        item = result.first()
        
        if item is None:
            # Skip if item doesn't exist in DB
            continue
        
        # Step 3: Check for idempotent upsert (skip if external_id exists)
        # Note: InteractionLog.external_id is int | None per the model
        log_external_id = log.get("id")
        if log_external_id is not None:
            try:
                log_external_id = int(log_external_id)
            except (ValueError, TypeError):
                log_external_id = None
        
        if log_external_id is not None:
            statement = select(InteractionLog).where(
                InteractionLog.external_id == log_external_id
            )
            result = await session.exec(statement)
            existing_log = result.first()
            
            if existing_log is not None:
                # Skip duplicate logs (idempotent)
                continue
        
        # Step 4: Create new InteractionLog
        # Parse submitted_at timestamp
        submitted_at_str = log.get("submitted_at", "")
        try:
            created_at = datetime.fromisoformat(submitted_at_str.replace("Z", "+00:00"))
            created_at = created_at.replace(tzinfo=None)  # Store without timezone
        except (ValueError, AttributeError):
            created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        
        new_interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at
        )
        session.add(new_interaction)
        new_interactions_count += 1
    
    # Commit all changes
    await session.commit()
    
    return new_interactions_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.
    
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    # Step 1: Fetch and load items (this CALLS the API)
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)
    
    # Step 2: Determine the last synced timestamp
    statement = select(InteractionLog).order_by(InteractionLog.created_at.desc())
    result = await session.exec(statement)
    last_log = result.first()
    
    if last_log is not None:
        since = last_log.created_at
    else:
        since = None
    
    # Step 3: Fetch and load logs (this CALLS the API)
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items_catalog, session)
    
    # Get total count of interactions in DB
    statement = select(InteractionLog)
    result = await session.exec(statement)
    all_logs = result.all()
    total_records = len(all_logs)
    
    return {
        "new_records": new_records,
        "total_records": total_records
    }