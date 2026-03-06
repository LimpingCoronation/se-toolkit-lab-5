"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from sqlalchemy import func, text
from typing import List, Dict, Any

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()

# Some changes


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    - Find the lab item by matching title (e.g. "lab-01" → title contains "Lab 01")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    # Step 1: Find the lab item
    # The lab parameter is like "lab-01", but title might be "Lab 01" or similar
    # We search in attributes JSONB column which stores lab_id from ETL
    lab_id_pattern = f"%{lab}%"
    
    # First try to find by attributes->>lab_id
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.attributes[("lab_id",)].astext.like(lab_id_pattern)
    )
    result = await session.exec(statement)
    lab_item = result.first()
    
    # Fallback: search by title if attributes doesn't match
    if lab_item is None:
        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab}%")
        )
        result = await session.exec(statement)
        lab_item = result.first()
    
    if lab_item is None:
        # Return empty buckets if lab not found
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Step 2: Find all tasks that belong to this lab
    statement = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.exec(statement)
    task_ids = [row for row in result.all()]
    
    # Include the lab itself in item_ids (some interactions might be at lab level)
    item_ids = [lab_item.id] + task_ids
    
    # Step 3: Query interactions with scores and bucket them
    # Use raw SQL for CASE WHEN bucketing
    if item_ids:
        placeholders = ",".join([str(id) for id in item_ids])
        query = text(f"""
            SELECT 
                CASE 
                    WHEN score >= 0 AND score <= 25 THEN '0-25'
                    WHEN score > 25 AND score <= 50 THEN '26-50'
                    WHEN score > 50 AND score <= 75 THEN '51-75'
                    WHEN score > 75 AND score <= 100 THEN '76-100'
                    ELSE 'other'
                END AS bucket,
                COUNT(*) AS count
            FROM interacts
            WHERE item_id IN ({placeholders})
            AND score IS NOT NULL
            GROUP BY bucket
        """)
        result = await session.exec(query)
        bucket_counts = {row[0]: row[1] for row in result.all()}
    else:
        bucket_counts = {}
    
    # Step 4: Return all four buckets even if count is 0
    buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [
        {"bucket": bucket, "count": bucket_counts.get(bucket, 0)}
        for bucket in buckets
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    # Step 1: Find the lab item
    lab_id_pattern = f"%{lab}%"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.attributes[("lab_id",)].astext.like(lab_id_pattern)
    )
    result = await session.exec(statement)
    lab_item = result.first()
    
    if lab_item is None:
        # Fallback: search by title
        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab}%")
        )
        result = await session.exec(statement)
        lab_item = result.first()
    
    if lab_item is None:
        return []
    
    # Step 2: Find all tasks that belong to this lab
    statement = select(ItemRecord).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    ).order_by(ItemRecord.title)
    result = await session.exec(statement)
    tasks = result.all()
    
    # Step 3: For each task, compute avg_score and attempts
    results = []
    for task in tasks:
        # Query interactions for this task
        statement = select(
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        ).where(
            InteractionLog.item_id == task.id,
            InteractionLog.score.isnot(None)
        )
        result = await session.exec(statement)
        row = result.first()
        
        avg_score = float(row[0]) if row[0] is not None else 0.0
        attempts = row[1] if row[1] is not None else 0
        
        results.append({
            "task": task.title,
            "avg_score": round(avg_score, 1),
            "attempts": attempts
        })
    
    return results


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    # Step 1: Find the lab item
    lab_id_pattern = f"%{lab}%"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.attributes[("lab_id",)].astext.like(lab_id_pattern)
    )
    result = await session.exec(statement)
    lab_item = result.first()
    
    if lab_item is None:
        # Fallback: search by title
        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab}%")
        )
        result = await session.exec(statement)
        lab_item = result.first()
    
    if lab_item is None:
        return []
    
    # Step 2: Find all tasks that belong to this lab
    statement = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.exec(statement)
    task_ids = [row for row in result.all()]
    
    # Include the lab itself
    item_ids = [lab_item.id] + task_ids
    
    if not item_ids:
        return []
    
    # Step 3: Group interactions by date
    # Use func.date() to extract date from created_at
    statement = select(
        func.date(InteractionLog.created_at).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(item_ids)
    ).group_by(
        func.date(InteractionLog.created_at)
    ).order_by(
        func.date(InteractionLog.created_at)
    )
    
    result = await session.exec(statement)
    
    # Format results
    return [
        {"date": str(row[0]), "submissions": row[1]}
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    # Step 1: Find the lab item
    lab_id_pattern = f"%{lab}%"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.attributes[("lab_id",)].astext.like(lab_id_pattern)
    )
    result = await session.exec(statement)
    lab_item = result.first()
    
    if lab_item is None:
        # Fallback: search by title
        statement = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab}%")
        )
        result = await session.exec(statement)
        lab_item = result.first()
    
    if lab_item is None:
        return []
    
    # Step 2: Find all tasks that belong to this lab
    statement = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.exec(statement)
    task_ids = [row for row in result.all()]
    
    # Include the lab itself
    item_ids = [lab_item.id] + task_ids
    
    if not item_ids:
        return []
    
    # Step 3: Join interactions with learners and group by student_group
    # We need to use a join query
    statement = select(
        Learner.student_group.label("group"),
        func.avg(InteractionLog.score).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(item_ids),
        InteractionLog.score.isnot(None),
        Learner.student_group != ""
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    result = await session.exec(statement)
    
    # Format results
    return [
        {
            "group": row[0],
            "avg_score": round(float(row[1]), 1) if row[1] is not None else 0.0,
            "students": row[2]
        }
        for row in result.all()
    ]