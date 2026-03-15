"""
DIA v2 - Feedback Writer
=========================
Writes user feedback (thumbs up/down + optional comment) to Snowflake.
Table: DIA_AGENT_FEEDBACK (created by deploy/create_feedback_table.sql)
"""

import logging
import uuid
from datetime import datetime, timezone

import snowflake.connector
from config import SnowflakeConfig

logger = logging.getLogger("dia-v2.feedback")

INSERT_SQL = """
INSERT INTO DIA_AGENT_FEEDBACK (
    feedback_id, session_id, query_text, answer_text,
    sql_generated, intent, rating, feedback_text, submitted_at, app_version
) VALUES (
    %(feedback_id)s, %(session_id)s, %(query_text)s, %(answer_text)s,
    %(sql_generated)s, %(intent)s, %(rating)s, %(feedback_text)s,
    %(submitted_at)s, %(app_version)s
)
"""


def write_feedback(
    rating: int,                  # 1 = thumbs up, -1 = thumbs down
    query_text: str,
    answer_text: str | None = None,
    sql_generated: str | None = None,
    intent: str | None = None,
    feedback_text: str | None = None,
    session_id: str | None = None,
) -> bool:
    """Insert one feedback row into Snowflake. Returns True on success."""
    record = {
        "feedback_id":  str(uuid.uuid4()),
        "session_id":   session_id or "unknown",
        "query_text":   query_text[:4000],
        "answer_text":  (answer_text or "")[:8000],
        "sql_generated": sql_generated,
        "intent":       intent,
        "rating":       rating,
        "feedback_text": feedback_text,
        "submitted_at": datetime.now(timezone.utc),
        "app_version":  "v2.0",
    }

    try:
        conn = snowflake.connector.connect(**SnowflakeConfig.connection_params())
        cur = conn.cursor()
        cur.execute(INSERT_SQL, record)
        conn.commit()
        conn.close()
        logger.info(f"Feedback recorded: rating={rating}, session={record['session_id']}")
        return True
    except Exception as e:
        logger.error(f"Failed to write feedback: {e}")
        return False
