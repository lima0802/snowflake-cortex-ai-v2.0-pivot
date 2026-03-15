-- =============================================================================
-- DIA v2 — Feedback Table
-- Run once to create the feedback collection table.
-- =============================================================================

CREATE TABLE IF NOT EXISTS DIA_AGENT_FEEDBACK (
    feedback_id     VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),   -- UUID
    session_id      VARCHAR(255),                                     -- browser/chat session
    query_text      TEXT            NOT NULL,                         -- user question
    answer_text     TEXT,                                             -- agent answer shown
    sql_generated   TEXT,                                             -- SQL that was run
    intent          VARCHAR(50),                                      -- classified intent
    rating          NUMBER(1,0)     NOT NULL,                         -- 1=thumbs up, -1=thumbs down
    feedback_text   TEXT,                                             -- optional free-text comment
    submitted_at    TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    app_version     VARCHAR(20)     DEFAULT 'v2.0'
);

-- View for quick analysis
CREATE OR REPLACE VIEW DIA_AGENT_FEEDBACK_SUMMARY AS
SELECT
    DATE_TRUNC('DAY', submitted_at)                                         AS feedback_date,
    intent,
    COUNT(*)                                                                AS total_responses,
    SUM(CASE WHEN rating = 1  THEN 1 ELSE 0 END)                           AS thumbs_up,
    SUM(CASE WHEN rating = -1 THEN 1 ELSE 0 END)                           AS thumbs_down,
    ROUND(SUM(CASE WHEN rating = 1 THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(*), 0), 1)                                         AS satisfaction_pct,
    COUNT(CASE WHEN feedback_text IS NOT NULL THEN 1 END)                  AS comments_count
FROM DIA_AGENT_FEEDBACK
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
