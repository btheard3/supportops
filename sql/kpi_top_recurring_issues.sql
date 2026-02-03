/*
KPI 5: Top Recurring Issues (Tags)

Definition:
Most frequently occurring issue classifications derived from tag fields.

Grain:
1 row = (queue, tag)

Purpose:
- Identify systemic problem areas
- Inform root cause analysis
- Highlight self-service and deflection opportunities
- Enable queue-specific prioritization

Notes:
- Tags are reshaped into long format
- One ticket can have multiple tags
- DISTINCT ticket_id + tag prevents double counting
*/

WITH fact_tickets AS (
    SELECT
        ROW_NUMBER() OVER () AS ticket_id,
        LOWER(TRIM(queue)) AS queue,
        tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8
    FROM dataset_tickets
),

tag_long AS (
    -- One row per ticket_id + tag
    SELECT
        ticket_id,
        queue,
        LOWER(TRIM(tag)) AS tag
    FROM fact_tickets
    UNPIVOT (
        tag FOR tag_col IN (tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8)
    )
    WHERE tag IS NOT NULL
      AND TRIM(tag) <> ''
),

ticket_tag_distinct AS (
    -- Prevent double counting same tag on same ticket
    SELECT DISTINCT
        ticket_id,
        queue,
        tag
    FROM tag_long
),

tag_counts AS (
    SELECT
        queue,
        tag,
        COUNT(*) AS ticket_count
    FROM ticket_tag_distinct
    GROUP BY queue, tag
)

-- FINAL OUTPUT (ONE SELECT)
SELECT
    queue,
    tag,
    ticket_count
FROM tag_counts
ORDER BY ticket_count DESC;
