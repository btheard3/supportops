/*
KPI 5: Top Recurring Issues (Tags)
----------------------------------
Definition:
Most frequent issue classifications derived from tag columns (tag_1 ... tag_8).

Grain:
1 row = 1 support ticket (fact)
Tags are many-to-many: one ticket can have multiple tags.

Purpose:
- Identify systemic problem areas (root cause targets)
- Inform self-service / deflection opportunities
- Enable queue-specific issue prioritization

Notes:
- Uses UNPIVOT to reshape wide tags -> long tags
- We count TAG OCCURRENCES (ticket_id, tag) distinct pairs to avoid double-counting
  if a tag appears multiple times across tag columns for the same ticket.
- Assumes Phase 2 standardization (trim + lowercase) is applied here too.
*/

WITH fact_tickets AS (
  SELECT
    ROW_NUMBER() OVER ()              AS ticket_id,
    LOWER(TRIM(queue))                AS queue,
    LOWER(TRIM(priority))             AS priority,
    LOWER(TRIM(type))                 AS type,
    LOWER(TRIM(language))             AS language,
    tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8
  FROM dataset_tickets
),

-- Reshape tags into long format (one row per ticket_id + tag)
tag_long AS (
  SELECT
    ticket_id,
    queue,
    LOWER(TRIM(tag)) AS tag
  FROM fact_tickets
  UNPIVOT (tag FOR tag_col IN (tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8))
  WHERE tag IS NOT NULL AND TRIM(tag) <> ''
),

-- De-duplicate per ticket to prevent double counting same tag across multiple tag columns
ticket_tag_distinct AS (
  SELECT DISTINCT
    ticket_id,
    queue,
    tag
  FROM tag_long
),

-- KPI 5A: Top recurring issues overall
top_tags_overall AS (
  SELECT
    tag,
    COUNT(*) AS ticket_tag_count
  FROM ticket_tag_distinct
  GROUP BY tag
),

-- KPI 5B: Top recurring issues by queue
top_tags_by_queue AS (
  SELECT
    queue,
    tag,
    COUNT(*) AS ticket_tag_count
  FROM ticket_tag_distinct
  GROUP BY queue, tag
)

-- Output 1: Overall top issues
SELECT
  tag,
  ticket_tag_count
FROM top_tags_overall
ORDER BY ticket_tag_count DESC
LIMIT 20;

-- Output 2: Top issues by queue (ops action)
SELECT
  queue,
  tag,
  ticket_tag_count
FROM top_tags_by_queue
ORDER BY queue, ticket_tag_count DESC;
