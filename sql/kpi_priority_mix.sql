/*
KPI 2: Priority Mix
-------------------
Definition:
Distribution of support tickets across priority levels.

Grain:
1 row = 1 support ticket

Purpose:
- Distinguish urgent work from routine requests
- Prevent low-priority noise from hiding critical incidents
- Support staffing decisions and escalation policies

Notes:
- No SLA timing available (no timestamps), so "urgency" is defined by provided priority labels
- Assumes Phase 2 standardization was applied (trim + lowercase)
*/

WITH fact_tickets AS (
  SELECT
    LOWER(TRIM(queue))     AS queue,
    LOWER(TRIM(priority))  AS priority,
    LOWER(TRIM(type))      AS type,
    LOWER(TRIM(language))  AS language
  FROM dataset_tickets
),

priority_counts AS (
  SELECT
    priority,
    COUNT(*) AS ticket_count
  FROM fact_tickets
  GROUP BY priority
),

total AS (
  SELECT SUM(ticket_count) AS total_tickets
  FROM priority_counts
),

priority_mix AS (
  SELECT
    pc.priority,
    pc.ticket_count,
    (pc.ticket_count * 1.0 / t.total_tickets) AS share
  FROM priority_counts pc
  CROSS JOIN total t
)

SELECT
  priority,
  ticket_count,
  share
FROM priority_mix
ORDER BY ticket_count DESC;
