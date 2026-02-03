/*
KPI 4: Ticket Type Mix
------------------------------------
Definition:
Distribution of tickets by ticket type (incident, request, problem, change).

Grain:
1 row = 1 support ticket

Purpose:
- Understand operational posture: reactive (incidents) vs planned (requests/changes)
- Identify queues with disproportionate incident load
- Inform staffing + process improvement (incident reduction, deflection, routing)

Notes:
- Assumes Phase 2 standardization was applied (trim + lowercase)
- Type is treated as mutually exclusive per ticket (as provided)
*/

WITH fact_tickets AS (
  SELECT
    LOWER(TRIM(queue))     AS queue,
    LOWER(TRIM(type))      AS type,
    LOWER(TRIM(priority))  AS priority,
    LOWER(TRIM(language))  AS language
  FROM dataset_tickets
  WHERE type IS NOT NULL AND TRIM(type) <> ''
),

-- Total tickets for percent calcs
totals AS (
  SELECT COUNT(*) AS total_tickets
  FROM fact_tickets
),

-- KPI 4A: Overall type mix (counts + share)
overall_type_mix AS (
  SELECT
    type,
    COUNT(*) AS ticket_count
  FROM fact_tickets
  GROUP BY type
),

overall_type_mix_with_share AS (
  SELECT
    otm.type,
    otm.ticket_count,
    (otm.ticket_count * 1.0 / t.total_tickets) AS share
  FROM overall_type_mix otm
  CROSS JOIN totals t
),

-- KPI 4B: Type mix by queue (counts + share within each queue)
queue_type_counts AS (
  SELECT
    queue,
    type,
    COUNT(*) AS ticket_count
  FROM fact_tickets
  GROUP BY queue, type
),

queue_totals AS (
  SELECT
    queue,
    SUM(ticket_count) AS queue_total
  FROM queue_type_counts
  GROUP BY queue
),

queue_type_mix_with_share AS (
  SELECT
    qtc.queue,
    qtc.type,
    qtc.ticket_count,
    (qtc.ticket_count * 1.0 / qt.queue_total) AS share_in_queue
  FROM queue_type_counts qtc
  JOIN queue_totals qt
    ON qtc.queue = qt.queue
)

-- Output 1: Overall type mix
SELECT
  type,
  ticket_count,
  share
FROM overall_type_mix_with_share
ORDER BY ticket_count DESC;

-- Output 2: Type mix by queue (most useful for ops)
SELECT
  queue,
  type,
  ticket_count,
  share_in_queue
FROM queue_type_mix_with_share
ORDER BY queue, ticket_count DESC;
