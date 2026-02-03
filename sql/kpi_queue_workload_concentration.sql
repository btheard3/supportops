/*
KPI 3: Queue Workload Concentration
-----------------------------------
Definition:
Distribution of tickets across queues + concentration metrics (Top N share, cumulative share).

Grain:
1 row = 1 support ticket

Purpose:
- Identify overloaded teams / bottlenecks
- Spot routing inefficiencies (too much work landing in a few queues)
- Support staffing allocation

Notes:
- Queue is used as best available “ownership” proxy (no agent ids)
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

queue_counts AS (
  SELECT
    queue,
    COUNT(*) AS ticket_count
  FROM fact_tickets
  GROUP BY queue
),

totals AS (
  SELECT SUM(ticket_count) AS total_tickets
  FROM queue_counts
),

queue_shares AS (
  SELECT
    qc.queue,
    qc.ticket_count,
    (qc.ticket_count * 1.0 / t.total_tickets) AS share
  FROM queue_counts qc
  CROSS JOIN totals t
),

queue_ranked AS (
  SELECT
    queue,
    ticket_count,
    share,
    SUM(share) OVER (ORDER BY share DESC) AS cum_share
  FROM queue_shares
),

top_n_share AS (
  SELECT
    SUM(share) AS top3_share
  FROM (
    SELECT share
    FROM queue_ranked
    ORDER BY share DESC
    LIMIT 3
  )
)

-- Output 1: full ranked distribution + cumulative share
SELECT
  queue,
  ticket_count,
  share,
  cum_share
FROM queue_ranked
ORDER BY share DESC;

-- Output 2: one-liner concentration headline (Top 3 queues share)
SELECT
  top3_share
FROM top_n_share;
