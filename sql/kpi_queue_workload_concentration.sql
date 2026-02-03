/*
KPI 3: Queue Workload Concentration

Definition:
Distribution of tickets across queues + concentration signals (share + cumulative share).

Grain:
1 row = 1 support ticket

Purpose:
- Identify overloaded teams / bottlenecks
- Reveal routing inefficiencies (too much work landing in a few queues)
- Support staffing allocation

Notes:
- Queue is the best available "ownership proxy" (no agent ids)
- Assumes Phase 2 standardization was applied (trim + lowercase)
- DuckDB: One KPI file should return ONE final result set (one final SELECT)
*/

WITH fact_tickets AS (
    -- Build a clean "fact-like" view directly from the raw CSV table loaded by DuckDB
    -- (We standardize here so SQL results match notebook logic.)
    SELECT
        LOWER(TRIM(queue))     AS queue,
        LOWER(TRIM(priority))  AS priority,
        LOWER(TRIM(type))      AS type,
        LOWER(TRIM(language))  AS language
    FROM dataset_tickets
),

queue_counts AS (
    -- Count tickets by queue
    -- This is the base distribution we need
    SELECT
        queue,
        COUNT(*) AS ticket_count
    FROM fact_tickets
    GROUP BY queue
),

totals AS (
    -- Total tickets across all queues
    SELECT
        SUM(ticket_count) AS total_tickets
    FROM queue_counts
),

queue_shares AS (
    -- Convert counts into shares
    -- share = ticket_count / total_tickets
    SELECT
        qc.queue,
        qc.ticket_count,
        qc.ticket_count * 1.0 / t.total_tickets AS share
    FROM queue_counts qc
    CROSS JOIN totals t
),

queue_ranked AS (
    -- Rank queues by share and compute cumulative share (Pareto-style)
    -- cum_share helps show concentration (e.g., top queues account for X% of tickets)
    SELECT
        queue,
        ticket_count,
        share,
        SUM(share) OVER (ORDER BY share DESC) AS cum_share
    FROM queue_shares
)

-- FINAL OUTPUT (ONE SELECT):
-- Ranked queue distribution + cumulative share (Power BI can compute "Top 3 share" easily)
SELECT
    queue,
    ticket_count,
    share,
    cum_share
FROM queue_ranked
ORDER BY share DESC;
