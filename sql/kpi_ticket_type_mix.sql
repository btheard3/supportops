/*
KPI 4: Ticket Type Mix

Definition:
Distribution of tickets by ticket type (incident, request, problem, change),
broken down by queue for operational context.

Grain:
1 row = (queue, type)

Purpose:
- Understand reactive vs planned work
- Identify queues dominated by incidents
- Inform staffing, deflection, and routing decisions

Notes:
- Assumes Phase 2 standardization (trim + lowercase)
- Ticket type is treated as mutually exclusive per ticket
*/

WITH fact_tickets AS (
    -- Clean, fact-like view
    SELECT
        LOWER(TRIM(queue))    AS queue,
        LOWER(TRIM(type))     AS type
    FROM dataset_tickets
    WHERE type IS NOT NULL
      AND TRIM(type) <> ''
),

queue_type_counts AS (
    -- Count tickets by queue + type
    SELECT
        queue,
        type,
        COUNT(*) AS ticket_count
    FROM fact_tickets
    GROUP BY queue, type
),

queue_totals AS (
    -- Total tickets per queue (for % calculations)
    SELECT
        queue,
        SUM(ticket_count) AS queue_total
    FROM queue_type_counts
    GROUP BY queue
),

queue_type_mix AS (
    -- Add share within each queue
    SELECT
        qtc.queue,
        qtc.type,
        qtc.ticket_count,
        qtc.ticket_count * 1.0 / qt.queue_total AS share_in_queue
    FROM queue_type_counts qtc
    JOIN queue_totals qt
      ON qtc.queue = qt.queue
)

-- FINAL OUTPUT (ONE SELECT)
SELECT
    queue,
    type,
    ticket_count,
    share_in_queue
FROM queue_type_mix
ORDER BY queue, ticket_count DESC;
