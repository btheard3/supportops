/*
KPI: Ticket Volume

Definition:
Count of support tickets grouped by operational dimensions
(queue, priority, type, language).

Grain:
1 row = 1 support ticket

Purpose:
- Understand where work is landing
- Support workload analysis and capacity planning
- Serve as the foundation metric for all other KPIs

Notes:
- No time-based analysis (timestamps not present)
- Assumes categorical fields are already lightly standardized
*/

WITH fact_tickets AS (
    SELECT
        LOWER(TRIM(queue))     AS queue,
        LOWER(TRIM(priority))  AS priority,
        LOWER(TRIM(type))      AS type,
        LOWER(TRIM(language))  AS language
    FROM dataset_tickets
),

-- Ticket volume by queue
ticket_volume_by_queue AS (
    SELECT
        queue,
        COUNT(*) AS ticket_count
    FROM fact_tickets
    GROUP BY queue
),

-- Ticket volume by priority
ticket_volume_by_priority AS (
    SELECT
        priority,
        COUNT(*) AS ticket_count
    FROM fact_tickets
    GROUP BY priority
),

-- Ticket volume by ticket type
ticket_volume_by_type AS (
    SELECT
        type,
        COUNT(*) AS ticket_count
    FROM fact_tickets
    GROUP BY type
),

-- Ticket volume by language
ticket_volume_by_language AS (
    SELECT
        language,
        COUNT(*) AS ticket_count
    FROM fact_tickets
    GROUP BY language
)

-- Final SELECTs
SELECT * FROM ticket_volume_by_queue ORDER BY ticket_count DESC;
-- SELECT * FROM ticket_volume_by_priority ORDER BY ticket_count DESC;
-- SELECT * FROM ticket_volume_by_type ORDER BY ticket_count DESC;
-- SELECT * FROM ticket_volume_by_language ORDER BY ticket_count DESC;
