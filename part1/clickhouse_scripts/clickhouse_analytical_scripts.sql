CREATE TABLE analytical.user_activity_agg (
user_id UInt64,
num_visits AggregateFunction(count, UInt64),
last_visit AggregateFunction(max, DateTime(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY user_id;

CREATE MATERIALIZED VIEW analytical.user_activity_mv
TO analytical.user_activity_agg AS
SELECT
    user_id,
    countState() AS num_visits,
    maxState(event_time) AS last_visit
FROM live.user_events
GROUP BY user_id;


-- query on table
SELECT
    user_id,
    countMerge(num_visits) AS total_visits,
    maxMerge(last_visit) AS last_visit
FROM analytical.user_activity_agg
GROUP BY user_id
ORDER BY user_id;