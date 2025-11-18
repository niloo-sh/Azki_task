CREATE DATABASE live;

CREATE TABLE live.users_event_queue
(
    event_time              DateTime64(3),
    user_id                 Int32,
    session_id              String,
    channel                 String,
    premium_amount          Int64
)
ENGINE = Kafka('kafka-broker-1:19092', 'azki_user_events',
         'user_evens_consumer_group_1', 'JSONEachRow')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081';



CREATE MATERIALIZED VIEW live.user_events_mv
TO live.user_events
AS
SELECT
	ue.event_time as event_time,
	ue.user_id as user_id,
	ue.session_id as session_id,
	ue.channel as channel,
	ue.premium_amount as premium_amount,
	u.city as city,
	u.signup_date as signup_date
from
live.users_event_queue ue
left join (select * from dimension.users final) u
on ue.user_id = u.user_id



create table live.user_events
(
    event_time              DateTime64(3),
    user_id                 Int32,
    session_id              String,
    channel                 String,
    premium_amount          Int64,
	city					String,
	signup_date				Date
)
Engine = MergeTree()
ORDER BY (toDate(event_time), channel,user_id)
