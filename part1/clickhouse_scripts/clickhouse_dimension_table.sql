CREATE DATABASE dimension;

CREATE TABLE dimension.users_queue
(
    user_id                 Int64,
    signup_date             Int32,
    city                    String,
    device_type             String,
    __op                    Nullable(String),
    __EVENT_TS              Nullable(Int64),
    __KAFKA_EVENT_TS        Nullable(Int64)
)
ENGINE = Kafka('kafka-broker-1:19092', 'azki_mysql.mydb.users',
         'users_consumer_group_1', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081';


CREATE TABLE dimension.users (
    `user_id`     Int64,
    `signup_date` DATE,
    `city`        String,
    `device_type` String
) ENGINE = ReplacingMergeTree()
ORDER BY (user_id)
SETTINGS index_granularity = 8192;



CREATE MATERIALIZED VIEW dimension.users_mv
TO dimension.users
AS
SELECT
    user_id                                AS user_id,
    addDays(toDate('1970-01-01'), signup_date)     AS signup_date,
    city                                   AS city,
    device_type                            AS device_type
FROM dimension.users_queue;
