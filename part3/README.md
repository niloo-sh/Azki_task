## Data Quality and Monitoring
### problem:
Design a data quality validation plan for the pipeline, covering sync and delay issues,
missing events, schema drift, and load monitoring.

Bonus:
Write backfill code with Spark to handle missing data and backfill scenarios.
-----------------------------------------------------------------------------------------------

### Solution:

            +--------------------+
            |   MySQL Database   |
            | (Source System)    |
            +---------+----------+
                      |
                      |  CDC / Mysql source Connector
                      v
        +---------------------------------------+
        |                 Kafka                 |
        |---------------------------------------|
        |  +-------------+   +----------------+ |
        |  | Kafka       |   | Schema         | |
        |  | Brokers     |   | Registry       | |
        |  +-------------+   +----------------+ |
        |               +--------------------+  |
        |               | Kafka Connect      |  |
        |               | (Source + Sink)    |  |
        |               +--------------------+  |
        +------------------------+--------------+
                                 |
                                 |  kafka Engin
                                 v
                     +---------------------------+
                     |       ClickHouse          |
                     |     (Destination DB)      |
                     +---------------------------+

### kafka connect issues
In using kafka connect some issues may happen, based on change in schema, data missing and other case 
these issues are listed in this link. [medium link](https://medium.com/@a.tambakouzadeh/checklist-to-troubleshoot-kafka-connect-issues-using-debezium-platform-for-cdc-and-mysql-data-b4d517d152a4)

### Monitoring

monitoring on the above system can be down on different level:
    
- message per topic monitoring
- connector monitoring (up/down). This can help us in connector monitorig.
[External link to another git repo](https://github.com/snapp-incubator/connector-guardian)
- clickhouse running query monitoring. how long each query is running, each query read how many line
- ....

all of these monitoring can be run with prometeous and grafana, based on jmx exporter exposed data.

--------------------------------
Bonus:

Write backfill code with Spark to handle missing data and backfill scenarios.

in response to this part you have multiple choices that read data with spark from mysql is not good enought.

direct query on the source database when you activate mysql binlog is not acceptable.

- use connector with statement to get just specific data based on query. 

with this solution you can use other parts of your pipeline without any changes.

