## Data Modeling and Ingestion
### problme:
Design an ETL pipeline that reads events (user_events.csv) from Kafka, joins them with the
MySQL users table (users.csv), and loads aggregated results into ClickHouse (any
aggregation like avg, sum, or count is acceptable).
Bonus:
set up a Kafka cluster using Compose with all necessary tools, along with the required
Connect config files.
-------------------------------------------------
This task has different part(please run them one by one):

#### 1. Setup Infra
Based on *docker-compose.yaml* file all kafka component, clickhouse and mysql will be running after

    docker compose up -d

now you can access all components of infrastructure.

#### 2. Load data into source database
now run bellow command to load *users.csv* into **users** table in mysql and grant to user for reading binlog
    
    python3 initializer.py
*At first you should install *requirements.py**

notice: you should put users.scv in part1 directory

#### 3. SetUp connector to load data into kafka brokers with schema registry

To load data from mysql to kafka topic,I set up a source connector that load data from binlog to kafka topic.

    python3 connector_uploader.py

#### 4. create materialized view and aggregation trees
please look at *clickhouse_scripts* directory
- run *clickhouse_dimension_table* scripts
- run *clickhouse_live_tables* scripts
- run *clickhouse_analytical_scripts*

#### 5. Load user event into
now it's time to *user_events.scv* into kafka topics.
    
    python3 kafka-writer.py
 

