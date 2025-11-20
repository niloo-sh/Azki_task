## Query Performance and Data Governance

### Problem:
Create a denormalized table in ClickHouse where order details are added to the user events
data when event type = purchase, using materialized views.
Assume order details come from 5 different production tables: four types of products (third,
body, medical, fire), and financial data comes from the financial order table. Multiple joins
and unions may be required.
Propose optimizations to improve query performance, and suggest data governance or
access-control mechanisms.

-------------------------------------------------
This task has several parts:

#### Create denormalized table

I consider user_events topic from last part as the source table and join other tables with them.

please look at *denormalized_table_scripts*

I use several materialized view to create denormalized table without Union.
with several materialized view to one destination table.


#### Performance optimization

In this part I use, clickhouse projection, skip Index and column materialized to optimize performance.
check *performance_optimization_scripts.sql*

#### access-control mechanisms

This part is about creating user, role, grant permission and create QUOTA.

In addition, using view to mask credential data.

check *data_governance_and_access_control_scripts.sql*





