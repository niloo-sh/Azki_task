from airflow.decorators import dag, task
import pandas as pd
from datetime import datetime
import logging
from clickhouse_driver import Client
from airflow.models import Variable

logger = logging.getLogger(__name__)


def load_into_clickhouse(dataframe: pd.DataFrame):
    client = Client(host='localhost',
                    port='9000',
                    database='dimension',
                    user='default',
                    password='',
                    settings={'use_numpy': True})
    client.insert_dataframe(f"INSERT INTO dimension.users VALUES", dataframe)
@dag(
    schedule_interval="@once",
    start_date=datetime(2024, 7, 28, 20, 00),
    catchup=True,
    tags=["fix_data", "Mysql", "clickhouse"],
    max_active_tasks=3,
    concurrency=1,
    max_active_runs=1
    )
def fill_missed_data_dag():
    from airflow.hooks.mysql_hook import MySqlHook
    from airflow.hooks.base import BaseHook

    @task()
    def get_missed_data():
        mysql_hook_prod = MySqlHook(mysql_conn_id="mysql_conn", schema="mydb")
        min_id = Variable.get("order_id", deserialize_json=True)["min_id"]
        max_id = Variable.get("order_id", deserialize_json=True)["max_id"]
        source_table_name = Variable.get("order_id", deserialize_json=True)["source_table_name"]

        print(f"max_id is: {max_id}, min_id is:{min_id}, source_table_name is: {source_table_name}")

        sql_query = f"SELECT * from {source_table_name} " \
                    f"WHERE o.id >= {min_id} and o.id<= {max_id}"

        print(f"second query is:{sql_query}")
        cursor_prod = mysql_hook_prod.get_conn().cursor()
        cursor_prod.execute(sql_query)
        order_order_product_df = pd.DataFrame(cursor_prod.fetchall())

        print("loading data into clickhouse.....")
        load_into_clickhouse(order_order_product_df)

    @task()
    def optimize_table():
        destination_table_name = Variable.get("order_id", deserialize_json=True)["source_table_name"]
        destination_table_order_by = Variable.get("order_id", deserialize_json=True)["destination_table_order_by"]

        clickhouse_info_v1 = BaseHook.get_connection('click')
        clickhouse_client_v1 = Client.from_url(
            f"clickhouse://{clickhouse_info_v1.login}:{clickhouse_info_v1.password}@{clickhouse_info_v1.host}:"
            f"{clickhouse_info_v1.port}/{clickhouse_info_v1.schema}"
        )

        logger.info(f"clickhouse url is: clickhouse://{clickhouse_info_v1.login}:{clickhouse_info_v1.password}@"
                    f"{clickhouse_info_v1.host}:{clickhouse_info_v1.port}/{clickhouse_info_v1.schema}")

        insert_query_core_table = f"OPTIMIZE TABLE {destination_table_name} DEDUPLICATE BY " \
                                  f"{destination_table_order_by};"
        clickhouse_client_v1.execute(insert_query_core_table)

    get_missed_data() >> optimize_table()


fill_missed_data_dag = fill_missed_data_dag()

