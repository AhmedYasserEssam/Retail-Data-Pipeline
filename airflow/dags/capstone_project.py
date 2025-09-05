from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# PostgreSQL connection ID configured in Airflow
PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def fetch_order_data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    query = """
        SELECT
            o.orderdate::date AS sales_date,
            od.quantity AS quantity,
            p.price AS price
        FROM orders o
        JOIN order_details od ON o.orderid = od.orderid
        JOIN products p ON od.productid = p.productid
    """
    df = pd.read_sql(query, conn)
    df.to_csv('/home/kiwilytics/airflow_output/daily_sales_data.csv', index=False)

def process_daily_revenue():
    df = pd.read_csv("/home/kiwilytics/airflow_output/daily_sales_data.csv")
    df['total_revenue'] = df['quantity'] * df['price']
    revenue_per_day = df.groupby('sales_date')['total_revenue'].sum().reset_index()
    revenue_per_day.to_csv('/home/kiwilytics/airflow_output/daily_revenue_data.csv', index=False)

def time_series_plot():
    df = pd.read_csv("/home/kiwilytics/airflow_output/daily_revenue_data.csv")
    df['sales_date'] = pd.to_datetime(df['sales_date'])
    
    plt.figure(figsize=(10,6))
    plt.plot(df['sales_date'], df['total_revenue'], marker='o')
    plt.title("Daily Revenue Over Time")
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('/home/kiwilytics/airflow_output/daily_revenue_plot.png')

with DAG(
    dag_id='daily_sales_revenue_analysis',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    description='Daily revenues visualization in Airflow',
) as dag:
    
    task_fetch_data = PythonOperator(
        task_id='fetch_orders_data',
        python_callable=fetch_order_data,
    )

    task_daily_revenues = PythonOperator(
        task_id='calculate_total_revenues',
        python_callable=process_daily_revenue,
    )

    task_time_series_plot = PythonOperator(
        task_id='plot_revenues',
        python_callable=time_series_plot,
    )

    task_fetch_data >> task_daily_revenues >> task_time_series_plot
