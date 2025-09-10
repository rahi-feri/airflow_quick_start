from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta




def extract_website_sales_data(your_input_value: int, **context):
    import os
    # Simulate extraction from a website database or API
    print("Extracting website sales data...")
    # In a real scenario, this would involve database queries or API calls
    return_value = f'today is: {datetime.today()} and the input value is {your_input_value}'
    with open(f'scripts/file_inside.txt', 'w') as f:
        f.writelines(return_value)
    context['ti'].xcom_push(key='my_custom_key', value=return_value) #xcom
    pass

def extract_mobile_app_sales_data():
    # Simulate extraction from a mobile app backend
    print("Extracting mobile app sales data...")
    # In a real scenario, this would involve database queries or API calls
    pass

def transform_sales_data():
    # Simulate data cleaning, aggregation, and joining
    print("Transforming and combining sales data...")
    # This would involve Pandas, Spark, or other data manipulation libraries
    pass

def generate_report():
    # Simulate report generation (e.g., creating a CSV, PDF, or dashboard data)
    print("Generating daily sales report...")
    # This might use reporting tools or libraries
    pass

def send_report_by_email():
    # Simulate sending the report via email
    print("Sending report via email...")
    # This would involve an email library or service integration
    pass

with DAG(
    dag_id='sales_report_etl',
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['sales', 'reporting', 'etl'],
) as dag:
    # Task 1: Extract website sales data
    extract_website_task = PythonOperator(
        task_id='extract_website_sales',
        python_callable=extract_website_sales_data,
        op_kwargs={'your_input_value': 10}
    )

    # Task 2: Extract mobile app sales data
    extract_mobile_app_task = PythonOperator(
        task_id='extract_mobile_app_sales',
        python_callable=extract_mobile_app_sales_data,
    )

    # Task 3: Transform and combine sales data (depends on both extractions)
    transform_data_task = PythonOperator(
        task_id='transform_and_combine_sales_data',
        python_callable=transform_sales_data,
    )

    # Task 4: Generate the final report
    generate_report_task = PythonOperator(
        task_id='generate_sales_report',
        python_callable=generate_report,
    )

    # Task 5: Send the report via email
    send_email_task = PythonOperator(
        task_id='send_report_email',
        python_callable=send_report_by_email,
    )

    # Define task dependencies
    [extract_website_task, extract_mobile_app_task] >> transform_data_task
    transform_data_task >> generate_report_task
    generate_report_task >> send_email_task