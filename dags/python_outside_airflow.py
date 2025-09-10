from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperatofr
from datetime import datetime, timedelta

path = '${AIRFLOW_HOME}/scripts/projects/project_sales_etl'

with DAG(
    dag_id='sales_report_etl_2',
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=10),
    catchup=False,
    tags=['sales', 'reporting', 'etl'],
    # params={'your_input_value', 20}
) as dag:
    # Task 1: Extract website sales data
    extract_website_task = BashOperator(
        task_id='extract_website_sales',
        params = {'a' : 'your_a:30', 'b': 'your_b:100'},
        bash_command='python3 %s/extract_website_sales_data.py {{params.a}} {{params.b}}' % path,
        # run_as_user='my_other_user',
    )
    
    test = BashOperator(
        task_id='test',
        params = {'your_input_value' : '30'},
        bash_command='echo {{params.your_input_value}}',
        # run_as_user='my_other_user',
    )

    # Task 2: Extract mobile app sales data
    extract_mobile_app_task = BashOperator(
        task_id='extract_mobile_app_sales',
        bash_command=f'python {path}/extract_mobile_app_sales_data.py',
    )

    # Task 3: Transform and combine sales data (depends on both extractions)
    transform_data_task = BashOperator(
        task_id='transform_and_combine_sales_data',
        bash_command=f'python3 {path}/transform_sales_data.py',
    )

    # Task 4: Generate the final report
    generate_report_task = BashOperator(
        task_id='generate_sales_report',
        bash_command=f'python3 {path}/generate_report.py',
    )

    # Task 5: Send the report via email
    send_email_task = BashOperator(
        task_id='send_report_email',
        bash_command=f'python3 {path}/send_report_by_email.py',
    )

    # Define task dependencies
    [extract_website_task, extract_mobile_app_task, test] >> transform_data_task
    transform_data_task >> generate_report_task
    generate_report_task >> send_email_task