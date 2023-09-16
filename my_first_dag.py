# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

from airflow.operators.dummy import DummyOperator 
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

with DAG(
    "my_first_dag",
	default_args={
		"depends_on_past": False,
        	"email": ["rinlaphat.mark@g.swu.ac.th"],
        	"email_on_failure": False,
        	"email_on_retry": False,
        	"retries": 1,
        	"retry_delay": timedelta(minutes=5),

    },
    description="A simple tutorial DAG",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

	t1 = BashOperator(
		task_id="print_date",
		bash_command="date",
	)
	t2 = BashOperator(
		task_id="print_date2",
		bash_command="date",
	)
	t1 >> t2

	def dummy_test():
    		return 'branch_a'

	A_task = DummyOperator(task_id='branch_a', dag=dag)
	B_task = DummyOperator(task_id='branch_false', dag=dag)

	branch_task = BranchPythonOperator(
		task_id='branching',
		python_callable=dummy_test,
		dag=dag,
		)

	branch_task >> A_task 
	branch_task >> B_task

