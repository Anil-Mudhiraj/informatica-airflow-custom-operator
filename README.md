# informatica-airflow-custom-operator
Airflow Custom Operators to trigger Informatica Cloud assests like Mapping Task, Taskflow, File Ingestion Task, Synchronization Task, Replication Task and Linear Taskflows.

## How to use?
1. Copy the plugin code to your airflow evironment i.e into your dags folder.
2. Create a HTTP Connection in airflow with HOST as Informatica Cloud Login URL, username and password.
3. Create a DAG with a MappingTaskOperator.
