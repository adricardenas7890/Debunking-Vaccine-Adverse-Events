#this version assumes we can use the primaryinfo table that is unioned

import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

#change this to either unioned table or ALL tables from dataset2original
BQ_TABLES = [
    'dataset2.primaryinfo'
]

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 5, 1)
}

sql1 = 'create table dataset2.workflow as select * from dataset2.primaryinfo' #insert SQL for unioning or importing unioned table
sql2 = ''
sql3 = ''
sql4 = ''
sql5 = ''
sql6 = ''
sql7 = ''


with models.DAG(
        'workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    createPrimaryInfo = BashOperator(
        task_id='create dataset workflow',
        bash_command='bq mk workflow',)

    #figure out if we need this or we can import our unioned table
    fillPrimaryInfo = BashOperator(
        task_id='fill workflow',
        bash_command='bq query --use_legacy_sql=false "' + sql1 + '"')  # default trigger rule is all_success

    '''
    #use if you need to union tables
    for table in BQ_TABLES:
        sql = 'create table dataflow.' + table + ' as select * from college.' + table
        bash_tasks = BashOperator(
            task_id='copy_{}_table'.format(table),
            bash_command='bq query --use_legacy_sql=false "' + sql + '"'
        )
    '''
    changeNullsState = BashOperator(
        task_id='change nulls to false state',
        bash_command='bq query --use_legacy_sql=false "' + sql2 + '"')  # default trigger rule is all_success

    changeNullsHospital = BashOperator(
        task_id='change nulls to false hospitalization',
        bash_command='bq query --use_legacy_sql=false "' + sql3 + '"')  # default trigger rule is all_success

    changeNullsDisabled = BashOperator(
        task_id='change nulls to false disabled',
        bash_command='bq query --use_legacy_sql=false "' + sql4 + '"')  # default trigger rule is all_success

    changeNullsDied = BashOperator(
        task_id='change nulls to false',
        bash_command='bq query --use_legacy_sql=false "' + sql5 + '"')  # default trigger rule is all_success

    changeNullsRecovered = BashOperator(
        task_id='change nulls to U',
        bash_command='bq query --use_legacy_sql=false "' + sql6 + '"')  # default trigger rule is all_success

    normalizeState = BashOperator(
        task_id='normalize uncapitalized values state',
        bash_command='bq query --use_legacy_sql=false "' + sql7 + '"')  # default trigger rule is all_success

    createPrimaryInfo >> fillPrimaryInfo >> changeNullsState >> changeNullsHospital >> changeNullsDisabled >> changeNullsDied >> changeNullsRecovered >> changeNullsRecovered >> normalizeState