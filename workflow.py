#this version assumes we can use the primaryinfo table that is unioned

import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator

'''
#change this to either unioned table or ALL tables from dataset2original
BQ_TABLES = [
    'dataset2.primaryinfo'
]
'''

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 5, 1)
}

sql1 = ('create table dataset2.workflow as' +
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2006" as year'+
    'from dataset2_original.pi2006' +
    'where vaers_id is not null' +
    'union distinct'+
    'elect distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2007" as year' +
    'from dataset2_original.pi2007' +
    'where vaers_id is not null' +
    'union distinct' +
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2008" as year'+
    'from dataset2_original.pi2008'+
    'where vaers_id is not null'+
    'union distinct'+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2009" as year'+
    'from dataset2_original.pi2009'+
    'where vaers_id is not null'+
    'union distinct '+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2010" as year'+
    'from dataset2_original.pi2010'+
    'where vaers_id is not null'+
    'union distinct '+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2011" as year'+
    'from dataset2_original.pi2011'+
    'where vaers_id is not null'+
    'union distinct'+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2012" as year'+
    'from dataset2_original.pi2012'+
    'where vaers_id is not null'+
    'union distinct '+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2013" as year'+
    'from dataset2_original.pi2013'+
    'where vaers_id is not null'+
    'union distinct'+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2014" as year'+
    'from dataset2_original.pi2014'+
    'where vaers_id is not null'+
    'union distinct'+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2015" as year'+
    'from dataset2_original.pi2015'+
    'where vaers_id is not null'+
    'union distinct'+
    'select distinct vaers_id, state,hospital as hospitalization, disable as disabled, age_yrs as age, sex, died, recovd as recovered, "2016" as year'+
    'from dataset2_original.pi2016'+
    'where vaers_id is not null')
sql2 = 'update dataset2.workflow set state = "U" where state is null'
sql3 = 'update dataset2.workflow set hospitalization = False where hospitalization is null'
sql4 = 'update dataset2.workflow set disabled = False where disabled is null'
sql5 = 'update dataset2.workflow set died = False where died is null'
sql6 = 'update dataset2.workflow set recovered = "U" where recovered is null'
sql7 = 'update dataset2.workflow set state = UPPER(state) where state != UPPER(state)'


with models.DAG(
        'workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    createPrimaryInfo = BashOperator(
        task_id='create_dataset_workflow',
        bash_command='bq mk workflow')

    fillPrimaryInfo = BashOperator(
        task_id='fill_workflow',
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
        task_id='change_nulls_to_false_state',
        bash_command='bq query --use_legacy_sql=false "' + sql2 + '"')  # default trigger rule is all_success

    changeNullsHospital = BashOperator(
        task_id='change_nulls_to_false_hospitalization',
        bash_command='bq query --use_legacy_sql=false "' + sql3 + '"')  # default trigger rule is all_success

    changeNullsDisabled = BashOperator(
        task_id='change_nulls_to_false_disabled',
        bash_command='bq query --use_legacy_sql=false "' + sql4 + '"')  # default trigger rule is all_success

    changeNullsDied = BashOperator(
        task_id='change_nulls_to_false',
        bash_command='bq query --use_legacy_sql=false "' + sql5 + '"')  # default trigger rule is all_success

    changeNullsRecovered = BashOperator(
        task_id='change_nulls_to_u',
        bash_command='bq query --use_legacy_sql=false "' + sql6 + '"')  # default trigger rule is all_success

    normalizeState = BashOperator(
        task_id='normalize_uncapitalized_values_state',
        bash_command='bq query --use_legacy_sql=false "' + sql7 + '"')  # default trigger rule is all_success

    createPrimaryInfo >> fillPrimaryInfo >> changeNullsState, changeNullsHospital, changeNullsDisabled,changeNullsDied, changeNullsRecovered, normalizeState
