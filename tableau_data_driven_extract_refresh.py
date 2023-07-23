#!/usr/bin/env python
# coding: utf-8

import os, sys
import pandas as pd
from pyodbc import connect as py_con
from databricks import sql
import tableauserverclient as TSC
from datetime import datetime
import time

starttime = datetime.now()
process_status = 1


def get_db_conn():
    db_conn_str = "<your DSN>"
    return py_con(db_conn_str, autocommit=True)

## execute sql query on database
def execute_sql_query(sql_qry):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_qry)

## reads one entry at a time to process
def read_ctrl_tbl_data(qry):
    ctrl_data = None
    with get_db_conn() as conn:
        ctrl_data = pd.read_sql(qry, conn)
    
    if ctrl_data is not None and ctrl_data.size > 0:
        ctrl_data = ctrl_data.to_dict(orient='records')[0]
        return ctrl_data
    else:
        print("No ctrl table entry to process...")
        return None

## update status in supporting table
def update_ctrl_tbl_status(ctrl_data, status):
    execute_sql_query(ctrl_tbl_update_qry.format(id=ctrl_data['id'], status=status))
        
## refreshes the extract based on LUID
def refresh_datasource(datasource_id):
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name='<your_tableau_token_name>', personal_access_token='token_value', site_id='your_tableau_site_name')
    server = TSC.Server('https://<your_tableau_server_base_url>/', use_server_version=True)
    
    with server.auth.sign_in(tableau_auth):
        # get datasource by id value
        datasource = server.datasources.get_by_id(datasource_id)

        # trigger datasource refresh. this will return job_id
        job = server.datasources.refresh(datasource)
        print("datasource refresh job: %s" % job.id)

        # wait for refresh job to complete
        print("waiting for job to complete...")
        job = server.jobs.wait_for_job(job)
        print("job finished successfully...")

## send alerts or notifications to email/slack/msteams
def send_alert(message, status):
    ## TO-DO
    

ctrl_tbl_select_qry = """select top 1 a.*, c.tabelau_object_luid as extract_id
  from etl_status_table a
  inner join (select table_name, max(id) last_row_id from etl_status_table group by 1) b
    on a.id = b.last_row_id and a.tableau_job_creation_timestamp is null
  inner join etl_tableau_relationship_table c
    on a.table_name = c.etl_table_name
  order by a.etl_finish_timestamp"""
# print(ctrl_tbl_select_qry)

ctrl_tbl_update_qry = """update etl_status_table
set tableau_job_creation_timestamp = getdate(), job_status = '{status}'
where id = {id}"""


try:
    # read etl table entry - one at a time
    ctrl_data = read_ctrl_tbl_data(ctrl_tbl_select_qry)
    
    # read next etl entry until all entries are not processed
    while ctrl_data is not None:
        try:
            print("Ctrl tbl entry to process:\n%s" % ctrl_data)
            # update status to "Processing"
            update_ctrl_tbl_status(ctrl_data, status='P')
            # trigger datasource refresh in tableau server
            refresh_datasource(datasource_id=ctrl_data['extract_id'])
            # update status to "success"
            update_ctrl_tbl_status(ctrl_data, status='Y')
            print("="*30)
            # send alerts or notifications
            #send_alert(message="<your message>" % ctrl_data, status=1)
            # read next entry to process
            ctrl_data = read_ctrl_tbl_data(ctrl_tbl_select_qry)
        except Exception as ex:
            print("Error details:")
            process_status = 0
            #send_alert(message="<your message>" % ctrl_data, status=0)
        
except Exception as ex:
    print("Error details:")
    process_status = 0
    #send_alert(message="<your message>" % ctrl_data, status=0)
    
finally:
    endtime = datetime.now()
    total_time = (endtime - starttime).seconds
    print("Total time taken: %.2f seconds" % (total_time))
    print('='*10 + ' End of the process ' + '='*10)

