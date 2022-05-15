from secrets import client_secrets, gsheet_secret, mailer_secret
from dependencies import Connector, get_dates, get_gsheet, sheet_setup, mail_alert
from config import sic_codes_resto, max_age_days, status_filters, gsheet, mail_receiver, officer_column
import json
from datetime import datetime, date
import pandas as pd
from json.decoder import JSONDecodeError
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

#create a dict with params for Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0, 0, 0, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 10)
}

def uber_dag():
    #Connect to the google sheet specified in the file
    spreadsheet = get_gsheet(url = gsheet['url'], scope = gsheet['scope'], gsheet_secret = gsheet_secret)

    #Check that the needed sheets to dump data are present in the provided spreadsheet
    sheet_check =  sheet_setup(
        spreadsheet,
        api_data_sheet_name = gsheet['api_data_sheet_name'],
        logs_sheet_name = gsheet['logs_sheet_name']
        )
    #Get already present company IDs from the document
    spread_data = pd.DataFrame(spreadsheet.worksheet(gsheet['api_data_sheet_name']).get_all_records())
    print(f'{len(spread_data)} company records located in the provided Google Sheet.')

    #Initiliase a connector object to send requests
    print('Initializing Connector')
    connector = Connector(client_secrets['rest_key'], client_secrets['stream_key'])

    #create an array of dates to throw requests per day to get all the companies
    date_list = get_dates(date.today(), max_age_days)
    print('Searching for companies, please wait.')
    #Get companies data from the API
    for day in date_list:
        companies_js = connector.advanced_company_search(
            sic_codes = ','.join(sic_codes_resto),
            size = 5000,
            company_status = ','.join(status_filters),
            incorporated_from = str(day),
            incorporated_to = str(day)
        )
        #Catch errors with no results
        if companies_js.status_code == 404: continue
        connector.store_companies(json.loads(companies_js.text)['items'])
    print(f'{len(connector.companies)} companies collected.')
    connector.log_dict['companies_collected'] = len(connector.companies)
    connector.log_dict['data_collection_done_ts'] = str(datetime.now())

    #Check if companies from the API are present in the Google Sheet and their detailed statuses are ok
    if len(spread_data) == 0:
        connector.validate_leads(gsheet_company_numbers = [], check_detailed_status = False)
    else:
        connector.validate_leads(gsheet_company_numbers = list(spread_data['company_number'].astype(str).values), check_detailed_status = False)
    print('Leads validation - done. Performing an interim save to the data google sheet.')
    connector.log_dict['validated_companies'] = len(connector.valid_companies)
    connector.log_dict['validation_done_ts'] = str(datetime.now())

    #Append interim data to the Google Sheet
    spreadsheet.worksheet(gsheet['api_data_sheet_name']).append_rows(connector.prepare_interim_update())
    print('Interim Update - Done')
    connector.log_dict['interim_update_ts'] = str(datetime.now())

    #Collect officer data for the companies
    follow_up = connector.prepare_follow_up_update()
    update_range = f"{officer_column}{len(spread_data) + 2}:{officer_column}"
    spreadsheet.worksheet(gsheet['api_data_sheet_name']).update(update_range, follow_up)
    print("Officer data appeneded to the Google Sheet!")

    #Log steps
    connector.log_dict['follow_up_done_ts'] = str(datetime.now())
    print('Done with all the steps! Adding info to the log sheet!')
    connector.log_dict['run_finished_ts'] = str(datetime.now())

    #Append log dict to the dataframe
    #spreadsheet.worksheet(gsheet['logs_sheet_name']).append_row(connector.prepare_record_logs())
    spreadsheet.worksheet(gsheet['logs_sheet_name']).append_row(list(connector.log_dict.values()))
    print('Logs have been stored to the Gsheet, all the steps have been completed!')

    #Inform about results of the execution
    mail_alert(
        recipient = mail_receiver,
        sender = 'whatever',
        content = f'''
        Run started at {connector.log_dict['run_start_ts']} results:
        {str(connector.log_dict)}
        ''',
        mailer_secret = mailer_secret
    )

dag = DAG(
    'uber_test_dag', #name
    default_args = default_args, #params - not sure if I understand
    description = 'DAG for Uber UK Companies House',
    schedule_interval = timedelta(minutes = 30)
)

run_etl = PythonOperator(
    task_id = 'uber_test_dag', #assign a name (id)
    python_callable = uber_dag,
    dag = dag,
)
#Define order of the tasks
run_etl