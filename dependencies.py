#do some OOP here: like create a connector class and store all the methods to interact with the API there
from time import sleep
from requests.auth import HTTPBasicAuth
import requests
from requests.adapters import HTTPAdapter
import config
from requests.exceptions import Timeout
import sys
from datetime import datetime, timedelta
import json
from ratelimit import limits, RateLimitException, sleep_and_retry
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import smtplib
from email.message import EmailMessage
from secrets import mailer_secret

#Functions
def mail_alert(recipient, sender, content, mailer_secret, subject = 'UK Leads Alert'):
    '''Function to inform the owner of the code about the execution issues.\n
    Arguments:\n
        recipient: email to where the message should be sent,
        sender (optional)
        conten: text of the message
        mailer_secret: json from secrets.pt file with login data for the email account that will send the message.
        subject: subject of the email message, hard-coded, but can be moved to config if needed!
        '''
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['to'] = recipient
    msg.set_content(content)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(mailer_secret['email'], mailer_secret['pass'])
        smtp.send_message(msg)

def get_dates(date, days_count):
    '''Function to create a list of dates back from the given date.
    \nArguments:\n
        date: date from which the count should happend
    '''
    return [date - timedelta(days = x) for x in range(days_count)]

def get_gsheet(url, gsheet_secret, scope):
    '''
    Function to connect to Google Sheets API.\n
    Inputs:\n
    url of the google sheet, google sheet secrets json dict, scopes for the API
    Will stop execution of the whole code in case there is no connection of the document with the given url cannot be accessed.
    NB: the document must be shared with the service account provided in the secrets.
    '''
    client = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(gsheet_secret, scope))
    if client: print('Connection to Google Sheet has been established')
    else:
        print('Failed to Connect to Google Sheet API, check configs and restart the execution')
        mail_alert(
            recipient = config.mail_receiver,
            sender = 'whatever',
            content = f'''Script Failed to Connect to Google Sheet API.
            \nCheck configs ASAP!
            ''',
            mailer_secret = mailer_secret
        )
        sys.exit()
    try:
        return client.open_by_url(url)    
    except:
        print('Failed to Open the specified document, check configs and restart the execution')
        mail_alert(
            recipient = config.mail_receiver,
            sender = 'whatever',
            content = f'''The script failed with opening the Gsheet.
            \nMake sure the document is shared with service account and exists!
            ''',
            mailer_secret = mailer_secret
        )
        sys.exit()

def sheet_setup(spreadsheet, api_data_sheet_name, logs_sheet_name):
    '''Function to check if the provided sheet is has the worksheets to append data from API.\n
    In case the spreadsheet does not have the needed sheets, they will be created.
    Inputs:\n
        spreadsheet object, sheet name for data from api, sheet name for logs.
        Returns a dict with sheet names as keys and True / False values indicating of those sheets were created by the function.
    '''
    new_sheets_check = {}
    #Get worksheets' names of the spreadsheet to a list
    worksheet_titles = [ws.title for ws in spreadsheet.worksheets()]
    #Check if the sheets are present in the document
    for num, worksheet in enumerate([api_data_sheet_name, logs_sheet_name]):
        if not worksheet in worksheet_titles:
            print(f"No sheet with name {worksheet} located, creating one.")
            spreadsheet.add_worksheet(title = worksheet, rows = 1000, cols = 20)
            #Assign header to api data sheet
            if num == 0: spreadsheet.worksheet(worksheet).append_row(config.columns_api_data)
            #Assign header to log sheet 
            if num == 1: spreadsheet.worksheet(worksheet).append_row(config.columns_log_data)
            print(f"{worksheet} has been added to the spreadsheet.")
            new_sheets_check[worksheet] = True
        else:
            print(f"{worksheet} located in the file, no action needed.")
            new_sheets_check[worksheet] = False
            continue
    return new_sheets_check

def response_code_mapper(response):
    '''Function that maps request status code with its meaning and informs the user of the outcome.\n
    Sends an email in certain cases where the shutdown of the execution happens.'''
    status_code = response.status_code
    if status_code == 200: 
        print(status_code, 'Successful request')
        pass
    if status_code == 400:
        print('Bad Request, probably params are not correct')
        mail_alert(
            recipient = config.mail_receiver,
            sender = 'whatever',
            content = f'''The script encountered {status_code} response and stopped running.
            \nCheck your config and code!
            ''',
            mailer_secret = mailer_secret
        )
        sys.exit()
    if status_code == 404:
        print('No results matching query')
    if status_code == 401:
        print('Unauthorised response')
        mail_alert(
            recipient = config.mail_receiver,
            sender = 'whatever',
            content = f'''The script encountered {status_code} response and stopped running.
            \nCheck your access!
            ''',
            mailer_secret = mailer_secret
        )
        sys.exit()
    if status_code == 429:
        print('Request limit hit, need to wait.')
        sleep(config.rest_period_seconds)
    if status_code >= 500:
        print('No Response from the API')
        mail_alert(
            recipient = config.mail_receiver,
            sender = 'whatever',
            content = f'''The scripts encountered {status_code} response and stopped running.
            \nMost likely there is a problem with the API at the moment.
            ''',
            mailer_secret = mailer_secret
        )
        sys.exit()

#Object for interaction with Company House API
class Connector:
    '''Connector object to facilitate the connection to the companies house API'''
    hdr = {'Accept': 'application/json'}
    #Create a connector object that will be used to read data from the APIs
    def __init__(self, rest_key, stream_key):
        self.log_dict = {}
        self.log_dict['run_start_ts'] = str(datetime.now())
        self.rest_auth = HTTPBasicAuth(rest_key, '')
        self.stream_auth = HTTPBasicAuth(stream_key, '')
        #Initialise a request session to persist params for requests
        self.rest_sesh = requests.Session()
        self.rest_sesh.mount('https://', HTTPAdapter(max_retries = config.retry_cfg))
        self.rest_sesh.auth = (rest_key, '')
        self.rest_sesh.headers.update(self.hdr)
    
    @sleep_and_retry
    @limits(calls = config.rest_limit_requests, period = config.rest_period_seconds)
    def advanced_company_search(
        self,
        company_name = None,
        company_status = None,
        company_subtype = None,
        company_type = None,
        dissolved_from = None,
        dissolved_to = None,
        incorporated_from = None,
        incorporated_to = None,
        location = None,
        sic_codes = None,
        size = None,
        start_index = None    
    ):
        '''Function to perform an advanced search of companies via Company House API.\n
        Takes arguments that are specified in the documentation of the advanced search endpoint\n
        Decorators are applied to overcome the issue of getting 429 response.\n
        Returns response object.'''
        r = self.rest_sesh.get(
            f'{config.base_url_rest}/advanced-search/companies',
            params = {
                'company_name': company_name,
                'company_status': company_status,
                'company_subtype': company_subtype,
                'company_type': company_type,
                'dissolved_from': dissolved_from,
                'dissolved_to': dissolved_to,
                'incorporated_from': incorporated_from,
                'incorporated_to': incorporated_to,
                'location': location,
                'sic_codes': sic_codes,
                'size': size,
                'start_index': start_index  
            }
        )
        response_code_mapper(r)
        return r

    @sleep_and_retry
    @limits(calls = config.rest_limit_requests, period = config.rest_period_seconds)
    def get_company_data(self, company_number):
        '''Function to get a company's data from the Companies House.\n
        Company number is a mandatory argument. Returns response object.'''
        try:
            r = self.rest_sesh.get(f'{config.base_url_rest}/company/{company_number}')
        except:
            print('Hit requests limit, taking a pause.')
            sleep(int(config.rest_period_seconds * 0.9))
            r = self.rest_sesh.get(f'{config.base_url_rest}/company/{company_number}')
        response_code_mapper(r)
        return r

    def store_companies(self, company_list):
        try: self.companies = self.companies + company_list
        except AttributeError: self.companies = company_list

    @sleep_and_retry
    @limits(calls = config.rest_limit_requests, period = config.rest_period_seconds)
    def get_company_officers(
        self,
        company_number,
        items_per_page = None,
        register_type = None,
        register_view = None,
        start_index = None,
        order_by = None
        ):
        '''Function to retrieve data on company's officers.\n
        Company number is a mandatory input, other arguments are params for the API request according to the documentation.\n
        Returns response object.'''
        try:
            r = self.rest_sesh.get(
                f'{config.base_url_rest}/company/{company_number}/officers',
                params = {
                    'items_per_page': items_per_page,
                    'register_type': register_type,
                    'order_by': order_by
                }
                )
        except:
            print('Hit requests limit, taking a pause.')
            sleep(int(config.rest_period_seconds * 0.9))
            r = self.rest_sesh.get(f'{config.base_url_rest}/company/{company_number}')
        response_code_mapper(r)
        return r

    def validate_leads(self, gsheet_company_numbers, check_detailed_status = False):
        '''A function that checks if company profiles from the API relevant to the added to the Gsheet.\n
        The main conditions are:\n
        1. The company number is already present in the gsheet.
        2. Company's detailed status == "active-proposal-to-strike-off" (optional check, is not not by default)
        The function saves the validated list inside the Connector object as valid_companies attribute(list)
        '''
        if len(gsheet_company_numbers) == 0 and check_detailed_status == False:
            self.valid_companies = self.companies
            print('No Data Located in Spredsheet, all companies are valid!')
            return
        try: list_to_check  = self.companies
        except AttributeError:
            print('No companies stored in the connector, stopping execution')
            sys.exit()
        print('Formed a list of companies to check, starting the loop.')
        valid_companies = []
        for company in list_to_check:
            company_num = str(company['company_number'])
            if company_num not in gsheet_company_numbers:
                if check_detailed_status == False: valid_companies.append(company)
                else:
                   company_profile = self.get_company_data(str(company_num))
                   print(company_profile.text)
                   company_profile = json.loads(company_profile.text)
                   try:
                       if company_profile['company_status_detail'] not in config.status_details_exclusion_filtes: valid_companies.append(company)
                   #Except block for cases where a copmpany does not have status_detail
                   except KeyError:
                       valid_companies.append(company)
        self.valid_companies = valid_companies
        if self.valid_companies != []:
            print("Valid companies saved within the connector object")
        else:
            print('No companies passed validation, stopping the execution')
            sys.exit()
        print(f'''{len(self.valid_companies)} of {len(self.companies)} companies passed validation.''')            
          
    def prepare_interim_update(self):
        print("Updating interim")
        '''Function without arguments that converts companies data into a set of rows to paste to Gsheet.\n
        Returns a list of rows to append to the Gsheet'''
        update_data = self.valid_companies
        for entry in update_data:
            entry[config.columns_api_data[2]] = ", ".join(list(entry['registered_office_address'].values()))
            entry[config.columns_api_data[5]] = ", ".join(entry['sic_codes'])
        update_data = pd.DataFrame(update_data)
        update_data = update_data[config.columns_api_data[:6]]
        update_data[config.columns_api_data[7]] = self.log_dict['run_start_ts']
        update_data = update_data.to_numpy().tolist()
        return update_data
    
    def prepare_follow_up_update(self):
        '''Function to populate data on company officers and prepare it for pasting to the GSheet.\n
        Returns a list of valued to paste to the Gsheet'''
        try:
            follow_up = self.valid_companies
        except:
            print("No valid companoes to follow-up on, stopping execution!")
            sys.exit()
        #Call function to get officers data: loop
        problem_companies = []
        print('Collecting Data on officers from the API')
        for entry in follow_up:
            r = self.get_company_officers(entry['company_number'])
            print(r.text)
            try:
                officer_data = json.loads(r.text)['items']
            except:
                print('Unexpected Error, most likely a bad request, skipping the company')
                problem_companies.append(entry['company_number'])
                entry['officers_string'] = ''
                continue
            #Create a list of officers for the company
            officers = [f"{officer['officer_role']}: {officer['name']}" for officer in officer_data]
            #Save to Json
            entry['officers_string'] = "; ".join(officers)
        #Check for problematic entries and send an email if needed
        print('API data collection done, checking for problematic companies')
        if problem_companies != []:
            print(f"{len(problem_companies)} problematic companies located. Sending an email alert!")
            #Send mail with problematic companies
            mail_alert(
                recipient = config.mail_receiver,
                sender = 'whatever',
                content = f'''The following companies did not return officer data during {self.log_dict['run_start_ts']} run:
                {",".join(problem_companies)}''',
                mailer_secret = mailer_secret
            )
        #Store Data to Pandas and convert to list for update
        else: print('No problematic companies located.')
        print('Compiling update!')
        follow_up = [[item] for item in pd.DataFrame(follow_up)['officers_string']]
        print('Update compiled!')
        return follow_up

    def prepare_record_logs(self):
        '''Function to data from log_dict of the connector to the gsheet, requires no inputs\n
        Returns a list of values to append as row'''
        log_data = pd.DataFrame(self.log_dict).to_numpy().tolist()
        return log_data

    def stream_companies(self, timepoint = '', timeout_connect = '', timeout_read = ''):
        '''Function to connect and stream data from the Company House API. Not used in the tracker.'''
        s = requests.Session()
        session_start_ts = datetime.now()
        try:
            r = s.get(
                config.stream_company_url,
                auth = self.stream_auth,
                headers = self.hdr,
                params = {
                    'timepoint': timepoint
                },
                stream = True,
                timeout = (timeout_connect, timeout_read)
                )
        except Timeout:
                print('Streaming timed out, stopping execution.')
                sys.exit()
        for line in r.iter_lines():
            resp_ts = datetime.now()
            if (resp_ts - session_start_ts).total_seconds() > config.streaming_session_max_duration:
                print('Streaming session execution time limit, stopping execution.')
                sys.exit()
            if line:
                if r.status_code == 429:
                    print('Stremer hit the rate limit, stopping execution.')
                    sys.exit()
                if r.status_code == 401:
                    print('Wrong auth, stopping execution.')
                    sys.exit()
                if r.status_code == 401:
                    print('Wrong timepoint, stopping execution.')
                    sys.exit()
                print(type(line), resp_ts)
                print(json.loads(line))
        print('Streaming done')
