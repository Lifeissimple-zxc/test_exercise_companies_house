from requests.adapters import Retry
#session params for retries
retry_cfg = Retry(
    total = 5,
    backoff_factor = 0.5,
    status_forcelist = [429]
)

stream_company_url = 'https://stream.companieshouse.gov.uk/companies'
streaming_session_max_duration = 300
base_url_rest = 'https://api.company-information.service.gov.uk'
sic_codes_resto = [
    '56101',
    '56102',
    '56103'
]
max_age_days = 60 #Data for sales is only valid for 60 days hence is this value
#rest api calls limits
rest_period_seconds = 290 #300 - 10 sec buffer
rest_limit_requests = 530 #600 - 70 (buffer not to push the limit to get 429)
#filters for company advanced search
status_filters = [
    #https://www.1stformations.co.uk/blog/company-status/
    #https://doorda.com/glossary/company-statuses/
    #sources based on which some statuses should be excldued
    'active',
    #'dissolved',
    #'liquidation',
    #'receivership',
    #'administration',
    #'voluntary-arrangement'
    #'converted-closed',
    #'insolvency-proceedings',
    #'registered', #returns a bad request even if used alone so decided to comment it
    #'removed',
    #'closed',
    'open'
]
status_details_exclusion_filtes = [
    #'transferred-from-uk',
    'active-proposal-to-strike-off'
    #'petition-to-restore-dissolved'
    #'transformed-to-se'
    #'converted-to-plc'
]
#Gsheet Data
gsheet = {
    'url': 'https://docs.google.com/spreadsheets/d/1d_chjFaU8vxkmcCv2uiJPwBmPRbPOp0LMVchestdqbo/edit#gid=0',
    'api_data_sheet_name': 'leads_from_api',
    'logs_sheet_name': 'logs_sheet',
    'scope': ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
}
columns_api_data = [
    'company_name',
    'company_number',
    'address_string',
    'date_of_creation',
    'company_type',
    'sic_codes_string',
    'run_added_ts',
    'company_officer_names'
]

columns_log_data = [
    'run_start_ts',
    'companies_collected',
    'data_collection_done_ts',
    'validated_companies',
    'validation_done_ts',
    'interim_update_ts',
    'follow_up_done_ts',
    'run_finished_ts'
]

officer_column = 'H'

mail_receiver = 'mararkarp@gmail.com'