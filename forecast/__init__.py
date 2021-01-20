from __future__ import print_function
import logging
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import \
    NoSuchElementException, TimeoutException, InvalidArgumentException, WebDriverException
# from googletrans import Translator, constants
from google_trans_new import google_translator
import schedule
import time
# from datetime import datetime, timedelta
import datetime
today = datetime.date.today()
next_week = today + datetime.timedelta(days=7)
import urllib.request
from apiclient import discovery
from google.oauth2 import service_account
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient


def multiply_round(df, new_col, col, factor):
    df[new_col] = df[col] * factor
    df[new_col] = df[new_col].astype(int)


def forecast():
    ## get_data returns a dataframe df with the data shown
    # in the main table of 'https://www.corona.ps/details'.
    # It also translates the text in the table from arabic to english.
    # Besides returning df it also generates a file named
    # 'COVID_ps_%timestamp.csv' in the same folder containting this file.,
    # The timestamp correspond to the actual date.
    
    # Initialise Azure blob service
    credentials = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(credentials)
    
    #Website with data
    # URL_or = 'https://www.corona.ps/details'
    URL_or = 'https://www.corona.ps/'
    
    #Magic
    page = requests.get(URL_or)
    soup = BeautifulSoup(page.content, 'html.parser')
    #Find tables in webpage
    tables = soup.find_all("table")
    
    if not tables: # if the corona.ps is not responsive
        print("https://www.corona.ps/ is not accessible at the moment")
        container_client = blob_service_client.get_container_client('covid-otp-fcdata')
        blob_list = container_client.list_blobs(name_starts_with="COVID_ps_")
        print(blob_list)
        #list_properties = pd.DataFrame()
        #for blob in blob_list:
        #    list_properties[0] = blob
        #    list_properties[1] = blob.properties.last_modified

        table = max(blob_list, key=os.path.getctime)
        blob_service_client.get_blob_to_path('covid-otp-fcdata', table, 'blob_ps')
        df_ps = pd.read_csv('blob_ps')
    else:
        #tables[4] has the necessary data
        table = tables[4]
        tab_data = [[cell.text for cell in row.find_all(["th","td"])]
                            for row in table.find_all("tr")]
        #generate dataframe
        df_ps = pd.DataFrame(tab_data)

        # init the Google API translator
        # translator = Translator()
        translator = google_translator() 

        #translate the first row with titles
        for i in range(0, df_ps.shape[1]):
            translation =  translator.translate(df_ps[i][0], lang_tgt="en")
            df_ps[i][0] = translation
        #now translate the 0th column (with governorates)
        for i in range(1, df_ps.shape[0]):
            translation =  translator.translate(df_ps[0][i], lang_tgt="en")
            df_ps[0][i] = translation
        
        #Header 
        df_ps = df_ps.rename(columns=df_ps.iloc[0]).drop(df_ps.index[0])
        
        # Returns a datetime object containing the local date and time
        # dateTimeObj = datetime.now()
        dateTimeObj = today
        timestampStr = dateTimeObj.strftime("%d-%b-%Y")
        name_df = 'COVID_ps_'+ timestampStr + '.csv'
        
        output = df_ps.to_csv()
        
        # save csv to azure blob
        blob_client = blob_service_client.get_blob_client(container='covid-otp-fcdata', blob=name_df)
        blob_client.upload_blob(output, blob_type="BlockBlob")
        
        print('Dataframe generated\nSaved as '+ name_df)

    # initialize google sheets API
    scopes = ['https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/spreadsheets']
    credentials = service_account.Credentials.from_service_account_file('service_account_key.json', scopes=scopes)
    # pylint: disable=maybe-no-member
    service = discovery.build('sheets', 'v4', credentials=credentials)
    
    # get latest forecast data
    fc_latest = urllib.request.urlretrieve("https://raw.githubusercontent.com/mrc-ide/global-lmic-reports/master/PSE/projections.csv", \
                            "data/forecast_latest.csv")
    # https://github.com/mrc-ide/global-lmic-reports/blob/master/PSE/projections.csv
        # save csv to azure blob
    
    blob_client = blob_service_client.get_blob_client(container='covid-otp-fcdata', blob='forecast_latest.csv')
    blob_client.upload_blob(fc_latest, blob_type="BlockBlob")

    blob_service_client.get_blob_to_path('covid-otp-fcdata', 'forecast_latest.csv', 'blob_fc')
    df = pd.read_csv('blob_fc')
    df['date'] = pd.to_datetime(df['date'])

    # calculate future cases (in the next 7 days)
    df_new_cases = df[(df['scenario'] == 'Surged Maintain Status Quo') & (df['compartment'] == 'infections')]
    df_new_cases = df_new_cases[['date', 'y_25', 'y_median', 'y_75']]
    df_new_cases = df_new_cases[(df_new_cases['date'] > pd.to_datetime(today)) & (df_new_cases['date'] <= pd.to_datetime(next_week))].sum()

    # calculate future hospitalizations (in the next 7 days)
    # df_new_hosp = df[(df['scenario'] == 'Surged Maintain Status Quo') & (df['compartment'] == 'hospital_incidence')]
    # df_new_hosp = df_new_hosp[['date', 'y_25', 'y_median', 'y_75']]
    # df_new_hosp = df_new_hosp[(df_new_hosp['date'] > today) & (df_new_hosp['date'] <= next_week)].sum()

    # get latest data on cases and hospitalizations
    # spreadsheetId = '1HZm2kQTodAC2Bz7l2Uw77RTGjtEOc5ywtrpzOF_oxtM'
    # rangeName = 'Districts!A:K'
    # result = service.spreadsheets().values().get(
    #     spreadsheetId=spreadsheetId, range=rangeName).execute()
    # values = result.get('values', [])
    # df = pd.DataFrame.from_records(values)[1:] # convert to pandas dataframe
    # dateTimeObj = datetime.now()
    #dateTimeObj = today
    #timestampStr = dateTimeObj.strftime("%d-%b-%Y")
    #name_df = 'COVID_ps_'+ timestampStr + '.csv'

    #blob_service_client.get_blob_to_path('covid-otp-fcdata', name_df, 'blob_cases')
    #df = pd.read_csv('blob_cases')
    # calculate proportion of new cases (in the last 7 days) per district
    df_ps.iloc[:,3] = df_ps.iloc[:,3].astype(str).apply(lambda x: x.replace(',','')).astype(int) # 3th column contains new cases in the last 7 days, per district
    total_new_cases = df_ps.iloc[:,3].sum()
    df_ps['proportion_new_cases'] = df_ps.iloc[:,3] / total_new_cases

    # calculate future cases and hosp. per district based on the proportion of new cases per district
    # multiply_round(df, 'new_hosp_min', 'proportion_new_cases', df_new_hosp['y_25'])
    # multiply_round(df, 'new_hosp_mean', 'proportion_new_cases', df_new_hosp['y_median'])
    # multiply_round(df, 'new_hosp_max', 'proportion_new_cases', df_new_hosp['y_75'])
    multiply_round(df_ps, 'new_cases_min', 'proportion_new_cases', df_new_cases['y_25'])
    multiply_round(df_ps, 'new_cases_mean', 'proportion_new_cases', df_new_cases['y_median'])
    multiply_round(df_ps, 'new_cases_max', 'proportion_new_cases', df_new_cases['y_75'])

    # reformat data and push to google sheets
    data_to_upload = [['New Cases (min)', 'New Cases (mean)', 'New Cases (max)']] +\
                    df_ps[['new_cases_min', 'new_cases_mean', 'new_cases_max']].values.tolist()
    TargetSpreadsheetId = '17E5dC_v186JcthIFLFZCNWG_jlKhD73_tOF9R2_TlM4'
    TargetRangeName = 'Forecast!B:D'
    body = {
    "range": TargetRangeName,
    "values": data_to_upload
    }
    value_input_option = 'USER_ENTERED'
    result = service.spreadsheets().values().update(
        spreadsheetId=TargetSpreadsheetId, range=TargetRangeName, valueInputOption=value_input_option, body=body).execute() 


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due')
    try:
        forecast()
    except Exception as e:
        logging.error('Error:')
        logging.error(e)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
