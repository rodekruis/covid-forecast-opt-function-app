from __future__ import print_function
import logging
import os
import json
import io
import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import \
    NoSuchElementException, TimeoutException, InvalidArgumentException, WebDriverException
# from googletrans import Translator, constants
from google_trans_new import google_translator
from matplotlib import pyplot as plt
import schedule
import time
# from datetime import datetime, timedelta
import datetime
today = datetime.date.today()
last_week = today - datetime.timedelta(days=7)
next_week = today + datetime.timedelta(days=7)
import urllib.request
from apiclient import discovery
# from apiclient.http import MediaFileUpload
from google.oauth2 import service_account
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings


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
        container_client = blob_service_client.get_container_client('covid-opt-fc-data')
        blob_list = container_client.list_blobs(name_starts_with="COVID_ps_")
        list_properties = pd.DataFrame(columns=("file","date"))
        i=0
        for blob in blob_list:
            list_properties.loc[i] = [blob, blob.last_modified]
            i+=1
        file_ps = list_properties.loc[list_properties['date'].idxmax()]["file"]
        blob_table = blob_service_client.get_blob_client('covid-opt-fc-data', file_ps)
        table = io.StringIO(str(blob_table.download_blob().readall(), "utf-8"))
        df_ps = pd.read_csv(table)
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
        blob_client = blob_service_client.get_blob_client('covid-opt-fc-data', name_df)
        blob_client.upload_blob(output, blob_type="BlockBlob", overwrite=True)
        
        print('Dataframe generated\nSaved as '+ name_df)
 
    # get latest forecast data
    URL_fc = "https://raw.githubusercontent.com/mrc-ide/global-lmic-reports/master/PSE/projections.csv"
    # https://github.com/mrc-ide/global-lmic-reports/blob/master/PSE/projections.csv
    df = pd.read_csv(URL_fc)
    df_csv = df.to_csv()
    # save csv to azure blob
    blob_fc = blob_service_client.get_blob_client('covid-opt-fc-data', 'forecast_latest.csv')
    blob_fc.upload_blob(df_csv, blob_type="BlockBlob", overwrite=True)

    df['date'] = pd.to_datetime(df['date'])

    # calculate future cases (in the next 7 days)
    df_new_cases = df[(df['scenario'] == 'Surged Maintain Status Quo') & (df['compartment'] == 'infections')]
    df_new_cases = df_new_cases[['date', 'y_25', 'y_median', 'y_75']]
    df_new_cases = df_new_cases[(df_new_cases['date'] > pd.to_datetime(last_week)) & (df_new_cases['date'] <= pd.to_datetime(next_week))].reset_index()

    # calculate future ICU incidence (in the next 7 days)
    df_icu_inci = df[(df['scenario'] == 'Surged Maintain Status Quo') & (df['compartment'] == 'ICU_incidence')]
    df_icu_inci = df_icu_inci[['date', 'y_25', 'y_median', 'y_75']]
    df_icu_inci = df_icu_inci[(df_icu_inci['date'] > pd.to_datetime(last_week)) & (df_icu_inci['date'] <= pd.to_datetime(next_week))]

    # initialize google sheets API
    scopes = ['https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/spreadsheets']
    blob_service_key = blob_service_client.get_blob_client('azure-webjobs-secrets', 'service_account_key.json')
    service_key = json.loads(blob_service_key.download_blob().readall())
    credentials = service_account.Credentials.from_service_account_info(service_key, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials, cache_discovery=False)

    
    df_ps.iloc[:,2] = df_ps.iloc[:,2].astype(str).apply(lambda x: x.replace(',','')).astype(int) # 3th column contains new cases in the last 7 days, per district
    df_gaza = pd.DataFrame(columns=df_ps.columns)

    # Gaza breakdown
    gaza_governorates = ['Jabalia', 'Gaza City', 'Der Albalah', 'Khan Younis', 'Rafah']
    gaza_cases = [9237, 21101, 5564, 8399, 4611]
    gaza_ratios = [cases/48912 for cases in gaza_cases]          # figures from Jan 20, 2021
    for i in range(len(gaza_governorates)):
        df_gaza.loc[i, 'Governorate '] = gaza_governorates[i]
        df_gaza.loc[i, 'Cases today '] = float(df_ps[df_ps['Governorate ']=="Gaza strip "]['Cases today '].values)*gaza_ratios[i]
    df_ps = df_ps.append(df_gaza)
    df_ps = df_ps[~df_ps['Governorate '].isin(["Gaza strip "])]

    # CALCULATE RATIO AND PROJECT FOR EACH GOVERNORATE 
    total_new_cases = df_ps.iloc[:,2].sum()
    df_ps['proportion_new_cases'] = df_ps.iloc[:,2] / total_new_cases

    df_ps_week = pd.DataFrame()
    for i in range(len(df_new_cases)):
        df_ps_date = df_ps
        # calculate future cases and hosp. per district based on the proportion of new cases per district
        # multiply_round(df, 'new_hosp_min', 'proportion_new_cases', df_new_hosp['y_25'])
        # multiply_round(df, 'new_hosp_mean', 'proportion_new_cases', df_new_hosp['y_median'])
        # multiply_round(df, 'new_hosp_max', 'proportion_new_cases', df_new_hosp['y_75'])
        multiply_round(df_ps, 'new_cases_min', 'proportion_new_cases', df_new_cases.loc[i, 'y_25'])
        multiply_round(df_ps, 'new_cases_mean', 'proportion_new_cases', df_new_cases.loc[i, 'y_median'])
        multiply_round(df_ps, 'new_cases_max', 'proportion_new_cases', df_new_cases.loc[i, 'y_75'])
        df_ps_date["date"] = df_new_cases.loc[i,'date'] 
        df_ps_week = df_ps_week.append(df_ps_date)

    # plot ICU forecast
    fig1, ax1 = plt.subplots(figsize=(20, 10), dpi=300)
    ax1.plot(df_icu_inci['date'], df_icu_inci['y_median'], color="crimson", label='ICU incidence')
    ax1.fill_between(df_icu_inci['date'],
            df_icu_inci['y_25'],
            df_icu_inci['y_75'], color="crimson", alpha=0.35)
    ax1.axvline(today, linestyle='dashed', label="Today", color="k")
    ax1.set(title="ICU incidence forecast", xlabel="Date", ylabel="New cases")
    ax1.set_ylim(bottom=0)
    ax1.legend(bbox_to_anchor=(1.0, 1.0), loc='upper left')
    ax1.grid()
    io_fig1 = io.BytesIO()
    fig1.savefig(io_fig1, format='png')
    io_fig1.seek(0)
    plt.close()
    blob_fig1 = blob_service_client.get_blob_client('covid-opt-fc-outputs', str(today) + '_ICU_forecast.png')
    blob_fig1.upload_blob(io_fig1.read(), blob_type="BlockBlob", overwrite=True)

    # plot new cases forecast per governorate
    for i, m in df_ps_week.groupby('Governorate '):
        fig, ax = plt.subplots(figsize=(15, 7), dpi=300)
        ax.plot(m['date'], m['new_cases_mean'], label=i)
        ax.fill_between(m['date'],
                m['new_cases_min'],
                m['new_cases_max'], alpha=0.35)
        ax.axvline(today, linestyle='dashed', label="Today", color="k")
        ax.set(title="COVID cases forecast "+ str(i), xlabel="Date", ylabel="New cases")
        ax.legend(bbox_to_anchor=(1.0, 1.0), loc='upper left')
        ax.grid()
        io_fig = io.BytesIO()
        fig.savefig(io_fig, format='png')
        io_fig.seek(0)
        plt.close()
        blob_fig = blob_service_client.get_blob_client('covid-opt-fc-outputs', str(today) + "_" + str(i) + '_covid_forecast.png')
        blob_fig.upload_blob(io_fig.read(), blob_type="BlockBlob", overwrite=True, content_settings=ContentSettings(content_type='image/png'))

    df_ps_week['date'] = df_ps_week['date'].dt.strftime('%Y%m%d')

    # EXPORT FORECAST OUTPUTS AS CSV
    df_to_export = df_ps_week[['Governorate ', 'date', 'new_cases_min', 'new_cases_mean', 'new_cases_max']].reset_index(drop=True)
    df_out1 = df_to_export.to_csv()
    blob_df = blob_service_client.get_blob_client('covid-opt-fc-outputs', str(today) + '_covid_forecast.csv')
    blob_df.upload_blob(df_out1, blob_type="BlockBlob", overwrite=True)
    df_icu_inci = df_icu_inci.rename(columns={'y_25' : 'icu_indicence_min', 'y_median': 'icu_indicence_mean', 'y_75': 'icu_indicence_max'}).reset_index(drop=True)
    df_out2 = df_icu_inci.to_csv()
    blob_df = blob_service_client.get_blob_client('covid-opt-fc-outputs', str(today) + '_ICUincidence_forecast.csv')
    blob_df.upload_blob(df_out2, blob_type="BlockBlob", overwrite=True)



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
