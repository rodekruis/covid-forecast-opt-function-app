from __future__ import print_function
from forecast.settings import *
import logging
import os
import io
import requests
from bs4 import BeautifulSoup
import pandas as pd
from googletrans import Translator
from matplotlib import pyplot as plt
import time
import datetime

today = datetime.date.today()
last_week = today - datetime.timedelta(days=7)
next_week = today + datetime.timedelta(days=7)
import urllib.request
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings


class CovidForecast:
    def multiply_round(self, df, new_col, col, factor):
        df[new_col] = df[col] * factor
        df[new_col] = df[new_col].astype(int)

    def translate(self, df):
        # init the Google API translator
        translator = Translator()
        # translator = google_translator()

        # translate the first row with titles
        for i in range(0, df.shape[1]):
            translation = translator.translate(df[i][0], dest="en")
            df[i][0] = translation.text

        # now translate the 0th column (with governorates)
        for i in range(1, df.shape[0]):
            translation = translator.translate(df[0][i], dest="en")
            df[0][i] = translation.text
        return df

    def authenticate_container(self):

        # Initialise Azure blob service
        credentials = os.environ["AzureWebJobsStorage"]
        self.blob_service_client = BlobServiceClient.from_connection_string(credentials)

    def access_reported_data(self, URL_report):

        timeToTryAccess = 6000
        timeToRetry = 600

        # Magic
        try:
            page = requests.get(URL_report)
            accessDone = True
        except urllib.error.URLError:
            logging.info("corona.ps access failed. " "Trying again in 10 minutes")
            time.sleep(timeToRetry)
        if accessDone == False:
            logging.error(
                "ERROR: corona.ps access failed for "
                + str(timeToTryAccess / 3600)
                + " hours"
            )
            raise ValueError()

        soup = BeautifulSoup(page.content, "html.parser")
        # Find tables in webpage
        tables = soup.find_all("table")

        return tables

    def get_report_data(self):

        tables = self.access_reported_data(URL_report)

        if not tables:  # if the corona.ps is not responsive
            print("https://www.corona.ps/ is not accessible at the moment")
            container_client = self.blob_service_client.get_container_client(
                "covid-opt-fc-data"
            )
            blob_list = container_client.list_blobs(name_starts_with="COVID_ps_")
            list_properties = pd.DataFrame(columns=("file", "date"))
            i = 0
            for blob in blob_list:
                list_properties.loc[i] = [blob, blob.last_modified]
                i += 1
            file_ps = list_properties.loc[list_properties["date"].idxmax()]["file"]
            blob_table = self.blob_service_client.get_blob_client(
                "covid-opt-fc-data", file_ps
            )
            table = io.StringIO(str(blob_table.download_blob().readall(), "utf-8"))
            self.IsReportAccessible = False
            self.timestampStr = table.split("_")[-1].split(".")[0]
            df_report = pd.read_csv(table)
        else:
            # tables[4] has the necessary data
            table = tables[4]
            tab_data = [
                [cell.text for cell in row.find_all(["th", "td"])]
                for row in table.find_all("tr")
            ]
            # generate dataframe
            df_report = pd.DataFrame(tab_data)

            df_report = self.translate(df_report)

            # Header
            df_report.loc[0] = df_report.loc[0].str.strip()
            df_report = df_report.rename(columns=df_report.iloc[0]).drop(
                df_report.index[0]
            )

            # Returns a datetime object containing the local date and time
            self.IsReportAccessible = True
            dateTimeObj = today
            self.timestampStr = dateTimeObj.strftime("%d-%b-%Y")
            name_df = "COVID_ps_" + self.timestampStr + ".csv"

            output = df_report.to_csv()

            # save csv to azure blob
            blob_client = self.blob_service_client.get_blob_client(
                "covid-opt-fc-data", name_df
            )
            blob_client.upload_blob(output, blob_type="BlockBlob", overwrite=True)

            logging.info("Dataframe generated. Saved as " + name_df)

        return df_report

    # Hello there. What's up?

    def get_MRC_data(self):

        # get latest forecast data
        df_projection = pd.read_csv(URL_forecast)
        df_csv = df_projection.to_csv()
        # save csv to azure blob
        blob_fc = self.blob_service_client.get_blob_client(
            "covid-opt-fc-data", "forecast_latest.csv"
        )
        blob_fc.upload_blob(df_csv, blob_type="BlockBlob", overwrite=True)

        df_projection["date"] = pd.to_datetime(df_projection["date"])

        # calculate future cases (in the next 7 days)
        df_new_cases = df_projection[
            (df_projection["scenario"] == "Surged Maintain Status Quo")
            & (df_projection["compartment"] == "infections")
        ]
        df_new_cases = df_new_cases[["date", "y_25", "y_median", "y_75"]]
        df_new_cases = df_new_cases[
            (df_new_cases["date"] > pd.to_datetime(last_week))
            & (df_new_cases["date"] <= pd.to_datetime(next_week))
        ].reset_index()

        # calculate future ICU incidence (in the next 7 days)
        df_icu_inci = df_projection[
            (df_projection["scenario"] == "Surged Maintain Status Quo")
            & (df_projection["compartment"] == "ICU_incidence")
        ]
        df_icu_inci = df_icu_inci[["date", "y_25", "y_median", "y_75"]]
        df_icu_inci = df_icu_inci[
            (df_icu_inci["date"] > pd.to_datetime(last_week))
            & (df_icu_inci["date"] <= pd.to_datetime(next_week))
        ]

        return df_new_cases, df_icu_inci

    def get_IHME_data(self):

        # get latest forecast data
        df_projection = pd.read_csv(URL_forecast)
        df_csv = df_projection.to_csv()
        # save csv to azure blob
        blob_fc = self.blob_service_client.get_blob_client(
            "covid-opt-fc-data", "forecast_latest.csv"
        )
        blob_fc.upload_blob(df_csv, blob_type="BlockBlob", overwrite=True)

        # # get forecast data from blob
        # forecast_blob_client = self.blob_service_client.get_blob_client(
        #     "covid-opt-fc-data", forecast_file_name
        # )  # TODO: change storage name
        # forecast_file_path = "./" + forecast_file_name
        # with open(forecast_file_path, "wb") as download_file:
        #     download_file.write(forecast_blob_client.download_blob().readall())
        # df_projection = pd.read_csv(forecast_file_path)

        df_projection["date"] = pd.to_datetime(df_projection["date"])

        # calculate future cases (in the next 7 days)
        df_new_cases = df_projection[df_projection["location_name"] == "Palestine"]
        df_new_cases = df_new_cases[
            ["date", "cases_lower", "cases_mean", "cases_upper"]
        ]
        df_new_cases = df_new_cases[
            (df_new_cases["date"] > pd.to_datetime(last_week))
            & (df_new_cases["date"] <= pd.to_datetime(next_week))
        ].reset_index()

        df_new_cases = df_new_cases.rename(
            columns={
                "cases_lower": "y_25",
                "cases_mean": "y_median",
                "cases_upper": "y_75",
            }
        )

        # calculate future ICU incidence (in the next 7 days)
        df_icu_inci = df_projection[df_projection["location_name"] == "Palestine"]
        df_icu_inci = df_icu_inci[
            ["date", "icu_beds_lower", "icu_beds_mean", "icu_beds_upper"]
        ]
        df_icu_inci = df_icu_inci[
            (df_icu_inci["date"] > pd.to_datetime(last_week))
            & (df_icu_inci["date"] <= pd.to_datetime(next_week))
        ]

        # save ICU forecast as csv
        df_icu_inci_csv = df_icu_inci.reset_index(drop=True)
        df_icu_inci_csv = df_icu_inci_csv.to_csv()
        blob_df = self.blob_service_client.get_blob_client(
            "covid-opt-fc-outputs", str(today) + "_ICUincidence_forecast.csv"
        )
        blob_df.upload_blob(df_icu_inci_csv, blob_type="BlockBlob", overwrite=True)

        df_icu_inci = df_icu_inci.rename(
            columns={
                "icu_beds_lower": "y_25",
                "icu_beds_mean": "y_median",
                "icu_beds_upper": "y_75",
            }
        )

        return df_new_cases, df_icu_inci

    def forecast_new_cases(self, df_report, df_new_cases):

        # calculate proportion of new cases (in the last 7 days) per district
        df_report.iloc[:, 2] = (
            df_report.iloc[:, 2]
            .astype(str)
            .apply(lambda x: x.replace(",", ""))
            .astype(int)
        )  # 3th column contains new cases in the last 7 days, per district
        df_report.iloc[:, 1] = (
            df_report.iloc[:, 1]
            .astype(str)
            .apply(lambda x: x.replace(",", ""))
            .astype(int)
        )  # 2th column contains all time total cases, per district

        # Gaza breakdown
        total_new_cases = df_report.iloc[:, 2].sum()
        # df_gaza = pd.DataFrame(columns=df_report.columns)
        # df_gaza.iloc[:,0] = ['Jabalia', 'Gaza City', 'Der Albalah', 'Khan Younis', 'Rafah']
        # gaza_cases = [9237, 21101, 5564, 8399, 4611]
        # gaza_ratios = [cases/48912 for cases in gaza_cases]          # figures from Jan 20, 2021

        if total_new_cases != 0:
            # gaza_today = df_report[df_report['Governorate'].str.contains("Gaza")].iloc[0,2]
            # df_gaza.iloc[:,2] = [gaza_today*i for i in gaza_ratios]
            # df_report = df_report.append(df_gaza)
            # df_report = df_report[~df_report['Governorate'].isin(["Gaza strip"])]
            df_report["proportion_new_cases"] = df_report.iloc[:, 2] / total_new_cases
            self.IsReportLatest = True
        else:
            # gaza_today = df_report[df_report['Governorate'].str.contains("Gaza")].iloc[0,1]
            # df_gaza.iloc[:,1] = [gaza_today*i for i in gaza_ratios]
            # df_report = df_report.append(df_gaza)
            # df_report = df_report[~df_report['Governorate'].isin(["Gaza strip"])]

            total_new_cases = df_report.iloc[:, 1].sum()
            df_report["proportion_new_cases"] = df_report.iloc[:, 1] / total_new_cases
            self.IsReportLatest = False

        # CALCULATE RATIO AND PROJECT FOR EACH GOVERNORATE

        df_report_week = pd.DataFrame()
        for i in range(len(df_new_cases)):
            df_report_date = df_report
            # calculate future cases and hosp. per district based on the proportion of new cases per district
            # multiply_round(df, 'new_hosp_min', 'proportion_new_cases', df_new_hosp['y_25'])
            # multiply_round(df, 'new_hosp_mean', 'proportion_new_cases', df_new_hosp['y_median'])
            # multiply_round(df, 'new_hosp_max', 'proportion_new_cases', df_new_hosp['y_75'])
            self.multiply_round(
                df_report,
                "new_cases_min",
                "proportion_new_cases",
                df_new_cases.loc[i, "y_25"],
            )
            self.multiply_round(
                df_report,
                "new_cases_mean",
                "proportion_new_cases",
                df_new_cases.loc[i, "y_median"],
            )
            self.multiply_round(
                df_report,
                "new_cases_max",
                "proportion_new_cases",
                df_new_cases.loc[i, "y_75"],
            )
            df_report_date["date"] = df_new_cases.loc[i, "date"]
            df_report_week = df_report_week.append(df_report_date)

        # EXPORT FORECAST OUTPUTS AS CSV
        df_report_week_csv = df_report_week.copy()
        df_report_week_csv["date"] = df_report_week_csv["date"].dt.strftime("%Y%m%d")
        df_report_week_csv = df_report_week_csv[
            ["Governorate", "date", "new_cases_min", "new_cases_mean", "new_cases_max"]
        ].reset_index(drop=True)
        df_out1 = df_report_week_csv.to_csv()
        blob_df = self.blob_service_client.get_blob_client(
            "covid-opt-fc-outputs", str(today) + "_covid_forecast.csv"
        )
        blob_df.upload_blob(df_out1, blob_type="BlockBlob", overwrite=True)

        return df_report_week

    def plot_icu(self, df_icu_inci):

        # plot ICU forecast
        fig1, ax1 = plt.subplots(figsize=(15, 7), dpi=300)
        ax1.axvline(today, linestyle="dashed", label="Today", color="k")
        ax1.plot(
            df_icu_inci["date"],
            df_icu_inci["y_median"],
            color="crimson",
            label="Median ICU incidence",
        )
        ax1.fill_between(
            df_icu_inci["date"],
            df_icu_inci["y_25"],
            df_icu_inci["y_75"],
            label="Confidence range",
            color="crimson",
            alpha=0.35,
        )
        ax1.set_title("NEW ICU INCIDENCE FORECAST", fontweight="bold", fontsize=16)
        ax1.set(xlabel="Date", ylabel="New cases")
        ax1.set_ylim(bottom=0)
        ax1.legend(loc="upper right")  # bbox_to_anchor=(1.0, 1.0)
        plt.gcf().text(
            0.02,
            0.05,
            "{0} forecast data is up-to-date on {1}.".format(
                forecast_source, self.timestampStr
            ),
        )
        if self.IsReportAccessible and self.IsReportLatest:
            plt.gcf().text(
                0.02,
                0.02,
                "MoH daily report data from corona.ps is up-to-date on {0}.".format(
                    self.timestampStr
                ),
            )
        elif self.IsReportAccessible and not self.IsReportLatest:
            plt.gcf().text(
                0.02,
                0.02,
                "MoH daily report data from corona.ps is not yet updated on {0}. Forecast is made with Total cases instead of Daily new cases.".format(
                    self.timestampStr
                ),
            )
        elif not self.IsReportAccessible:
            plt.gcf().text(
                0.02,
                0.02,
                "Site corona.ps is temporarily inaccessible. Forecast is made with MoH daily data from corona.ps on {0}.".format(
                    self.timestampStr
                ),
            )
        ax1.grid()
        io_fig1 = io.BytesIO()
        fig1.savefig(io_fig1, format="png")
        io_fig1.seek(0)
        plt.close()
        blob_fig1 = self.blob_service_client.get_blob_client(
            "covid-opt-fc-outputs", str(today) + "_ICU_forecast.png"
        )
        blob_fig1.upload_blob(
            io_fig1.read(),
            blob_type="BlockBlob",
            overwrite=True,
            content_settings=ContentSettings(content_type="image/png"),
        )

    def plot_new_cases(self, df_report_week):
        # plot new cases forecast per governorate
        for i, m in df_report_week.groupby("Governorate"):
            fig, ax = plt.subplots(figsize=(15, 7), dpi=300)
            ax.axvline(today, linestyle="dashed", label="Today", color="k")
            ax.plot(m["date"], m["new_cases_mean"], label="Median cases")
            ax.fill_between(
                m["date"],
                m["new_cases_min"],
                m["new_cases_max"],
                label="Confidence range",
                alpha=0.35,
            )
            ax.set_title(
                "NEW COVID-19 CASES FORECAST \n {0}".format(str(i)),
                fontweight="bold",
                fontsize=16,
            )
            ax.set(xlabel="Date", ylabel="New cases")
            ax.legend(loc="upper right")  # bbox_to_anchor=(1.0, 1.0)
            ax.set_ylim(
                [
                    round(m["new_cases_min"].min() - m["new_cases_min"].min() * 0.2),
                    round(m["new_cases_max"].max() + m["new_cases_max"].max() * 0.2),
                ]
            )
            plt.gcf().text(
                0.02,
                0.05,
                "{0} forecast data is up-to-date on {1}.".format(
                    forecast_source, self.timestampStr
                ),
            )
            if self.IsReportAccessible and self.IsReportLatest:
                plt.gcf().text(
                    0.02,
                    0.02,
                    "MoH daily report data from corona.ps is up-to-date on {0}.".format(
                        self.timestampStr
                    ),
                )
            elif self.IsReportAccessible and not self.IsReportLatest:
                plt.gcf().text(
                    0.02,
                    0.02,
                    "MoH daily report data from corona.ps is not yet updated on {0}. Forecast is made with Total cases instead of Daily new cases.".format(
                        self.timestampStr
                    ),
                )
            elif not self.IsReportAccessible:
                plt.gcf().text(
                    0.02,
                    0.02,
                    "Site corona.ps is temporarily inaccessible. Forecast is made with MoH daily data from corona.ps on {0}.".format(
                        self.timestampStr
                    ),
                )
            ax.grid()
            io_fig = io.BytesIO()
            fig.savefig(io_fig, format="png")
            io_fig.seek(0)
            plt.close()
            blob_fig = self.blob_service_client.get_blob_client(
                "covid-opt-fc-outputs",
                str(today) + "_" + str(i) + "_covid_forecast.png",
            )
            blob_fig.upload_blob(
                io_fig.read(),
                blob_type="BlockBlob",
                overwrite=True,
                content_settings=ContentSettings(content_type="image/png"),
            )

