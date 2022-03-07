# Forecast data sources:
forecast_source = "IHME"  # "MRC"/ "IHME"

# Forecast data
if forecast_source == "MRC":
    URL_forecast = "https://raw.githubusercontent.com/mrc-ide/global-lmic-reports/master/PSE/projections.csv"
elif forecast_source == "IHME":
    URL_forecast = "https://ihmecovid19storage.blob.core.windows.net/latest/data_download_file_reference_2022.csv"
    forecast_file_name = "data_download_file_reference_2022.csv"

# Website with data
URL_report = "https://www.corona.ps/"

