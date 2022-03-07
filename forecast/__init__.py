from forecast.settings import *
import azure.functions as func
import logging
import datetime
from forecast.utils import CovidForecast

today = datetime.date.today()
last_week = today - datetime.timedelta(days=7)
next_week = today + datetime.timedelta(days=7)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due")

    fc = CovidForecast()
    fc.authenticate_container()

    try:
        df_report = fc.get_report_data()
    except Exception as e:
        logging.error("Error:")
        logging.error(e)

    if forecast_source == "MRC":
        try:
            df_new_cases, df_icu_inci = fc.get_MRC_data()
        except Exception as e:
            logging.error("Error:")
            logging.error(e)
    elif forecast_source == "IHME":
        try:
            df_new_cases, df_icu_inci = fc.get_IHME_data()
        except Exception as e:
            logging.error("Error:")
            logging.error(e)

    try:
        df_report_week = fc.forecast_new_cases(df_report, df_new_cases)
    except Exception as e:
        logging.error("Error:")
        logging.error(e)

    try:
        fc.plot_new_cases(df_report_week)
    except Exception as e:
        logging.error("Error:")
        logging.error(e)
    try:
        fc.plot_icu(df_icu_inci)
    except Exception as e:
        logging.error("Error:")
        logging.error(e)

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
