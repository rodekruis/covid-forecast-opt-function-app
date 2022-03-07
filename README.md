# covid-forecast-palestine-function-app

COVID-19 emergency support to Palestine Red Crescent. An automated function in Python to forecast cases per governorate of OPT.

## COVID-19 forecast

**Input:**

- Latest COVID-19 forecast from [MRC Centre for Global Infectious Disease Analysis, Imperial College London](https://mrc-ide.github.io/global-lmic-reports/) or from [Institute for Health Metrics and Evaluation, University of Washington School of Medicine](https://www.healthdata.org/covid/data-downloads)
- Latest COVID-19 (daily) indicators per state reported by Ministry of Health, summarized at [corona.ps](https://corona.ps/).

**Output:**

- A csv contained new forecast cases of every governorate in the upcoming week. Forecast of one week before is also included for reference.
- A csv contained new forecast ICU incidence of the state in the upcoming week. Forecast of one week before is also included for reference.
- Each figure for each governorate's new forecast cases.
- A figures for new forecast ICU bed needs.
- Outputs is stored at the MS Teams OPT channel.

**Disclaimer:**

- The forecast estimate of Centre for Global Infectious Disease Analysis or Institute for Health Metrics and Evaluation accounting for undiagnosed cases.
- Latest measurements from local government does not take into account the forecast estimate.

## Azure function app 

The Azure function is set up from the [Azure function template for Python](https://github.com/jmargutt/azure-python-function-app).
Requirements and basic setup can be found in the link above.
