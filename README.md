# covid-forecast-palestine-function-app

COVID-19 emergency support to Palestine Red Crescent. An automated function in Python to forecast cases per governorate of OPT.

## COVID-19 forecast

**Input:**

- Latest COVID-19 forecast from [MRC Centre for Global Infectious Disease Analysis, Imperial College London](https://mrc-ide.github.io/global-lmic-reports/)

- Latest COVID-19 (daily) report from [corona.ps](https://corona.ps/). Gaza strip is also broken down in to 5 governorates based on data from the NS.

**Output:**

- A csv contained new forecast cases of every governorate in the upcoming week. Forecast of one week before is also included for reference.
- A csv contained new forecast ICU incidence of the state in the upcoming week. Forecast of one week before is also included for reference.
- Each figure for each governorate's new forecast cases.
- A figures for new forecase ICU incidence.
- Outputs is stored at the MS Teams OPT channel.

**Disclaimer:**

- The forecast estimate of Centre for Global Infectious Disease Analysis accounting for undiagnosed cases.

- The forecast estimate of sometimes show 0 due to missing data from the site corona.ps.

## Azure function app 

The Azure function is set up from the [Azure function template for Python](https://github.com/jmargutt/azure-python-function-app).
Requirements and basic setup can be found in the link above.
