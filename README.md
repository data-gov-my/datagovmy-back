# datagovmy-back

The backend API for Datagovmy that serves data available via [`datagovmy/datagovmy-meta`](https://github.com/data-gov-my/datagovmy-meta) to the frontend at [`datagovmy/datagovmy-front`](https://github.com/data-gov-my/datagovmy-front).
- Django app name: `data_gov_my`
- Database: `postgresql`
- Cache: `redis`

## Table of Contents

- [Virtual environment setup](#setup-virtual-environment)
- [Database setup](#setup-db)
- [Run Development Server](#run-development-server)
- [API Endpoints](#api-endpoints)
- [Fetching new data](#fetching-new-data)
- [Private tokens required](#private-tokens-required)
- [How it works](#how-it-works)
- [Meta Jsons version 1.0](#meta-jsons)
- [Catalog Jsons version 1.0](#catalog-jsons)
- [Chart Builders version 1.0](#chart-builders)
- [Contributing](#contributing)


## Setup virtual environment

```bash
<your python exe> -m venv env

# activate and install dependencies
source env/bin/activate
pip install -r requirements.txt
pre-commit install
```

## Setup DB

1. Setup a DB server (PostgresSQL DB recommended) and populate your DB instance settings in the `.env` file.
2. Run migrations to setup all tables: `python manage.py migrate`
3. Fetch data from `datagovmy/datagovmy-meta` repo and populate or update the DB: `python manage.py loader UPDATE {category}`
4. To rebuild the DB from scratch: `python manage.py loader REBUILD`
5. List of valid categories are, `DATA_CATALOG`, `DASHBOARDS`, `I18N`, `FORMS`, `EXPLORERS`, `PUBLICATION`, `PUBLICATION_DOCS`,`PUBLICATION_UPCOMING`

## Run Development Server
`python manage.py runserver`

## API Endpoints
API will be running at `http://127.0.0.1:8000/`

Despite Datagovmy having multiple dashboards and data variables, there is only 1 endpoint, dedicated to each type, to serve all variables and dashboards.

- `dashboard/`
    - Every GET request to this endpoint, will have to be associated with a query parameter, `'dashboard'`.
    - This `dashboard` parameter indicates which dashboard API should be returned.
    - There can be multiple parameters as long as they are defined in the META Json and individual charts' parameter - this enables the `handle_request` method to execute them accordingly.
- `data-variable/`
    - Every get request to this endpoint, will have to be associated with a query parameter, `'id'`.
    - The `'id'` represents the unique identifier for each variable, within the data catalog.
    - The response from this endpoint, are information, such as chart data, variable explanations, download links, and more.
- `chart/`
    - Every get request to this endpoint, will have to be associated with 2 query parameters, `'dashboard'` and `'chart'`.
    - There can be multiple parameters as long as they are defined in the META Json and individual charts' parameter.
    - This endpoint allows for the ability to request data for individual charts from within a dashboard.
- `update/`
    - Every POST request to this endpoint, will trigger an update, to perform a selective update, based on the files changed in the git repository.


## Fetching new data
As of now, Datagovmy uses GitHub Actions to trigger a rebuild (when there is new code) or update of data (when there is new data). Alternatively, with your preferred deployment setup, there are 2 ways to update the data.
- Using POST method
  - A post request can be sent to the endpoint `https://<your url here>/update/` , which would by default trigger an update, and populate the DB with the new data.
- Using command line
  * Alternatively, if your desired cron / task scheduler runs locally, you could use the command line to trigger an update and populate the DB with the new data.
    > python manage.py loader ***category*** ***operation*** ***files***

  * `category` - Possible values are `DATA_CATALOG`, `DASHBOARDS`, `I18N`, `FORMS`, `EXPLORERS`, `PUBLICATION`, `PUBLICATION_DOCS`, or `PUBLICATION_UPCOMING`, depending on whichever you choose to update.
  * `operation` - Possible values are either `UPDATE` or `REBUILD`. `UPDATE` will update the datasets, by inserting new rows, or updating existing rows by their ID. Whereas `REBUILD` would delete all rows from the database, and populate from an empty database.
  * `files` - When using the `UPDATE` operation, will update specific files of your choice. The values must be a string, concatenated with a `,` dividing each dataset. E.g : `'meta_1,meta_2,meta_3'`. There is no need to add the additional `.json` suffix. This parameter is optional, and if left empty, will update every existing dataset of the chosen category.

## Private tokens required:
- Telegram ChatID and token - The Datagovmy backend loader updates status messages to a private Telegram channel for the development team to monitor data pipelines daily. Feel free to comment out any calls to the `trigger` module for the loader to work without Telegram updates.

## How it works
Before explaining the logic of Datagovmy's backend operations, here is a brief description of the thinking behind the architecture choices:
1. Datagovmy will only keep expanding as more dashboards are added, so we wanted to ensure that the number of endpoints did not balloon along with it.
2. Datagovmy will likely include more chart types over time, which may vary in what they require in terms of API structure and business logic.

Therefore, to minimise complexity, it was vital that we used an architecture that could keep the number of endpoints minimal, while simultaneously ensuring that chart data could be served in as versatile a manner as possible. Here are the key ingredients we used to do this:

## META Jsons
    Meta Json version 2.0
A META Json contains all information required to build a dashboard. These files can be found within the `dashboard` folder of the `datagovmy-meta` repo. Each one is responsible for either a dashboard, or a dedicated chart.
- Each META Json contains information regarding the required and optional parameters for the API, and the list of charts needed.
- Each Meta Json contains the route mapped to Front-end for revalidation.
- Each chart key contains multiple sub-keys representing chart names.
- Each sub-key contains all information required to fully specify a chart, such as parameters, chart types, API types, and variables.
- The variables and `chart_source` keys contain data which is fed to the chart builders, which serves the final data.

## Catalog Jsons
    Catalog Json version 1.0
A Catalog Json contains all information required to build a data variable, or a set of data-variables. These files can be found within the `catalog` forlder of the `aksara-data` repo. Each one is responsible for 1 or more data variables.
- Each Catalog Json contains information regarding the chart type of the variable, the explanations of the variables, as well as the file source of the data.
- The Catalog JSON can be divided into 2 essential parts :
    - File
        - The `file` key contains the description of the dataset, as well as the source of the data set.
        - Within the `file` key, there is a key named `variables`. This key, is an array of objects, containing the id, name, title, and description of each data-variable within the dataset.
        - The following are the conventions of the variable's id :
            1. `-1` : Indicates a variable should be used in the explanation, but has no dedicated chart / page for itself.
            2. `0` : Indicates the variables excluded for a data-variable of the table chart.
            3. `>= 1` : Indicates the variables included within the dataset, AND has a dedicated chart / page for itself.
    - Catalog Data
        - the `catalog_data` key, contains an array of objects, which represents the meta-data, chart variables and chart filters, required to build the respective data-variable page. These id in these variables will also correspond to the id within the `variables` key, nested in the `file` key.

## Chart Builders
    Chart Builders version 1.0
Critical design principle: All charts of the same type should be built using only 1 method. For instance, all time series charts on the site should (as far as possible) be handled by the same code unit.
- Within the `utils` folder in the project, the `chart_builder` file contains several methods for various charts, such as heatmaps, bar charts, choropleths, etc.
- As long as sufficient information is supplied (by the META Json), the chart builder can dynamically format a chart's api, thus requiring minimal to no 'hard-coding'. This increases versatility.
- Variable structures and data-types, of each chart type can be viewed in the `variable_structures.py` file, within the `utils` folder.

## Contributing
Pull requests are welcome. For any pull request, please make sure to either create a new branch, or submit a pull request to the `staging` branch. Please make sure to briefly describe the changes introduced in the pull request.
