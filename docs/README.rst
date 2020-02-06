Generates images of pivot table heatmaps for use in the monthly ridership roundtable report. This package is tailored to analyze ridership data from database1, but can be extended.
Files

    main.py: Contains the function calls that generate the data
    functions.py: Workhorse function definitions
    data.json: Supplemental data used by functions.py (e.g. time bin definitions, renaming)
    params.json: A set of parameters defined for each report; used by functions.py
    queries.json: Raw SQL queries
    secrets.json: Database connection parameters

Usage

cd planning-flask-server/roundtable_report
python setup.py install
python -m roundtable_report {EXPORT_PATH} ['query', 'vis']

    Raw SQL export: {EXPORT_PATH}/data/{previous month (yyyy-mm)}/{query name}.csv
    Images: {EXPORT_PATH}/data/{previous month (yyyy-mm)}/{report name}/{image name}.png
    'query' option: Only executes the SQL queries and exports the data files
    'vis' option: Only attempts to use the datafiles at {EXPORT_PATH}/data/ to generate the images

