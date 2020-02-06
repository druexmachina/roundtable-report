data.json

Replaces the sprawling case when clauses in the original SQL queries; each entry in the parent dictionary is converted into a pandas dataframe that is joined to the ridership data by the pivot_data function in functions.py.

    With the exception of the 'hour_bins' entry, each entry is a dictionary containing two entries:
        {SQL column name to change}: {list of values to replace}
        {Column name to create}: {list of replacement values}

    Example:

      "student_fare_codes": {
          "media": [166, 167, 169, 505, 766, 770],
          "s_fm_grp": ["Student 2 Ride", "Student Card", "Student Card", "Student Cash", "Student 2 Ride", "Student Card"]}

    The created column name is referenced in params.json.
    The name assigned to the overall entry ('student_fare_codes' in the example above) is referenced in the pivot_data function in functions.py.

params.json

Defines the structure of the package in the form of a dictionary and allows generic SQL queries to generate the raw data

    Each value is a dictionary of parameters with fixed names referenced by functions.py and main.py
        datafile: The name of the raw data file to import for this report, must be one of the keys in queries.json. Currently supported options include:
            fare-media-ridership: database1.[table1, table2]
            ventra-ridership: database2.[table1, table2]
            daily-ridership: database3.[table1, table2]
        sa_adj: A list with two entries; the first must be one of the strings in ['casa', 'sa', ''] corresponding to the adjustment applied using the system averages table (see key below), the second must be an integer and is used to divide the 'rides' column by. At this point, all existing reports use either 1000 or 1.
            'casa': rides / sa casa, rides / su casu, rides / wk * cawk
            'sa': rides / sa, rides / su, rides / wk
            '': rides (no adjustment applied)
        split_col: A list (can be empty); the list of columns used to partition the data into different pivot tables. Partitioning is done by mode (bus, rail) by default, and any columns listed here will add a degree of freedom to the partitioning.
        idx_col: A string; the column name to use for the y-axis of the pivot table.
        cat_col: A list; applies a custom ordering to the values in idx_col (and the rows of the pivot table by extension). The first entry is a string allowing the column to be renamed if desired. The second entry is a dictionary (can be empty). If the desired order is the same across bus and rail, then the string used for the dictionary key doesn't matter ('all' is used in existing reports). If a different order is needed for each mode, then 'bus' and 'rail' are required to be the keys. Each associated value is a list of the values in the column in the desired order.
        reorder_col: A dictionary (can be empty); applies a custom erdering to the pivot table columns. The same conditions apply as for the dictionary in cat_col.
        pivot_col: A string; the column name to use for the x-axis of the pivot table. If monthly YOY data is desired, the value should be 'Month'; this column is generated automatically during the munging process.
        focus_tbl: An integer [0-9]; the amount of columns to pull out into a smaller table and add a 'change' column to. Intended for use with reports with a pivot_col of 'Month' to analyze shorter time ranges.
        vis_title: A dictionary; the keys are the responses to generate pivot tables for, the values are the corresponding titles for these plots. The allowable keys are: 'diff' (YOY difference), 'pct_diff' (YOY percent difference), and 'pct_of_total' (idx_col percentage of the total in each pivot_col).
        outfile: A string; the name of the sub-folder to create that will house the images from the report.

    Placing 'sys' as one of the entries in split_col enables a third mode ('system') to be generated that adds the corresponding bus and rail values together.

    split_col, idx_col, pivot_col options:
        hr
        time_bin: bins hr
            Early Morning (3am to 6am)
            AM Peak (6am to 9am)
            Midday (9am to 3pm)
            PM Peak (3pm to 6pm)
            Evening (6pm to 10pm)
            Late Night (10pm to 3am)
        Month
        day_type
        fm_grp
        fm_grp_bin
        s_fm_grp
        v_fm_grp
        seg
            bus
            rail
        split_col is the only parameter that can be empty

    Each key is a hyphen-delimeted string: 'rides'-{one entry in split_col, 'sys' if present}-{idx_col}-{pivot_col}.
        If 'Month' is the pivot_col value, the pivot_col value in the key should be 'mo'.
        If 'sys' is a value in split_col and split_col has more than one value, then 'sys' should be the value placed in the key.

    Examples:

      "rides-day_type-hr-mo": {
          "datafile": "fare-media-ridership",
          "sa_adj": ["casa", 1000],
          "split_col": ["day_type"],
          "idx_col": "hr",
          "cat_col": ["Hour", {}],
          "reorder_col": "",
          "pivot_col": "Month",
          "focus_tbl": 0,
          "vis_title": {"diff": "YOY Value Change by Month in (000s)",
          "pct_diff": "YOY Percent Change by Month"},
          "outfile": "Mode_Hour-by-Day-Type_YOY"}

      "rides-sys-s_fm_grp-mo": {
          "datafile": "fare-media-ridership",
          "sa_adj": ["casa", 0],
          "split_col": ["sys"],
          "idx_col": "s_fm_grp",
          "cat_col": ["Faremedia Group", {
              "all": ["Student Card", "Student 2 Ride", "Student Cash", "Total"]}],
          "reorder_col": {},
          "pivot_col": "Month",
          "focus_tbl": 0,
          "vis_title": {
              "diff": "YOY Value Change by Month",
              "pct_diff": "YOY Percent Change by Month"},
          "outfile": "Student-Trends"}}

queries.json

Contains the raw SQL queries in the form of a dictionary; the keys are arbitrary and referenced in params.json, the values are the raw query text (typically contains bus and rail data combined with union all). The column names are referenced in functions.py, params.json, and data.json.

Any new entries to this file must have:

    'type': A string column with entries of either 'bus' or 'rail' corresponding to the source table
    'service_date': A datetime column with format '%Y-%m-%d'
    'rides': A numeric column corresponding to the desired response
    Additional columns must fit one of these descriptions:
        'hr': An integer column
        'day_type': A string column
        'seg': A string column
        'media': An integer column
        'finance_code': An integer column
        'fare_prod_name': A string column

secrets.json

Houses the connection parameters used by the SQLAlchemy package to create a database connection.
