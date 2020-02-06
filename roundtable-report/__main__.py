from datetime import date, time, datetime
from dateutil.relativedelta import relativedelta
import json
import os
import pkg_resources
import sys

from roundtable_report import functions as rrf


def print_log(msg):
    print(f"{datetime.now(): %Y-%m-%d %H:%M:%S}: {msg}")


def main(directory, mode):
    # Import report parameters
    paramfile = pkg_resources.resource_filename(
        'roundtable_report', 'params.json')
    with open(paramfile, 'r') as infile:
        param = json.load(infile)

    # Export directory creation
    mo = datetime.combine(date.today(), time.min) - \
        relativedelta(days=datetime.now().day - 1) - \
        relativedelta(months=1)
    directory = os.path.join(directory, 'data', mo.strftime('%Y-%m'))
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    print(f"Destination directory: {directory}")

    # Run SQL queries and export data to a local .csv file
    if mode in [0, 1]:
        queries = list(set([param[id]['datafile'] for id in param]))
        for query in queries:
            print_log(f"Starting {query} query...")
            table = rrf.query_data(query)
            print_log("Query complete!")
            print_log(f"Copying to local file '{query}.csv'...")
            rrf.export_data(table, directory, query)
            print_log(f"File created!")

    # Generate report images
    if mode in [0, 2]:
        for id, params in param.items():
            print(id)
            print_log("Starting table pivot...")
            pivot_tables = rrf.pivot_data(id, params, directory)
            print_log("Table pivot complete!")
            print_log("Starting visualization...")
            rrf.vis_data(params, directory, pivot_tables)
            print_log("Visualization complete!")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print(
            "An export path is required; please specify a directory to",
            "export data to as an argument to the module call...exiting")
        exit()
    directory = sys.argv[1]
    if not os.path.exists(directory):
        print(f"Path {directory} does not exist...exiting")
        exit()
    else:
        if len(sys.argv) > 2:
            if sys.argv[2].lower() == 'query':
                main(directory, 1)
            elif sys.argv[2].lower() == 'vis':
                main(directory, 2)
            else:
                print(f"Optional argument '{sys.argv[2]}' not recognized.\n"
                      "Usage: python -m roundtable_report {EXPORT_PATH} "
                      "['query', 'vis']")
                exit()
        else:
            main(directory, 0)
