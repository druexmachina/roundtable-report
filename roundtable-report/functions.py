from datetime import date, time, datetime
from dateutil.relativedelta import relativedelta
import json
import os
import pkg_resources
from textwrap import fill

import matplotlib.pyplot as plt
import matplotlib.font_manager as fnt
import pandas as pd
import seaborn as sns
import sqlalchemy as sa


def params_25M():
    """
    Creates a dictionary with start and end dates corresponding to the
    25-month range used in most queries

    :returns: a dict ready for use in a SQLAlchemy connection
    """
    prev_month_end = datetime.combine(date.today(), time.min) - \
        relativedelta(days=datetime.now().day - 1) - \
        relativedelta(days=1)
    prev_month_25m = prev_month_end + relativedelta(days=1) - \
        relativedelta(months=25)
    parameters = {'from_date': prev_month_25m.strftime('%Y%m%d'),
                  'to_date': prev_month_end.strftime('%Y%m%d')}

    return parameters


def query_data(query):
    """
    Opens a connection to the 'cpc2ds_admin' database, then executes each
    query.

    :returns: an iterator for the chunked data
    """
    queryfile = pkg_resources.resource_filename(
        'roundtable_report', 'queries.json')
    secretsfile = pkg_resources.resource_filename(
        'roundtable_report', 'secrets.json')
    with open(queryfile, 'r') as infile:
        queries = json.load(infile)
    with open(secretsfile, 'r') as infile:
        secrets = json.load(infile)
    engine = secrets['cpc2ds_admin']
    ora = sa.create_engine(sa.engine.url.URL(
        engine['dbapi'],
        username=engine['username'],
        password=engine['password'],
        host=engine['host'],
        port=engine['port'],
        query=engine['query']))
    params = params_25M()
    table = pd.read_sql(
        sa.text(queries[query]), ora, params=params, chunksize=(5 * 10**5))

    return table


def export_data(table, directory, query):
    """
    Takes the iterator returned from the query_data function and exports the
    merged chunks to a .csv file

    :param table: output of the query_data function (iterator)
    :param directory: destination directory to export data to
    :param query: name of query (and filename to export)
    """
    flag = 0
    for chunk in table:
        if flag == 0:
            chunk.to_csv(f"{directory}/{query}.csv")
            flag = 1
        else:
            with open(f"{directory}/{query}.csv", 'a') as file:
                chunk.to_csv(file, header=False)


def import_sys_avg():
    """
    Imports the system_averages table into a pandas dataframe and transforms
    the results for use with the main aggregation table

    :returns: pandas dataframe of transformed query results
    """
    # Perform query
    query = """
    select year, month, wk, sa, su, cawk, casa, casu
    from system_averages"""
    secretsfile = pkg_resources.resource_filename(
        'roundtable_report', 'secrets.json')
    with open(secretsfile, 'r') as infile:
        secrets = json.load(infile)
    engine = secrets['cpc2ds_admin']
    ora = sa.create_engine(sa.engine.url.URL(
        engine['dbapi'],
        username=engine['username'],
        password=engine['password'],
        host=engine['host'],
        port=engine['port'],
        query=engine['query']))
    system_averages = pd.read_sql(sa.text(query), ora)

    # Melt system average and calendar-adjusted system average columns
    sys_avg = pd.melt(system_averages,
                      id_vars=['year', 'month'],
                      value_vars=['wk', 'sa', 'su'],
                      var_name='daytype',
                      value_name='sa')
    cal_sys_avg = pd.melt(system_averages,
                          id_vars=['year', 'month'],
                          value_vars=['cawk', 'casa', 'casu'],
                          var_name='daytype',
                          value_name='casa')

    # Merge melted columns and format date and day_type columns to merge with
    # the main aggregation table
    cal_sys_avg.daytype = cal_sys_avg.daytype.apply(lambda x: x[2:])
    system_averages = pd.merge(
        sys_avg, cal_sys_avg, on=['year', 'month', 'daytype'])
    system_averages['day_type'] = system_averages.daytype.apply(
        lambda x: 'W' if x == 'wk' else x[-1].upper())
    system_averages['service_date'] = pd.to_datetime(
        (system_averages.year.astype(str) +
         system_averages.month.astype(str).str.zfill(2)),
        format="%Y%m")

    return system_averages


def import_r_grp():
    """
    Imports bus route information into a pandas dataframe and merges the
    relevant dict containing the new row names

    :returns: pandas dataframe of modified query results
    """
    datafile = pkg_resources.resource_filename(
        'roundtable_report', 'data.json')
    with open(datafile, 'r') as infile:
        data = json.load(infile)
    query = """
    select to_char(routenum) seg, rte_group from routes"""
    secretsfile = pkg_resources.resource_filename(
        'roundtable_report', 'secrets.json')
    with open(secretsfile, 'r') as infile:
        secrets = json.load(infile)
    engine = secrets['cpc2ds_admin']
    ora = sa.create_engine(sa.engine.url.URL(
        engine['dbapi'],
        username=engine['username'],
        password=engine['password'],
        host=engine['host'],
        port=engine['port'],
        query=engine['query']))
    r_grp = pd.read_sql(sa.text(query), ora)

    return pd.merge(
        r_grp,
        pd.DataFrame(data=data['route_groups']), on=['rte_group'])


def pivot_data(id, params, directory):
    """
    Creates a dictionary of pivot tables from the query results

    :param id: a hyphen-delimited string that translates to the aggregation
               performed
    :param params: dict of parameters used to manipulate the source data
    :param directory: destination directory to export data to
    :returns: a dict of pandas dataframes ready for visualization
    """
    # Pull extra column information
    datafile = pkg_resources.resource_filename(
        'roundtable_report', 'data.json')
    with open(datafile, 'r') as infile:
        data = json.load(infile)
    data['hour_bins'] = {int(k): v for k, v in data['hour_bins'].items()}
    group_by = ['type'] + params['split_col'] + \
        [params['idx_col'], 'service_date']
    if params['pivot_col'] != 'Month':
        group_by.insert(-2, params['pivot_col'])
    sys_avg = import_sys_avg()
    fm_grp = pd.DataFrame(data=data['fare_codes'])
    fm_grp_bin = pd.DataFrame(data=data['fare_code_bins'])
    s_fm_grp = pd.DataFrame(data=data['student_fare_codes'])
    v_fm_grp = pd.DataFrame(data=data['ventra_fare_codes'])
    r_grp = import_r_grp()

    # Import file piecewise and aggregate each chunk
    file = pd.read_csv(
        f"{directory}/{params['datafile']}.csv",
        chunksize=5 * 10**5)
    chunks = []
    for chunk in file:
        chunk['service_date'] = pd.to_datetime(
            chunk['service_date'], format='%Y-%m-%d')
        # System averages adjustment to rides column if needed
        if params['sa_adj'][0]:
            chunk = pd.merge(chunk, sys_avg, on=['service_date', 'day_type'])
            if params['sa_adj'][0] == 'sa':
                chunk.rides = chunk.rides / chunk.sa
            elif params['sa_adj'][0] == 'casa':
                chunk.rides = chunk.rides / chunk.sa * chunk.casa
        chunk.rides = chunk.rides / params['sa_adj'][1]
        # Merge extra column information if needed
        if 'fm_grp' in group_by:
            chunk = pd.merge(chunk, fm_grp, on=['finance_code'])
        if 'fm_grp_bin' in group_by:
            chunk = pd.merge(
                chunk, fm_grp_bin, on=['finance_code'], how='left')
            chunk.fm_grp_bin.fillna('Other Rides', inplace=True)
        if 's_fm_grp' in group_by:
            chunk = pd.merge(chunk, s_fm_grp, on=['media'])
        if 'v_fm_grp' in group_by:
            chunk = pd.merge(chunk, v_fm_grp, on=['fare_prod_name'])
        if 'seg' in group_by:
            chunk['seg'] = chunk['seg'].astype(str)
            chunk = pd.merge(chunk, r_grp, on=['seg'], how='left')
            chunk['seg'] = chunk.apply(
                lambda row: row.r_grp if row.type == 'bus' else row.seg,
                axis=1)
        if 'time_bin' in group_by:
            chunk['time_bin'] = chunk['hr'].map(data['hour_bins'])
        # Aggregate chunk
        if 'sys' in group_by:
            group_by_temp = group_by.copy()
            del group_by_temp[group_by_temp.index('sys')]
            chunks.append(chunk.groupby(group_by_temp).agg({'rides': 'sum'}))
        else:
            chunks.append(chunk.groupby(group_by).agg({'rides': 'sum'}))
    # Concatenate all chunks and perform a final aggregation to reconcile any
    # duplicate service_dates induced by chunking
    if 'sys' in group_by:
        # Add rows with a sum aggregation over the original aggregation
        # columns sans 'type' (need rides values for bus and rail combined)
        del group_by[group_by.index('sys')]
        table = pd.concat([chunk for chunk in chunks]) \
            .groupby(group_by) \
            .agg({'rides': 'sum'}) \
            .reset_index()
        table_total = table \
            .groupby(group_by[1:]) \
            .agg({'rides': 'sum'}) \
            .reset_index()
        table_total['type'] = 'system'
        table = pd.concat([table, table_total], sort=False)
    else:
        table = pd.concat([chunk for chunk in chunks]) \
            .groupby(group_by) \
            .agg({'rides': 'sum'}) \
            .reset_index()
    prev_month_start = datetime.combine(date.today(), time.min) - \
        relativedelta(days=datetime.now().day - 1) - \
        relativedelta(months=1)
    prev_13M_start = prev_month_start - relativedelta(years=1)
    # If not pivoting by month, filter to the two months of interest
    if params['pivot_col'] != 'Month':
        table = table[
            (table.service_date == pd.to_datetime(prev_month_start)) |
            (table.service_date == pd.to_datetime(prev_13M_start))]

    # Response calculations and final column cleanup in preparation for pivot
    vals = list(params['vis_title'].keys())
    if 'pct_of_total' in vals:
        # Filter to the 13 months of interest and compute the share percentage
        # of each media type
        table = table[
            (table.service_date <= pd.to_datetime(prev_month_start)) &
            (table.service_date >= pd.to_datetime(prev_13M_start))]
        table = table.set_index(['type', 'service_date', params['idx_col']]) \
            .groupby(level=[0, 1], as_index=False) \
            .apply(lambda row: row['rides'] / row['rides'].sum() * 100) \
            .reset_index(level=['type', 'service_date', params['idx_col']]) \
            .reset_index(drop=True) \
            .rename(columns={'rides': 'pct_of_total'})
        totals = table.groupby(['type', 'service_date']) \
            .agg({'pct_of_total': 'sum'}) \
            .reset_index()
        totals[params['idx_col']] = 'Total'
        table = pd.concat([table, totals], sort=False) \
            .reset_index(drop=True)
    else:
        # Generate a 'pre' column using a 12-month lookback and calculate
        # raw and percent differences
        table = table.set_index('service_date')
        totals = table \
            .groupby(group_by[:-2]) \
            .resample('1M')['rides'] \
            .sum() \
            .fillna(0) \
            .reset_index()
        totals[params['idx_col']] = 'Total'
        table = table \
            .groupby(group_by[:-1]) \
            .resample('1M')['rides'] \
            .sum() \
            .fillna(0) \
            .reset_index()
        table = pd.concat([table, totals], sort=False) \
            .reset_index(drop=True)
        table['pre'] = table \
            .sort_values(group_by) \
            .groupby(group_by[:-1])['rides'] \
            .shift(12)
        table.dropna(inplace=True)
        table['pct_diff'] = (table.rides / table.pre - 1) * 100
        table['diff'] = table.rides - table.pre
        # table = pd.concat([table, totals], sort=False)
    if params['pivot_col'] == 'Month':
        table['Month'] = table.service_date.dt.strftime('%Y-%m')
    table['key'] = table.apply(
        lambda row: ' - '.join(
            [row[col] for col in params['split_col'] if col != 'sys']), axis=1)

    # Rename index column, perform categorical column setup as needed
    table.rename(
        columns={params['idx_col']: params['cat_col'][0]}, inplace=True)
    table.dropna(inplace=True)

    # Export formatted data to .csv
    table.reset_index(drop=True, inplace=True)
    path = os.path.join(directory, params['outfile'])
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    print(path)
    table.to_csv(f"{path}/raw_data.csv", index=False)

    # Pivot
    id_label = '|'.join(id.split('-')[2:])
    modes = table.type.unique()
    pivot_tables = {
        f"{mode}|{split_key + '|' if split_key else ''}{id_label}|{val}":
            table[(table.type == mode) & (table['key'] == split_key)].pivot(
                index=params['cat_col'][0],
                columns=params['pivot_col'],
                values=val)
        for mode in modes
        for split_key in table['key'].unique()
        for val in vals}
    # Reorder rows if needed
    if params['cat_col'][1]:
        for label, pivot_table in pivot_tables.items():
            pivot_table_temp = pivot_table.reset_index()
            if len(params['cat_col'][1].items()) == 1:
                pivot_table_temp[params['cat_col'][0]] = pd.Categorical(
                    pivot_table_temp[params['cat_col'][0]],
                    list(params['cat_col'][1].values())[0],
                    ordered=True)
            else:
                pivot_table_temp[params['cat_col'][0]] = pd.Categorical(
                    pivot_table_temp[params['cat_col'][0]],
                    params['cat_col'][1][label.split('|')[0]],
                    ordered=True)
            pivot_tables[label] = pivot_table_temp \
                .set_index(params['cat_col'][0]) \
                .sort_values(params['cat_col'][0])
    # Reorder columns if needed
    if params['reorder_col']:
        for label, pivot_table in pivot_tables.items():
            cols = pivot_table.columns
            newCols = params['reorder_col'][label.split('|')[0]]
            toDel = []
            for col in newCols:
                if col not in cols:
                    toDel.append(col)
            for col in toDel:
                newCols.remove(col)
            pivot_table = pivot_table[newCols]
            if params['pivot_col'] == 'seg':
                pivot_table.columns = [
                    fill(col, 9) for col in pivot_table.columns]
            pivot_tables[label] = pivot_table
    # Generate focus tables if needed
    if params['focus_tbl']:
        ftables = {}
        for label, pivot_table in pivot_tables.items():
            ftable = pivot_table.iloc[:, -5:].copy()
            ftable[f"{ftable.columns[-1]}\nChange"] = ftable.iloc[:, -1] - \
                ftable.iloc[:, -5:].mean(axis=1)
            ftables[f"{label}{params['focus_tbl']}"] = ftable
        for label, pivot_table in ftables.items():
            pivot_tables[label] = pivot_table

    return pivot_tables


def graph_mod(params, label, ax1, ax2):
    """
    Performs non-universal visualization modifications

    :param params: dict of parameters used to manipulate the source data
    :param label: a hyphen-delimited string that defines the attributes of
                  each pivot table
    :param ax1: a matplotlib.axes.Axes object (main table)
    :param ax2: a matplotlib.axes.Axes object (aggregated 'Total' row)
    """
    if params['idx_col'] == 'time_bin':
        # Sets yticklabels and cell texts to bold for AM Peak and PM Peak
        start = end = -1
        for ticklabel in ax1.get_yticklabels():
            if ticklabel.get_text().find('AM Peak') != -1:
                start = ax1.get_yticklabels().index(ticklabel)
                ticklabel.set_fontproperties(
                    fnt.FontProperties(weight='bold'))
            if ticklabel.get_text().find('PM Peak') != -1:
                end = ax1.get_yticklabels().index(ticklabel)
                ticklabel.set_fontproperties(
                    fnt.FontProperties(weight='bold'))
        xLen = len(ax1.get_xticklabels())
        if start > -1:
            for i in range(xLen * start, xLen * (start + 1)):
                ax1.texts[i].set_weight('bold')
        if end > -1:
            for i in range(xLen * end, xLen * (end + 1)):
                ax1.texts[i].set_weight('bold')
    elif params['idx_col'] == 'fm_grp':
        # Draws cell borders that visually group different media
        if label[label.find("-")] == 'rail':
            item_idx = [3, 6, 12, 13, 15, 16, 19, 20, 25]
        else:
            item_idx = [1, 4, 6, 12, 13, 16, 17, 18, 21, 22, 28]
        ax1.hlines([x - 0.1 for x in item_idx], *ax1.get_xlim(), linewidth=1.0)
    elif params['idx_col'] == 'v_fm_grp':
        # Draws cell borders that visually group different media
        item_idx = [2, 4, 6, 8, 10, 11]
        ax1.hlines([x - 0.1 for x in item_idx], *ax1.get_xlim(), linewidth=1.0)


def vis_data(params, directory, pivot_tables):
    """
    Creates heatmaps of input pandas dataframes using Seaborn/Matplotlib and
    exports them as image files

    :param params: dict of parameters used to manipulate the source data
    :param directory: destination directory to export data to
    :param pivot_tables: dict of pandas dataframes to visualize
    """
    colors = [x for x in reversed(sns.color_palette("coolwarm", 11))]
    # Parameters for core data
    heatmap1 = {
        'cmap': colors,
        'robust': True,
        'annot': True,
        'fmt': '.1f',
        'linecolor': 'black',
        'cbar': False
    }
    # Parameters for summary row
    heatmap2 = {
        'cmap': [(1, 1, 1)],
        'robust': True,
        'annot': True,
        'fmt': '.1f',
        'annot_kws': {'weight': 'bold'},
        'linecolor': "black",
        'cbar': False
    }
    for label, table in pivot_tables.items():
        if len(table.index) > 0:
            with plt.style.context("seaborn-white"):
                # Title generation
                if len(label.split('|')) == 5:
                    mode, split, idx, col, val = label.split('|')
                    title_split = split + ' - '
                else:
                    mode, idx, col, val = label.split('|')
                    title_split = ''
                if params['focus_tbl'] and val not in params['vis_title']:
                    title = f"{mode.title()} - " + \
                        f"{title_split}{params['vis_title'][val[:-1]]}" + \
                        f" (-{val[-1]}M)"
                else:
                    title = f"{mode.title()} - " + \
                        f"{title_split}{params['vis_title'][val]}"
                # Overall figure size and relative size of the two subplots
                # scaled to the number of rows in the table
                if params['focus_tbl'] and val not in params['vis_title']:
                    fig, (ax1, ax2) = plt.subplots(
                        figsize=(params['focus_tbl'], len(table.index) / 3.5),
                        nrows=2,
                        gridspec_kw={'height_ratios': [len(table.index), 1]})
                else:
                    fig, (ax1, ax2) = plt.subplots(
                        figsize=(12, len(table.index) / 3.5),
                        nrows=2,
                        gridspec_kw={'height_ratios': [len(table.index), 1]})
                fig.subplots_adjust(hspace=(0.2 / len(table.index)))
                sns.heatmap(table[:-1], **heatmap1, ax=ax1)
                sns.heatmap(table[-1:], **heatmap2, ax=ax2)
                # Append '%' to cell values if response is a percentage
                if label.find("pct") > -1:
                    for t in ax1.texts:
                        t.set_text(t.get_text() + " %")
                    for t in ax2.texts:
                        t.set_text(t.get_text() + " %")
                # Move x-axis labels to top of plot
                ax1.xaxis.tick_top()
                ax1.xaxis.set_label_position('top')
                ax2.xaxis.set_visible(False)
                ax2.yaxis.set_label_text('')
                # Align the 'Total' label vertically with the rest of the row
                for ticklabel in ax2.get_yticklabels():
                    ticklabel.set_fontproperties(
                        fnt.FontProperties(weight='bold'))
                    ticklabel.set_verticalalignment("center")
                # De-rotate tick labels
                ax1.tick_params(
                    axis='both',
                    which='both',
                    left=False,
                    top=False,
                    labelrotation=0
                )
                ax2.tick_params(
                    axis='both',
                    which='both',
                    left=False,
                    bottom=False,
                    labelbottom=False,
                    labelrotation=0
                )
                # Cosmetic changes
                ax1.set_title(title, fontsize=12, fontweight='bold')
                ax1.set_xlabel(
                    table.columns.name, fontweight="bold", labelpad=8)
                ax1.set_ylabel(
                    table.index.name, fontweight="bold", labelpad=10)
                graph_mod(params, label, ax1, ax2)
                # Image export
                split_col = label.split('|')[-len(label.split('|')):-3]
                split_col.append(label.split('|')[-1])
                split_col = '-'.join([
                    x.replace('-', '')
                    .replace(' ', '')
                    .replace('/', '') for x in split_col])
                path = os.path.join(directory, params['outfile'])
                if not os.path.exists(path):
                    os.makedirs(path, exist_ok=True)
                plt.savefig(
                    f"{path}/{split_col}",
                    bbox_inches='tight')
                fig.clf()
                plt.close()
