import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from calendar import monthrange
from kbr_helper import p_logs, u_logs
import requests
import json

# Count of dataframe entries to see if missing data, should match # of days####


def monthly(loca):
    if np.isnan(loca['Usage_GB'].mean()):
        return 0
    else:
        return loca['Usage_GB'].mean()


def header1(start):
    for col_num, value in enumerate(header.columns.values):
        worksheet.write(start, col_num+1, value, bold)


def data(frame, start):
    frame.to_excel(writer, 'Sheet', startcol=1, startrow=start + 1, header=False, index=False, float_format='%.2f')


def df_location(ingest, physical, site):
    global avg_usage_cell_start
    row_start = worksheet.dim_rowmax + 4

    # Create DataFrames for location's usage
    location = pd.DataFrame()
    location = location.fillna(0)
    # location = location[~(location == 0).any(axis=1)]
    location = location.assign(Date=date, Usage_GB=df1[ingest],
                               Reduction=(df1[ingest] - df1[physical]) * 100 / df1[ingest],
                               Physical=df1[physical])
    location = location.assign(Daily_Cost=location['Usage_GB'] * daily_cost)
    location = location.sort_index(ascending=True)

    # Add detailed report
    worksheet.write('A' + str(row_start), locations[site], bold)
    header1(row_start)
    data(location, row_start)

    # Add summary cost / avg usage
    worksheet.write('B' + str(avg_usage_cell_start), monthly(location))
    worksheet.write('C' + str(avg_usage_cell_start), monthly(location) * monthly_cost, currency)
    # worksheet.write('D' + str(avg_usage_cell_start), sum(location['Usage_GB']))
    avg_usage_cell_start += 1
    # worksheet.write('D' + row_num, dframe.count()[0])


# get dates
current_month_str = datetime.utcnow().strftime('%m-%Y')
today = datetime.today()
first = today.replace(day=1)
lastMonth = first - timedelta(days=1)
today_month_number = today.month
num_days_report_month = monthrange(lastMonth.year, lastMonth.month)[1]
monthly_cost = 0.0083
daily_cost = monthly_cost / num_days_report_month

passparams = {'hide_filterinfo': '1', 'duration': '30d', 'filter.0.class_type/guid': '343db69c690d761d82566e5ecc824eb9'}
r = requests.get('https://sciencelogicurl.com/api/device', auth=(u_logs(), p_logs()),
                 verify=False, params=passparams)
status = r.status_code
devices = json.loads(r.content.decode('utf-8'))
# Remove Birmingham from billing list
for x in range(len(devices)):
    try:
        if devices[x]['description'] == 'BHM02BKUPP00001':
            devices.pop(x)
    except IndexError:
        pass

perf_id = (6426, 6427, 6428)

# Not needed, but more work to take out than to leave
data_columns = ['LON99BKUPP00001 (Ingested)', 'LON99BKUPP00001 (Physical)', 'LON99BKUPP00001 (Total Capacity)',
                'HOU99BKUPP00001 (Ingested)', 'HOU99BKUPP00001 (Physical)', 'HOU99BKUPP00001 (Total Capacity)',
                'AUS02BKUPP00001 (Ingested)', 'AUS02BKUPP00001 (Physical)', 'AUS02BKUPP00001 (Total Capacity)',
                'HSV01BKUPP00001 (Ingested)', 'HSV01BKUPP00001 (Physical)', 'HSV01BKUPP00001 (Total Capacity)',
                'FAB01BKUPP00001 (Ingested)', 'FAB01BKUPP00001 (Physical)', 'FAB01BKUPP00001 (Total Capacity)']

locations = {'LON99BKUPP00001': 'London',
             'HOU99BKUPP00001': 'Houston',
             'AUS02BKUPP00001': 'Austin',
             'HSV01BKUPP00001': 'Huntsville',
             'FAB01BKUPP00001': 'Farnborough',
             'BWI03BKUPP0001': 'Lexington',
             'SIN01BKUPP00001': 'Singapore',
             'SIN03BKUPP00001': 'Singapore-Backup',
             'PER01BKUPP00002': 'Perth',
             'PER05BKUPP00001': 'Perth-Backup'}

header = pd.DataFrame(columns=['Date', 'Usage GB', 'Reduction', 'Physical', 'Daily Cost'])

# Change report_month_number to month to retreive >1 months.
# report_month_number = 12
# get device list
df = pd.DataFrame()
for device in devices:
    device_id = device['URI']
    print(device)
    for perf in perf_id:
        url_usage = 'https://sciencelogicurl.com' + str(device_id) + '/performance_data/' \
                    + str(perf) + '/normalized_daily?insert_nulls=1&beginstamp=' + str(lastMonth.strftime('%m')) \
                    + '%2F1%2F' + str(lastMonth.strftime('%y')) + '&endstamp=' + str(lastMonth.strftime('%m')) \
                    + '%2F' + str(num_days_report_month) + '%2F' + str(lastMonth.strftime('%y'))

        passparams2 = {'hide_options': '1'}
        r_usage = requests.get(url_usage, auth=(u_logs(), p_logs()),
                         verify=False, params=passparams2)
        status = r.status_code
        usage = json.loads(r_usage.content.decode('utf-8'))
        if '0' in usage['data'].keys():
            items = usage['data']['0']['avg']
            df = df.append(items, ignore_index=True)
        else:
            items = usage['data']
            df = df.append(items, ignore_index=True)

df1 = df.T
df1 = df1.fillna(0)
df1 = df1.astype(float)
df1 = df1 / 1000 / 1000 / 1000

# Rename columns. Only first 6, old but less work not taking it out
i = 0
for col in data_columns:
    df1 = df1.rename(columns=({i: col}))
    i += 1
df1.round(2)

writer = pd.ExcelWriter('Billing_Rubrik_' + lastMonth.strftime('%m-%Y') + '.xlsx', engine='xlsxwriter', options={'nan_inf_to_errors': True})
df2 = pd.DataFrame()
df2.to_excel(writer, sheet_name='Sheet')
workbook = writer.book
worksheet = writer.sheets['Sheet']

# create formats
bold = workbook.add_format({'align': 'center', 'bold': True})
r_date = workbook.add_format({'align': 'justified', 'bold': True})
title = workbook.add_format({'align': 'justified', 'font_size': 18, 'font_name': 'Cambria (Headings)'})
currency = workbook.add_format({'num_format': '$#,##0.00'})

# Add report static info
worksheet.insert_image('A1', 'Technologent.png')
worksheet.insert_image('A8', 'kbr.png')
worksheet.insert_image('C8', 'rubrik.png')
worksheet.write('A5', 'Rubrik Storage Billing Report for ' + lastMonth.strftime('%m-%Y'), title)
worksheet.write('A6', 'Report Date: ' + str(datetime.today()), r_date)
worksheet.write('A11', 'Locations', bold)
worksheet.write('B11', 'Avg Usage in GB', bold)
worksheet.write('C11', 'Price', bold)
# worksheet.write('D11', 'Total Usage', bold)
worksheet.write('F11', 'Number of Days', bold)
worksheet.write('G11', num_days_report_month)
worksheet.write('F12', 'Price Per Day', bold)
worksheet.write('G12', daily_cost)

# Add summary locations
data_row = 11
i = data_row + 1
for loc in locations.values():
    worksheet.write('A'+str(i), loc)
    i += 1

# Add date column
index = []
df1.index = list(map(int, df1.index))
for dex in df1.index:
    z = datetime.utcfromtimestamp(dex).strftime('%m-%d-%Y')
    index.append(z)

date = pd.Series(index, index=df1.index)

# Run function to create DataFrame and add to report
avg_usage_cell_start = 12

df_location(27, 28, 'PER05BKUPP00001')

# Add total monthly cost
cost_start = 12 + len(locations)
worksheet.write('C' + str(cost_start), '=SUM(C12:C' + str(cost_start - 1) + ')', currency)

# Set final column size and save
worksheet.set_column(0, 5, 15)
writer.save()
