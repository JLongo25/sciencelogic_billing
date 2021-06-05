#!/bin/python3
import pandas as pd
import requests
import json
from es_connect_new import connect_elasticsearch
from kbr_helper import p_logs, u_logs

es = connect_elasticsearch()

passparams = {'hide_filterinfo': '1', 'duration': '1d', 'filter.0.class_type/guid': '343db69c690d761d82566e5ecc824eb9'}
r = requests.get('https://sciencelogicurl.com/api/device', auth=(u_logs(), p_logs()), verify=False, params=passparams)
status = r.status_code
devices = json.loads(r.content.decode('utf-8'))

"""
        ingested: 6426
        physical: 6427
        capacity : 6428
"""

perf_id = ['6426', '6427', '6428']

df = pd.DataFrame()
writer = pd.ExcelWriter('rubrik_output.xlsx')
# Set Report Start and End Date
start_date = '07/01/19'
end_date = '05/01/21'

for device in devices:
    device_id = device['URI']
    device_name = device['description']
    temp = []
    for perf in perf_id:
        print(perf)
        url_usage = f'https://sciencelogicurl.com{device_id}/performance_data/{perf}/normalized_daily?insert_nulls=1&beginstamp={start_date}&endstamp={end_date}'
        passparams2 = {'hide_options': '1'}
        r_usage = requests.get(url_usage, auth=(u_logs(), p_logs()),
                         verify=False, params=passparams2)
        status = r.status_code
        usage = json.loads(r_usage.content.decode('utf-8'))
        temp.append([usage['data']['0']['avg']][0])

    df = pd.DataFrame.from_dict(temp)
    df = df.transpose()
    df = df.astype('float')
    df = df.rename(columns={0: 'ingested', 1: 'physical', 2: 'capacity'})
    df['timestamp'] = pd.to_datetime(df.index, unit='s')
    print(df)
    df.to_excel(writer, sheet_name=device_name)
writer.save()


