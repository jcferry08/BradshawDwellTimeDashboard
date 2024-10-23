import pandas as pd
import numpy as np

def clean_open_dock(od_df):

    od_df['Reference Number'] = od_df['Reference Number'].astype(str).str.strip()
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r'<[^>]*>', '', regex=True)  # Remove HTML tags
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r'\n', ',', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r'\s+', ',', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r'[^0-9,]', '', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r',,', ',', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.rstrip(',')
    od_df['Reference Number'] = od_df['Reference Number'].str.lstrip(',')

    if 'Direction' in od_df.columns:
        od_df = od_df[od_df['Direction'] != 'Inbound']

    od_df['Reference Number'] = od_df['Reference Number'].replace('', np.nan)
    od_df = od_df.dropna(subset=['Reference Number'])

    columns_to_keep = ['Dwell Time (mins)', 'Reference Number']
    od_df = od_df[[col for col in columns_to_keep if col in od_df.columns]]

    od_df.rename(columns={'Dwell Time (mins)': 'Dwell Time', 'Reference Number': 'SO Number'}, inplace=True)

    if 'Dwell Time' in od_df.columns:
        od_df['Dwell Time'] = od_df['Dwell Time'].fillna(0).astype('float')
        od_df['Dwell Time'] = round(od_df['Dwell Time'] / 60, 2)

    return od_df

def clean_open_order(oo_df):

    columns_to_keep = ['Appt Date and Time', 'SO #', 'Shipment Nbr', 'Order Status']
    oo_df = oo_df.drop(columns=oo_df.columns.difference(columns_to_keep))

    oo_df['Shipment Nbr'] = oo_df['Shipment Nbr'].replace('', np.nan)
    oo_df = oo_df.dropna(subset = ['Shipment Nbr', 'Appt Date and Time'])

    oo_df['Appt Date and Time'] = pd.to_datetime(oo_df['Appt Date and Time'])
    oo_df['SO #'] = oo_df['SO #'].astype('object')
    oo_df['Shipment Nbr'] = oo_df['Shipment Nbr'].astype('int64')
    oo_df['Order Status'] = oo_df['Order Status'].astype('category')

    oo_df= oo_df[oo_df['Order Status'] == 'Shipped']

    oo_df.rename(columns={'Appt Date and Time': 'Appt DateTime', 'SO #': 'SO Number', 'Shipment Nbr': 'Shipment ID'}, inplace=True)

    oo_df = oo_df.drop(columns = 'Order Status')

    oo_df = oo_df.sort_values(by=['Appt DateTime', 'Shipment ID'], ascending=[False, True])

    oo_df = oo_df.drop_duplicates(subset='Shipment ID', keep='first')
    oo_df = oo_df.drop_duplicates(subset='SO Number', keep='first')

    return oo_df

def clean_trailer_activity(ta_df):

    ta_df.columns = ta_df.columns.str.strip()

    columns_to_keep = [
        'CHECKIN DATE TIME', 'APPOINTMENT DATE TIME', 'CHECKOUT DATE TIME',
        'CARRIER', 'VISIT TYPE', 'ACTIVITY TYPE', 'Date/Time', 'SHIPMENT_ID'
    ]
    ta_df = ta_df.drop(columns=ta_df.columns.difference(columns_to_keep))

    ta_df = ta_df[ta_df['ACTIVITY TYPE'] == 'CLOSED']
    ta_df = ta_df[ta_df['VISIT TYPE'].isin(['Pickup Load', 'Live Load'])]

    ta_df['SHIPMENT_ID'] = ta_df['SHIPMENT_ID'].replace('', np.nan)
    ta_df = ta_df.dropna(subset=['SHIPMENT_ID'])

    ta_df['SHIPMENT_ID'] = ta_df['SHIPMENT_ID'].astype(str).str.replace(',', '').str.extract(r'(\d+)', expand=False)
    ta_df['SHIPMENT_ID'] = pd.to_numeric(ta_df['SHIPMENT_ID'], errors='coerce').fillna(0).astype('int64')

    ta_df['CHECKIN DATE TIME'] = pd.to_datetime(ta_df['CHECKIN DATE TIME'], errors='coerce')
    ta_df['APPOINTMENT DATE TIME'] = pd.to_datetime(ta_df['APPOINTMENT DATE TIME'], errors='coerce')
    ta_df['CHECKOUT DATE TIME'] = pd.to_datetime(ta_df['CHECKOUT DATE TIME'], errors='coerce')
    ta_df['Date/Time'] = pd.to_datetime(ta_df['Date/Time'], errors='coerce')

    ta_df = ta_df.dropna(subset=['APPOINTMENT DATE TIME', 'CHECKOUT DATE TIME', 'CHECKIN DATE TIME'])

    ta_df = ta_df.sort_values(by=['Date/Time', 'SHIPMENT_ID'], ascending=[False, True])
    ta_df = ta_df.drop_duplicates(subset='SHIPMENT_ID', keep='first')

    def required_time(row):
        if row['VISIT TYPE'] == 'Live Load':
            return row['APPOINTMENT DATE TIME'] + pd.Timedelta(minutes=15)
        else:
            return row['APPOINTMENT DATE TIME'] + pd.Timedelta(hours=24)

    ta_df['Required Time'] = ta_df.apply(required_time, axis=1)

    def compliance(row):
        if row['Required Time'] >= row['CHECKIN DATE TIME']:
            return 'On Time'
        else:
            return 'Late'

    ta_df['Compliance'] = ta_df.apply(compliance, axis=1)

    ta_df['Scheduled Date'] = ta_df['APPOINTMENT DATE TIME'].dt.strftime("%m-%d-%Y")
    ta_df['Week'] = ta_df['APPOINTMENT DATE TIME'].dt.isocalendar().week
    ta_df['Month'] = ta_df['APPOINTMENT DATE TIME'].dt.month

    if 'Scheduled Date' not in ta_df.columns:
        raise ValueError("Scheduled Date column is missing after processing.")

    columns_to_keep = [
        'CHECKIN DATE TIME', 'CHECKOUT DATE TIME', 'CARRIER', 'VISIT TYPE',
        'Date/Time', 'SHIPMENT_ID', 'Required Time', 'Compliance',
        'Scheduled Date', 'Week', 'Month'
    ]
    ta_df = ta_df.drop(columns=ta_df.columns.difference(columns_to_keep))

    ta_df.rename(columns={
        'CHECKIN DATE TIME': 'Checkin DateTime',
        'CHECKOUT DATE TIME': 'Checkout DateTime',
        'CARRIER': 'Carrier',
        'VISIT TYPE': 'Visit Type',
        'Date/Time': 'Loaded DateTime',
        'SHIPMENT_ID': 'Shipment ID'
    }, inplace=True)

    return ta_df
