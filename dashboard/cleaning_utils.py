import pandas as pd
import numpy as np

def clean_open_dock(od_df):
    # Clean the Open Dock CSV file

    # Cleaning the 'Reference Number' field
    od_df['Reference Number'] = od_df['Reference Number'].str.strip()
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r'\n', ',', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r' ', ',', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r'[^0-9,]', '', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.replace(r',,', ',', regex=True)
    od_df['Reference Number'] = od_df['Reference Number'].str.rstrip(',')
    od_df['Reference Number'] = od_df['Reference Number'].str.lstrip(',')

    # Removing 'Inbound' direction
    od_df = od_df[od_df['Direction'] != 'Inbound']

    # Handle missing Reference Number
    od_df['Reference Number'] = od_df['Reference Number'].replace('', np.nan)
    od_df = od_df.dropna(subset=['Reference Number'])

    # Keeping only the required columns
    columns_to_keep = ['Dwell Time (mins)', 'Reference Number']
    od_df = od_df.drop(columns=od_df.columns.difference(columns_to_keep))

    # Renaming columns
    od_df.rename(columns={'Dwell Time (mins)': 'Dwell Time', 'Reference Number': 'SO Number'}, inplace=True)

    # Filling NaNs and converting 'Dwell Time' to hours
    od_df = od_df.fillna(0)
    od_df['Dwell Time'] = round(od_df['Dwell Time'].astype('float') / 60, 2)

    return od_df

def clean_open_order(oo_df):
    # Clean the Open Order CSV file

    # Keep only necessary columns
    columns_to_keep = ['Appt Date and Time', 'SO #', 'Shipment Nbr', 'Order Status']
    oo_df = oo_df.drop(columns=oo_df.columns.difference(columns_to_keep))

    # Convert 'Appt Date and Time' to datetime and fill NaNs if needed
    oo_df['Appt Date and Time'] = pd.to_datetime(oo_df['Appt Date and Time'], errors='coerce')
    oo_df = oo_df.dropna(subset=['Appt Date and Time'])

    # Convert 'SO #' to object type
    oo_df['SO #'] = oo_df['SO #'].astype('object')

    # Clean 'Shipment Nbr' to remove commas and ensure it is numeric
    oo_df['Shipment Nbr'] = oo_df['Shipment Nbr'].astype(str).str.replace(',', '').str.extract(r'(\d+)', expand=False)
    oo_df['Shipment Nbr'] = pd.to_numeric(oo_df['Shipment Nbr'], errors='coerce').fillna(0).astype('int64')

    # Filter for shipped orders only
    oo_df = oo_df[oo_df['Order Status'] == 'Shipped']

    # Rename columns
    oo_df.rename(columns={'Appt Date and Time': 'Appt DateTime', 'SO #': 'SO Number', 'Shipment Nbr': 'Shipment ID'}, inplace=True)

    # Drop unnecessary columns and handle duplicates
    oo_df = oo_df.drop(columns='Order Status')
    oo_df = oo_df.sort_values(by=['Appt DateTime', 'Shipment ID'], ascending=[False, True])
    oo_df = oo_df.drop_duplicates(subset='Shipment ID', keep='first')
    oo_df = oo_df.drop_duplicates(subset='SO Number', keep='first')

    return oo_df

def clean_trailer_activity(ta_df):
    # Clean the Trailer Activity CSV file

    # Keep only required columns
    columns_to_keep = ['CHECKIN DATE TIME', 'APPOINTMENT DATE TIME', 'CHECKOUT DATE TIME', 'CARRIER', 'VISIT TYPE', 'ACTIVITY TYPE ', 'Date/Time', 'SHIPMENT_ID']
    ta_df = ta_df.drop(columns=ta_df.columns.difference(columns_to_keep))

    # Filter rows based on 'ACTIVITY TYPE ' and 'VISIT TYPE'
    ta_df = ta_df[ta_df['ACTIVITY TYPE '] == 'CLOSED']
    ta_df = ta_df[ta_df['VISIT TYPE'].isin(['Pickup Load', 'Live Load'])]

    # Handle missing Shipment ID
    ta_df['SHIPMENT_ID'] = ta_df['SHIPMENT_ID'].replace('', np.nan)
    ta_df = ta_df.dropna(subset=['SHIPMENT_ID'])

    # Remove commas from 'SHIPMENT_ID' if any and ensure it is numeric
    ta_df['SHIPMENT_ID'] = ta_df['SHIPMENT_ID'].astype(str).str.replace(',', '').str.extract(r'(\d+)', expand=False)
    ta_df['SHIPMENT_ID'] = pd.to_numeric(ta_df['SHIPMENT_ID'], errors='coerce').fillna(0).astype('int64')

    # Sort and drop duplicates
    ta_df = ta_df.sort_values(by=['Date/Time', 'SHIPMENT_ID'], ascending=[False, True])
    ta_df = ta_df.drop_duplicates(subset='SHIPMENT_ID', keep='first')

    # Convert datetime columns to datetime type
    ta_df['CHECKIN DATE TIME'] = pd.to_datetime(ta_df['CHECKIN DATE TIME'], errors='coerce')
    ta_df['APPOINTMENT DATE TIME'] = pd.to_datetime(ta_df['APPOINTMENT DATE TIME'], errors='coerce')
    ta_df['CHECKOUT DATE TIME'] = pd.to_datetime(ta_df['CHECKOUT DATE TIME'], errors='coerce')
    ta_df['Date/Time'] = pd.to_datetime(ta_df['Date/Time'], errors='coerce')

    # Handle missing datetime values
    ta_df = ta_df.dropna(subset=['APPOINTMENT DATE TIME', 'CHECKOUT DATE TIME', 'CHECKIN DATE TIME'])

    # Conversions
    ta_df['VISIT TYPE'] = ta_df['VISIT TYPE'].astype('category')
    ta_df['ACTIVITY TYPE '] = ta_df['ACTIVITY TYPE '].astype('category')

    # Calculate required time
    def required_time(row):
        if row['VISIT TYPE'] == 'Live Load':
            return row['APPOINTMENT DATE TIME'] + pd.Timedelta(minutes=15)
        else:
            return row['APPOINTMENT DATE TIME'] + pd.Timedelta(hours=24)

    ta_df['Required Time'] = ta_df.apply(required_time, axis=1)

    # Determine compliance
    def compliance(row):
        if row['Required Time'] >= row['CHECKIN DATE TIME']:
            return 'On Time'
        else:
            return 'Late'

    ta_df['Compliance'] = ta_df.apply(compliance, axis=1)

    # Add additional columns for reporting
    ta_df['Scheduled Date'] = ta_df['APPOINTMENT DATE TIME'].dt.strftime("%m-%d-%Y")
    ta_df['Week'] = ta_df['APPOINTMENT DATE TIME'].dt.isocalendar().week
    ta_df['Month'] = ta_df['APPOINTMENT DATE TIME'].dt.month

    # Ensure the Scheduled Date column is present
    if 'Scheduled Date' not in ta_df.columns:
        raise ValueError("Scheduled Date column is missing after processing.")

    # Keep only the final required columns
    columns_to_keep = ['CHECKIN DATE TIME', 'CHECKOUT DATE TIME', 'CARRIER', 'VISIT TYPE', 'Date/Time', 'SHIPMENT_ID', 'Required Time', 'Compliance', 'Scheduled Date', 'Week', 'Month']
    ta_df = ta_df.drop(columns=ta_df.columns.difference(columns_to_keep))

    # Rename columns
    ta_df.rename(columns={'CHECKIN DATE TIME': 'Checkin DateTime', 'CHECKOUT DATE TIME': 'Checkout DateTime', 'CARRIER': 'Carrier', 'VISIT TYPE': 'Visit Type', 'Date/Time': 'Loaded DateTime', 'SHIPMENT_ID': 'Shipment ID'}, inplace=True)

    return ta_df
