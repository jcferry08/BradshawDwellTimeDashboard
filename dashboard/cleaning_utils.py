import pandas as pd
import numpy as np

def clean_open_dock(od_df):
    # Clean the 'Reference Number' field
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].astype(str).str.strip()
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].str.replace(r'<[^>]*>', '', regex=True)  # Remove HTML tags
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].str.replace(r'\n', ',', regex=True)
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].str.replace(r'\s+', ',', regex=True)
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].str.replace(r'[^0-9,]', '', regex=True)
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].str.replace(r',,', ',', regex=True)
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].str.rstrip(',')
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].str.lstrip(',')

    # Remove any rows that are Inbounds
    if 'Direction' in od_df.columns:
        od_df = od_df[od_df['Direction'] != 'Inbound']

    # Remove rows with missing Reference Numbers
    od_df.loc[:, 'Reference Number'] = od_df['Reference Number'].replace('', np.nan)
    od_df = od_df.dropna(subset=['Reference Number'])

    # Remove rows that are not 'Completed' or 'NoShow'
    if 'Status' in od_df.columns:
        od_df = od_df[~od_df['Status'].isin(['Arrived,', 'Cancelled', 'InProgress', 'Scheduled'])]

    # Keep required columns
    columns_to_keep = ['Dwell Time (mins)', 'Reference Number', 'Status']
    od_df = od_df[[col for col in columns_to_keep if col in od_df.columns]]

    # Renaming columns to match SQL query
    od_df.rename(columns={'Dwell Time (mins)': 'Dwell Time', 'Reference Number': 'SO Number'}, inplace=True)

    # Filling NaNs and converting 'Dwell Time' to hours
    if 'Dwell Time' in od_df.columns:
        od_df.loc[:, 'Dwell Time'] = od_df['Dwell Time'].fillna(0).astype('float')
        od_df.loc[:, 'Dwell Time'] = round(od_df['Dwell Time'] / 60, 2)

    # Ensure 'SO Number' column is standardized
    od_df['SO Number'] = od_df['SO Number'].astype(str).str.strip()
    
    return od_df

def clean_open_order(oo_df):
    # Standardize column names
    oo_df.columns = oo_df.columns.str.strip()

    # Keep necessary columns
    columns_to_keep = ['Appt Date and Time', 'SO #', 'Shipment Nbr', 'Order Status']
    oo_df = oo_df.drop(columns=oo_df.columns.difference(columns_to_keep))
    
    # Strip spaces and remove unexpected characters from 'Appt Date and Time'
    oo_df['Appt Date and Time'] = oo_df['Appt Date and Time'].str.strip()

    # Convert 'Appt Date and Time' to datetime, infer format
    oo_df['Appt Date and Time'] = pd.to_datetime(oo_df['Appt Date and Time'], errors='coerce')

    # Only drop rows with invalid dates
    oo_df = oo_df.dropna(subset=['Appt Date and Time'])

    # Convert 'SO #' to object type and ensure there are no empty values
    oo_df['SO #'] = oo_df['SO #'].astype(str).str.strip()
    oo_df = oo_df[oo_df['SO #'] != '']

    # Clean 'Shipment Nbr' to remove commas and keep it as a string
    oo_df['Shipment Nbr'] = oo_df['Shipment Nbr'].astype(str).str.replace(',', '')
    oo_df['Shipment Nbr'] = oo_df['Shipment Nbr'].str.extract(r'(\d+)', expand=False).fillna('')

    # Filter for shipped orders only (case insensitive and removing extra spaces)
    oo_df = oo_df[oo_df['Order Status'].str.strip().str.lower() == 'shipped']

    # Rename columns
    oo_df.rename(columns={'Appt Date and Time': 'Appt DateTime', 'SO #': 'SO Number', 'Shipment Nbr': 'Shipment ID'}, inplace=True)

    # Drop unnecessary columns and handle duplicates
    oo_df = oo_df.drop(columns='Order Status')
    oo_df = oo_df.sort_values(by=['Appt DateTime', 'Shipment ID'], ascending=[False, True])
    oo_df = oo_df.drop_duplicates(subset='Shipment ID', keep='first')
    oo_df = oo_df.drop_duplicates(subset='SO Number', keep='first')

    # Ensure there are no missing values in critical columns
    oo_df = oo_df.dropna(subset=['SO Number', 'Shipment ID'])

    return oo_df

def clean_trailer_activity(ta_df):
    # Standardize column names by removing leading/trailing spaces
    ta_df.columns = ta_df.columns.str.strip()

    # Keep only required columns
    columns_to_keep = [
        'CHECKIN DATE TIME', 'APPOINTMENT DATE TIME', 'CHECKOUT DATE TIME',
        'CARRIER', 'VISIT TYPE', 'ACTIVITY TYPE', 'Date/Time', 'SHIPMENT_ID'
    ]
    ta_df = ta_df.drop(columns=ta_df.columns.difference(columns_to_keep))

    # Filter rows based on 'ACTIVITY TYPE' and 'VISIT TYPE'
    ta_df = ta_df[ta_df['ACTIVITY TYPE'] == 'CLOSED']
    ta_df = ta_df[ta_df['VISIT TYPE'].isin(['Pickup Load', 'Live Load'])]

    # Handle missing Shipment ID and ensure it is treated as a string
    ta_df['SHIPMENT_ID'] = ta_df['SHIPMENT_ID'].astype(str).str.replace(',', '')
    ta_df['SHIPMENT_ID'] = ta_df['SHIPMENT_ID'].str.extract(r'(\d+)', expand=False).fillna('')
    ta_df = ta_df[ta_df['SHIPMENT_ID'] != '']

    # Convert datetime columns to datetime type
    datetime_columns = ['CHECKIN DATE TIME', 'APPOINTMENT DATE TIME', 'CHECKOUT DATE TIME', 'Date/Time']
    for col in datetime_columns:
        # Strip extra spaces and try to parse each column
        ta_df[col] = ta_df[col].str.strip()
        ta_df[col] = pd.to_datetime(ta_df[col], errors='coerce')

    # Handle missing datetime values carefully by inspecting each
    ta_df = ta_df.dropna(subset=['APPOINTMENT DATE TIME', 'CHECKOUT DATE TIME', 'CHECKIN DATE TIME'])

    # Sort and drop duplicates
    ta_df = ta_df.sort_values(by=['Date/Time', 'SHIPMENT_ID'], ascending=[False, True])
    ta_df = ta_df.drop_duplicates(subset='SHIPMENT_ID', keep='first')

    # Calculate required time
    def required_time(row):
        if row['VISIT TYPE'] == 'Live Load':
            return row['APPOINTMENT DATE TIME'] + pd.Timedelta(minutes=15)
        else:
            return row['APPOINTMENT DATE TIME'] + pd.Timedelta(hours=24)

    ta_df['Required Time'] = ta_df.apply(required_time, axis=1)

    # Determine compliance
    def compliance(row):
        if pd.notna(row['CHECKIN DATE TIME']) and row['Required Time'] >= row['CHECKIN DATE TIME']:
            return 'On Time'
        else:
            return 'Late'

    ta_df['Compliance'] = ta_df.apply(compliance, axis=1)

    # Add additional columns for reporting
    ta_df['Scheduled Date'] = ta_df['APPOINTMENT DATE TIME'].dt.strftime("%m-%d-%Y")
    ta_df['Week'] = ta_df['APPOINTMENT DATE TIME'].dt.isocalendar().week
    ta_df['Month'] = ta_df['APPOINTMENT DATE TIME'].dt.month

    # Keep only the final required columns
    columns_to_keep = [
        'CHECKIN DATE TIME', 'CHECKOUT DATE TIME', 'CARRIER', 'VISIT TYPE',
        'Date/Time', 'SHIPMENT_ID', 'Required Time', 'Compliance',
        'Scheduled Date', 'Week', 'Month'
    ]
    ta_df = ta_df.drop(columns=ta_df.columns.difference(columns_to_keep))

    # Rename columns
    ta_df.rename(columns={
        'CHECKIN DATE TIME': 'Checkin DateTime',
        'CHECKOUT DATE TIME': 'Checkout DateTime',
        'CARRIER': 'Carrier',
        'VISIT TYPE': 'Visit Type',
        'Date/Time': 'Loaded DateTime',
        'SHIPMENT_ID': 'Shipment ID'
    }, inplace=True)

    return ta_df
