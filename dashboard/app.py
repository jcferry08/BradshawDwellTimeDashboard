import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import duckdb
from cleaning_utils import clean_open_dock, clean_open_order, clean_trailer_activity
from datetime import datetime, timedelta

st.set_page_config(
    page_title='Bradshaw Dwell Time and Compliance Dashboard', 
    page_icon=':bar_chart:',
    layout='wide'
)

st.title('Dwell Time and Compliance Dashboard')
st.markdown('_Alpha V. 3.0.4')

tabs = st.tabs(["Data Upload", "Cleaned Data", "Daily Dashboard", "Weekly Dashboard", "Monthly Dashboard", "YTD Dashboard"])

# Dwell Time Calculation Function
def dwell_time(row):
    loaded_datetime = row['Loaded DateTime']
    checkin_datetime = row['Checkin DateTime']
    appt_datetime = row['Appointment DateTime']
    comp = row['Compliance']

    if pd.notna(loaded_datetime):
        if comp == 'On Time':
            dwell_time = round((loaded_datetime - appt_datetime).total_seconds() / 3600, 2)
        elif comp == 'Late':
            dwell_time = round((loaded_datetime - checkin_datetime).total_seconds() / 3600, 2)
        else:
            dwell_time = None
    else:
        dwell_time = None

    if dwell_time is not None and dwell_time <= 0:
        dwell_time = np.nan

    return dwell_time

# Define the dwell_and_ontime_compliance dataframe using session state
if 'dwell_and_ontime_compliance' not in st.session_state:
    st.session_state['dwell_and_ontime_compliance'] = pd.DataFrame()

# Create Data Upload tab
with tabs[0]:
    st.header("Data Upload")
    st.write("Please Upload the Open Dock, Open Order, and Trailer Activity CSV files here.")

    # Create side panel and function to load data. Then allow for a preview of each CSV file uploaded.
    @st.cache_data
    def load_data(file):
        data = pd.read_csv(file, parse_dates=True, low_memory=False)
        return data

    open_dock = st.sidebar.file_uploader("Upload Open Dock CSV file", type=['csv'])
    open_order = st.sidebar.file_uploader("Upload Open Order CSV file", type=['csv'])
    trailer_activity = st.sidebar.file_uploader("Upload Trailer Activity CSV file", type=['csv'])

    if open_dock is not None and open_order is not None and trailer_activity is not None:
        try:
            # Load the data
            od_df = load_data(open_dock)
            oo_df = load_data(open_order)
            ta_df = load_data(trailer_activity)

            # Clean and merge data sets immediately after all files are uploaded
            cleaned_open_dock = clean_open_dock(od_df)
            cleaned_open_order = clean_open_order(oo_df)
            cleaned_trailer_activity = clean_trailer_activity(ta_df)

            # Create a DuckDB connection
            con = duckdb.connect(":memory:")

            # Creating tables for DuckDB from cleaned DataFrames
            con.execute("CREATE TABLE open_dock AS SELECT * FROM cleaned_open_dock")
            con.execute("CREATE TABLE open_order AS SELECT * FROM cleaned_open_order")
            con.execute("CREATE TABLE trailer_report AS SELECT * FROM cleaned_trailer_activity")

            # First merge
            merged_df = con.execute("""
                SELECT
                    open_dock."SO Number" AS "Dock SO Number",
                    open_dock."Dwell Time" AS "Dwell Time",
                    open_dock."Status" AS "Event Status",
                    open_order."SO Number" AS "Order SO Number",
                    open_order."Appt DateTime" AS "Appointment DateTime",
                    open_order."Shipment ID" AS "Shipment ID"
                FROM open_dock
                LEFT JOIN open_order
                ON open_order."SO Number" = open_dock."SO Number"
            """).fetchdf()

            # Remove unnecessary columns
            if not merged_df.empty:
                columns_to_keep = ['Dock SO Number', 'Dwell Time', 'Event Status', 'Appointment DateTime', 'Shipment ID']
                merged_df = merged_df[columns_to_keep]
                merged_df.rename(columns={'Dock SO Number': 'SO Number'}, inplace=True)
                merged_df['SO Number'] = merged_df['SO Number'].astype('object')

            # Standardizing 'Shipment ID' column for the second merge
            merged_df['Shipment ID'] = merged_df['Shipment ID'].astype(str).str.strip()
            cleaned_trailer_activity['Shipment ID'] = cleaned_trailer_activity['Shipment ID'].astype(str).str.strip()

            # Second merge
            dwell_and_ontime_compliance = con.execute("""
                SELECT 
                    trailer_report."Shipment ID",
                    merged_df."SO Number",
                    merged_df."Appointment DateTime",
                    trailer_report."Required Time",
                    trailer_report."Checkin DateTime",
                    trailer_report."Checkout DateTime",
                    trailer_report.Carrier,
                    trailer_report."Visit Type",
                    trailer_report."Loaded DateTime",
                    trailer_report.Compliance,
                    merged_df."Dwell Time",
                    merged_df."Event Status",
                    trailer_report."Scheduled Date",
                    trailer_report.Week,
                    trailer_report.Month
                FROM merged_df
                LEFT JOIN trailer_report 
                ON trailer_report."Shipment ID" = merged_df."Shipment ID"
            """).fetchdf()

            # Remove unnecessary columns
            if not dwell_and_ontime_compliance.empty:
                columns_to_keep = [
                    'Shipment ID', 'SO Number', 'Appointment DateTime', 'Required Time',
                    'Checkin DateTime', 'Checkout DateTime', 'Carrier', 'Visit Type',
                    'Loaded DateTime', 'Compliance', 'Dwell Time', 'Event Status', 
                    'Scheduled Date', 'Week', 'Month'
                ]
                dwell_and_ontime_compliance = dwell_and_ontime_compliance[columns_to_keep]

                # Replace Compliance value with 'No Show' where Event Status is 'NoShow'
                dwell_and_ontime_compliance.loc[dwell_and_ontime_compliance['Event Status'] == 'NoShow', 'Compliance'] = 'No Show'
                dwell_and_ontime_compliance.loc[dwell_and_ontime_compliance['Event Status'] == 'NoShow', ['Checkin DateTime', 'Checkout DateTime', 'Loaded DateTime', 'Dwell Time']] = np.nan

                # Drop the 'Event Status' column
                dwell_and_ontime_compliance.drop(columns=['Event Status'], inplace=True)

                # Filter out specified carriers
                carriers_to_exclude = [
                    'AACT', 'DIMS', 'EXLA', 'SAIA', 'FXFE', 'FXLA', 'FXNL', 'F106', 'F107',
                    'F109', 'F110', 'F111', 'F112', 'F117', 'ODFL', 'U743', 'U746', 'U748', 'VQXX', 'CTII'
                ]
                dwell_and_ontime_compliance = dwell_and_ontime_compliance[~dwell_and_ontime_compliance['Carrier'].isin(carriers_to_exclude)]

                # Replace empty strings in 'Shipment ID' with NaN and drop rows with NaN in 'Shipment ID'
                dwell_and_ontime_compliance['Shipment ID'].replace('', np.nan, inplace=True)
                dwell_and_ontime_compliance.dropna(subset=['Shipment ID'], inplace=True)

                # Calculate dwell time using the provided function
                dwell_and_ontime_compliance['Calculated Dwell Time'] = dwell_and_ontime_compliance.apply(dwell_time, axis=1)

                # Compare the calculated dwell time with the existing dwell time
                dwell_and_ontime_compliance['Dwell Time'] = dwell_and_ontime_compliance[['Dwell Time', 'Calculated Dwell Time']].min(axis=1)

                # Drop intermediate column if not needed
                dwell_and_ontime_compliance.drop(columns=['Calculated Dwell Time'], inplace=True)

                # Store in session state
                st.session_state['dwell_and_ontime_compliance'] = dwell_and_ontime_compliance

        except Exception as e:
            st.warning(f"An error occurred while processing the uploaded files: {e}")

    with st.expander('Preview Open Dock CSV'):
        if open_dock is not None and 'od_df' in locals():
            st.write(od_df.head())
        else:
            st.info("Open Dock CSV not uploaded yet.")

    with st.expander('Preview Open Order CSV'):
        if open_order is not None and 'oo_df' in locals():
            st.write(oo_df.head())
        else:
            st.info("Open Order CSV not uploaded yet.")

    with st.expander('Preview Trailer Activity CSV'):
        if trailer_activity is not None and 'ta_df' in locals():
            st.write(ta_df.head())
        else:
            st.info("Trailer Activity CSV not uploaded yet.")

with tabs[1]:
    st.header("Cleaned Data")
    st.write("The data has been cleaned and merged. Please see the preview below and or download the cleaned data as a .csv.")

    with st.expander('Preview Cleaned Data'):
        if not st.session_state['dwell_and_ontime_compliance'].empty:
            st.write(st.session_state['dwell_and_ontime_compliance'].head())
        else:
            st.info("No cleaned data available yet. Please upload and clean the data first.")

    @st.cache_data
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')

    if not st.session_state['dwell_and_ontime_compliance'].empty:
        csv = convert_df(st.session_state['dwell_and_ontime_compliance'])

        st.download_button(
            label="Download Cleaned Data as CSV",
            data=csv,
            file_name='Bradshaw_Dwell_and_OnTime_Compliance.csv',
            mime='text/csv'
        )

with tabs[2]:
    st.header("Daily Dashboard")
    selected_date = st.date_input("Select Date for Daily Dashboard")
    if selected_date:
        selected_date_str = selected_date.strftime("%m-%d-%Y")

        st.write(f"Selected date for filtering: {selected_date_str}")

        if 'Scheduled Date' in st.session_state['dwell_and_ontime_compliance'].columns:
            filtered_df = st.session_state['dwell_and_ontime_compliance'][st.session_state['dwell_and_ontime_compliance']['Scheduled Date'] == selected_date_str]

            if filtered_df.empty:
                st.warning(f"No data found for the selected date: {selected_date_str}")
            else:
                # Creating layout for the graphs
                col1, col2 = st.columns([1, 1])

                # On Time Compliance by Date (left column)
                with col1:
                    with st.expander('On Time Compliance by Date'):
                        compliance_pivot = filtered_df.pivot_table(
                            values='Shipment ID', 
                            index='Scheduled Date',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                        compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
                        compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
                        st.subheader('On Time Compliance by Date')
                        st.table(compliance_pivot)

                # On Time Compliance by Carrier (right column)
                with col1:
                    with st.expander('On Time Compliance by Carrier'):
                        carrier_pivot = filtered_df.pivot_table(
                            values='Shipment ID',
                            index='Carrier',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                        carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                        carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
                        st.subheader('On Time Compliance by Carrier')
                        st.table(carrier_pivot)

                    # Creating the heatmap below the expander
                    heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
                    fig = go.Figure(data=go.Heatmap(
                        z=heatmap_data['On Time %'].values.reshape(-1, 1),
                        x=['On Time %'],
                        y=heatmap_data.index,
                        colorscale='RdYlGn',
                        colorbar=dict(title="On Time %"),
                        text=heatmap_data['On Time %'].values.reshape(-1, 1),
                        texttemplate="%{text:.2f}%",
                        showscale=True
                    ))
                    fig.update_layout(
                        title='On Time Compliance Percentage by Carrier',
                        xaxis_title='',
                        yaxis_title='Carrier',
                        yaxis_autorange='reversed',
                        height=len(heatmap_data) * 40 + 100
                    )
                    st.plotly_chart(fig, use_container_width=True, key="daily_heatmap")

                # Daily Count by Dwell Time (right column)
                with col2:
                    with st.expander('Daily Count by Dwell Time'):
                        if 'Dwell Time' in filtered_df.columns:
                            filtered_df['Dwell Time Category'] = pd.cut(
                                filtered_df['Dwell Time'],
                                bins=[0, 2, 3, 4, 5, np.inf],
                                labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                            )
                        else:
                            st.error("'Dwell Time' column is missing from the dataset.")

                        dwell_count_pivot = filtered_df.pivot_table(
                            values='Shipment ID',
                            index='Dwell Time Category',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                        dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                        dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                        st.subheader('Daily Count by Dwell Time')
                        st.table(dwell_count_pivot)

                    categories = dwell_count_pivot['Dwell Time Category']
                    late_percentages = dwell_count_pivot['Late % of Total']
                    on_time_percentages = dwell_count_pivot['On Time % of Total']
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=categories,
                        y=on_time_percentages,
                        name='On Time',
                        marker_color='green',
                        text=on_time_percentages,
                        textposition='inside'
                    ))
                    fig.add_trace(go.Bar(
                        x=categories,
                        y=late_percentages,
                        name='Late',
                        marker_color='red',
                        text=late_percentages,
                        textposition='inside'
                    ))
                    fig.update_layout(
                        barmode='stack',
                        title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
                        xaxis_title='Dwell Time Category',
                        yaxis_title='% of Total Shipments',
                        legend_title='Compliance',
                        xaxis_tickangle=-45
                    )
                    st.plotly_chart(fig, use_container_width=True, key="daily_100%_stacked")

                # Average Dwell Time by Visit Type (right column)
                with col2:
                    with st.expander('Average Dwell Time by Visit Type'):
                        dwell_average_pivot = filtered_df.pivot_table(
                            values='Dwell Time',
                            index='Visit Type',
                            columns='Compliance',
                            aggfunc='mean',
                            fill_value=np.nan
                        ).reset_index()

                        dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)
                        grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
                        grand_avg_row['Visit Type'] = 'Grand Average'
                        dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)
                        st.subheader('Average Dwell Time by Visit Type')
                        st.table(dwell_average_pivot)

                    if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            x=dwell_average_pivot['Visit Type'],
                            y=dwell_average_pivot['Late'],
                            name='Late',
                            marker=dict(color='rgba(255, 0, 0, 0.7)'),
                            text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],
                            textposition='auto',
                            textfont=dict(color='white')
                        ))
                        fig.add_trace(go.Bar(
                            x=dwell_average_pivot['Visit Type'],
                            y=dwell_average_pivot['On Time'],
                            name='On Time',
                            marker=dict(color='rgba(0, 128, 0, 0.7)'),
                            text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],
                            textposition='auto',
                            textfont=dict(color='white')
                        ))
                        fig.update_layout(
                            title='Average Dwell Time by Visit Type and Compliance',
                            xaxis_title='Visit Type',
                            yaxis_title='Average Dwell Time',
                            barmode='group',
                            xaxis_tickangle=-45,
                            legend_title='Compliance',
                            height=500,
                            width=800
                        )
                        st.plotly_chart(fig, use_container_width=True, key="daily_grouped_bar")
        else:
            st.error("Cannot filter by 'Scheduled Date' as the column does not exist.")

with tabs[3]:
    st.header("Weekly Dashboard")
    selected_week = st.number_input("Select Week Number for Weekly Report:", min_value=1, max_value=52)
    
    st.write(f"Selected week for filtering: {selected_week}")

    if 'Week' in st.session_state['dwell_and_ontime_compliance'].columns:
            filtered_df = st.session_state['dwell_and_ontime_compliance'][st.session_state['dwell_and_ontime_compliance']['Week'] == selected_week]

            if filtered_df.empty:
                st.warning(f"No data found for the selected week: {selected_week}")
            else:
                # Create two columns for layout
                col1, col2 = st.columns([1, 1])  # Column 1 is wider than Column 2
                # Pivot: On Time Compliance by Week (left column)
                with col1:
                    with st.expander('On Time Compliance by Week'):
                        compliance_pivot = filtered_df.pivot_table(
                            values='Shipment ID', 
                            index='Week',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                        compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
                        compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
                        st.subheader('On Time Compliance by Week')
                        st.table(compliance_pivot)

                    # Add Line Chart for Compliance Trend within the Week
                    
                    # Aggregating the data by date and compliance status
                    trend_data = filtered_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()

                    # Create line chart
                    fig = go.Figure()

                    # Add 'On Time' line to the chart
                    if 'On Time' in trend_data.columns:
                        fig.add_trace(go.Scatter(
                            x=trend_data['Scheduled Date'], 
                            y=trend_data['On Time'], 
                            mode='lines+markers+text',
                            name='On Time',
                            line=dict(color='green'),
                            text=trend_data['On Time'],  # Add counts as text labels
                            textposition='top center',  # Positioning the text above the points
                            textfont=dict(color='white'),  # Make the text color white
                        ))

                    # Add 'Late & No Show' line to the chart
                    if 'Late' in trend_data.columns or 'No Show' in trend_data.columns:
                        # Sum up 'Late' and 'No Show' counts for each date
                        trend_data['Late & No Show'] = trend_data.get('Late', 0) + trend_data.get('No Show', 0)
                        fig.add_trace(go.Scatter(
                            x=trend_data['Scheduled Date'], 
                            y=trend_data['Late & No Show'], 
                            mode='lines+markers+text',
                            name='Late & No Show',
                            line=dict(color='red'),
                            text=trend_data['Late & No Show'],
                            textposition='top center',
                            textfont=dict(color='white'),
                        ))


                    # Update layout for better readability
                    fig.update_layout(
                        title='Compliance Trend Over the Selected Week',
                        xaxis_title='Scheduled Date',
                        yaxis_title='Number of Shipments',
                        xaxis=dict(type='category'),  # Ensures dates are shown correctly even if sparse
                        template='plotly_white'
                    )

                    st.plotly_chart(fig, use_container_width=True, key="weekly_line_chart")

                # Pivot: On Time Compliance by Carrier (right column)
                with col1:
                    with st.expander('On Time Compliance by Carrier'):
                        # Creating the pivot table
                        carrier_pivot = filtered_df.pivot_table(
                            values='Shipment ID',
                            index='Carrier',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        
                        # Calculating Grand Total and On Time %
                        carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                        carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                        
                        # Sorting carriers by On Time % in descending order
                        carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
                        
                        # Displaying the pivot table in the expander
                        st.subheader('On Time Compliance by Carrier')
                        st.table(carrier_pivot)

                    # Creating the heat map below the expander in col1
                    # Prepare the data for the heat map
                    heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
                    
                    # Plotting the heat map using Plotly to blend better with Streamlit
                    fig = go.Figure(data=go.Heatmap(
                        z=heatmap_data['On Time %'].values.reshape(-1, 1),
                        x=['On Time %'],
                        y=heatmap_data.index,
                        colorscale='RdYlGn',  # Red to Green color map
                        colorbar=dict(title="On Time %"),
                        text=heatmap_data['On Time %'].values.reshape(-1, 1),
                        texttemplate="%{text:.2f}%",
                        showscale=True
                    ))
                    
                    # Customizing the plot layout
                    fig.update_layout(
                        title='On Time Compliance Percentage by Carrier',
                        xaxis_title='',
                        yaxis_title='Carrier',
                        yaxis_autorange='reversed',
                        height=len(heatmap_data) * 40 + 100  # Dynamic height based on number of carriers
                    )
                    
                    # Displaying the heat map using Streamlit
                    st.plotly_chart(fig, use_container_width=True, key="weekly_heatmap")

                # Daily Count by Dwell Time (right column)
                with col2:
                    with st.expander('Weekly Count by Dwell Time'):
                        # Check if 'Dwell Time' column exists in filtered_df to avoid KeyError
                        if 'Dwell Time' in filtered_df.columns:
                            # Creating 'Dwell Time Category' column if it doesn't exist
                            filtered_df['Dwell Time Category'] = pd.cut(
                                filtered_df['Dwell Time'],
                                bins=[0, 2, 3, 4, 5, np.inf],
                                labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                            )
                        else:
                            st.error("'Dwell Time' column is missing from the dataset.")

                        # Assuming dwell_count_pivot already exists
                        dwell_count_pivot = filtered_df.pivot_table(
                            values='Shipment ID',
                            index='Dwell Time Category',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                        dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                        dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                        
                        # Also showing the table for additional clarity
                        st.subheader('Weekly Count by Dwell Time')
                        st.table(dwell_count_pivot)

                    # Creating a 100% stacked bar chart using Plotly
                    categories = dwell_count_pivot['Dwell Time Category']
                    late_percentages = dwell_count_pivot['Late % of Total']
                    on_time_percentages = dwell_count_pivot['On Time % of Total']
                    
                    # Plotting with Plotly
                    fig = go.Figure()
                    
                    # Add On Time bars
                    fig.add_trace(go.Bar(
                        x=categories,
                        y=on_time_percentages,
                        name='On Time',
                        marker_color='green',
                        text=on_time_percentages,
                        textposition='inside'
                    ))
                    
                    # Add Late bars
                    fig.add_trace(go.Bar(
                        x=categories,
                        y=late_percentages,
                        name='Late',
                        marker_color='red',
                        text=late_percentages,
                        textposition='inside'
                    ))
                    
                    # Update layout for 100% stacked bar chart
                    fig.update_layout(
                        barmode='stack',
                        title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
                        xaxis_title='Dwell Time Category',
                        yaxis_title='% of Total Shipments',
                        legend_title='Compliance',
                        xaxis_tickangle=-45
                    )
                    
                    # Displaying in Streamlit
                    st.plotly_chart(fig, use_container_width=True, key="weekly_100%_stacked")

                # Pivot: Average Dwell Time by Visit Type (left column)
                with col2:
                    with st.expander('Average Dwell Time by Visit Type'):
                        dwell_average_pivot = filtered_df.pivot_table(
                            values='Dwell Time',
                            index='Visit Type',
                            columns='Compliance',
                            aggfunc='mean',
                            fill_value=np.nan
                        ).reset_index()

                        dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)

                        # Calculate Grand Average row
                        grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
                        grand_avg_row['Visit Type'] = 'Grand Average'
                        dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)

                        # Make sure the rendering part is inside the expander
                        st.subheader('Average Dwell Time by Visit Type')
                        st.table(dwell_average_pivot)

                    # Create a grouped bar chart to visualize dwell time by compliance (Late and On Time)
                    if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
                        fig = go.Figure()
                        
                        # Add bars for Late
                        fig.add_trace(go.Bar(
                            x=dwell_average_pivot['Visit Type'],
                            y=dwell_average_pivot['Late'],
                            name='Late',
                            marker=dict(color='rgba(255, 0, 0, 0.7)'),  # Red color with transparency
                            text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],  # Add percentages as text
                            textposition='auto',
                            textfont=dict(color='white')  # Set text color to white
                        ))
                        
                        # Add bars for On Time
                        fig.add_trace(go.Bar(
                            x=dwell_average_pivot['Visit Type'],
                            y=dwell_average_pivot['On Time'],
                            name='On Time',
                            marker=dict(color='rgba(0, 128, 0, 0.7)'),  # Green color with transparency
                            text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],  # Add percentages as text
                            textposition='auto',
                            textfont=dict(color='white')  # Set text color to white
                        ))

                        # Update layout for better readability
                        fig.update_layout(
                            title='Average Dwell Time by Visit Type and Compliance',
                            xaxis_title='Visit Type',
                            yaxis_title='Average Dwell Time',
                            barmode='group',
                            xaxis_tickangle=-45,
                            legend_title='Compliance',
                            height=500,
                            width=800
                        )

                        # Display the chart in Streamlit
                        st.plotly_chart(fig, use_container_width=True, key="weekly_grouped_bar")

with tabs[4]:
    st.header("Monthly Dashboard")
    selected_month = st.number_input("Select Month Number for Monthly Report:", min_value=1, max_value=12)

    st.write(f"Selected Month for filtering: {selected_month}")

    if 'Month' in st.session_state['dwell_and_ontime_compliance'].columns:
        filtered_df = st.session_state['dwell_and_ontime_compliance'][st.session_state['dwell_and_ontime_compliance']['Month'] == selected_month]

        if filtered_df.empty:
            st.warning(f"No data found for the selected month: {selected_month}")
        else:
            # Create two columns for layout
            col1, col2 = st.columns([1, 1])
            
            # Pivot: On Time Compliance by Month (left column)
            with col1:
                with st.expander('On Time Compliance by Month'):
                    compliance_pivot = filtered_df.pivot_table(
                        values='Shipment ID', 
                        index='Month',
                        columns='Compliance',
                        aggfunc='count',
                        fill_value=0
                    ).reset_index()
                    compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                    compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
                    compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
                    st.subheader('On Time Compliance by Month')
                    st.table(compliance_pivot)

                # Add Line Chart for Compliance Trend within the Month
                
                # Aggregating the data by date and compliance status
                trend_data = filtered_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()

                # Create line chart
                fig = go.Figure()

                # Add 'On Time' line to the chart
                if 'On Time' in trend_data.columns:
                    fig.add_trace(go.Scatter(
                        x=trend_data['Scheduled Date'], 
                        y=trend_data['On Time'], 
                        mode='lines+markers+text',
                        name='On Time',
                        line=dict(color='green'),
                        text=trend_data['On Time'],  # Add counts as text labels
                        textposition='top center',  # Positioning the text above the points
                        textfont=dict(color='white'),  # Make the text color white
                    ))

                # Add 'Late & No Show' line to the chart
                if 'Late' in trend_data.columns or 'No Show' in trend_data.columns:
                    trend_data['Late & No Show'] = trend_data.get('Late', 0) + trend_data.get('No Show', 0)
                    fig.add_trace(go.Scatter(
                        x=trend_data['Scheduled Date'], 
                        y=trend_data['Late & No Show'], 
                        mode='lines+markers+text',
                        name='Late & No Show',
                        line=dict(color='red'),
                        text=trend_data['Late & No Show'],
                        textposition='top center',
                        textfont=dict(color='white'),
                    ))

                # Update layout for better readability
                fig.update_layout(
                    title='Compliance Trend Over the Selected Month',
                    xaxis_title='Scheduled Date',
                    yaxis_title='Number of Shipments',
                    xaxis=dict(type='category'),  # Ensures dates are shown correctly even if sparse
                    template='plotly_white'
                )

                st.plotly_chart(fig, use_container_width=True, key="monthly_line_chart")

            # Pivot: On Time Compliance by Carrier (right column)
            with col1:
                with st.expander('On Time Compliance by Carrier'):
                    # Creating the pivot table
                    carrier_pivot = filtered_df.pivot_table(
                        values='Shipment ID',
                        index='Carrier',
                        columns='Compliance',
                        aggfunc='count',
                        fill_value=0
                    ).reset_index()
                    
                    # Calculating Grand Total and On Time %
                    carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                    carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                    
                    # Sorting carriers by On Time % in descending order
                    carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
                    
                    # Displaying the pivot table in the expander
                    st.subheader('On Time Compliance by Carrier')
                    st.table(carrier_pivot)

                # Creating the heat map below the expander in col1
                # Prepare the data for the heat map
                heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
                
                # Plotting the heat map using Plotly to blend better with Streamlit
                fig = go.Figure(data=go.Heatmap(
                    z=heatmap_data['On Time %'].values.reshape(-1, 1),
                    x=['On Time %'],
                    y=heatmap_data.index,
                    colorscale='RdYlGn',  # Red to Green color map
                    colorbar=dict(title="On Time %"),
                    text=heatmap_data['On Time %'].values.reshape(-1, 1),
                    texttemplate="%{text:.2f}%",
                    showscale=True
                ))
                
                # Customizing the plot layout
                fig.update_layout(
                    title='On Time Compliance Percentage by Carrier',
                    xaxis_title='',
                    yaxis_title='Carrier',
                    yaxis_autorange='reversed',
                    height=len(heatmap_data) * 40 + 100  # Dynamic height based on number of carriers
                )
                
                # Displaying the heat map using Streamlit
                st.plotly_chart(fig, use_container_width=True, key="monthly_heatmap")

            # Daily Count by Dwell Time (right column)
            with col2:
                with st.expander('Monthly Count by Dwell Time'):
                    # Check if 'Dwell Time' column exists in filtered_df to avoid KeyError
                    if 'Dwell Time' in filtered_df.columns:
                        # Creating 'Dwell Time Category' column if it doesn't exist
                        filtered_df['Dwell Time Category'] = pd.cut(
                            filtered_df['Dwell Time'],
                            bins=[0, 2, 3, 4, 5, np.inf],
                            labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                        )
                    else:
                        st.error("'Dwell Time' column is missing from the dataset.")

                    # Assuming dwell_count_pivot already exists
                    dwell_count_pivot = filtered_df.pivot_table(
                        values='Shipment ID',
                        index='Dwell Time Category',
                        columns='Compliance',
                        aggfunc='count',
                        fill_value=0
                    ).reset_index()
                    dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                    dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                    dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                    
                    # Also showing the table for additional clarity
                    st.subheader('Monthly Count by Dwell Time')
                    st.table(dwell_count_pivot)

                # Creating a 100% stacked bar chart using Plotly
                categories = dwell_count_pivot['Dwell Time Category']
                late_percentages = dwell_count_pivot['Late % of Total']
                on_time_percentages = dwell_count_pivot['On Time % of Total']
                
                # Plotting with Plotly
                fig = go.Figure()
                
                # Add On Time bars
                fig.add_trace(go.Bar(
                    x=categories,
                    y=on_time_percentages,
                    name='On Time',
                    marker_color='green',
                    text=on_time_percentages,
                    textposition='inside'
                ))
                
                # Add Late bars
                fig.add_trace(go.Bar(
                    x=categories,
                    y=late_percentages,
                    name='Late',
                    marker_color='red',
                    text=late_percentages,
                    textposition='inside'
                ))
                
                # Update layout for 100% stacked bar chart
                fig.update_layout(
                    barmode='stack',
                    title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
                    xaxis_title='Dwell Time Category',
                    yaxis_title='% of Total Shipments',
                    legend_title='Compliance',
                    xaxis_tickangle=-45
                )
                
                # Displaying in Streamlit
                st.plotly_chart(fig, use_container_width=True, key="monthly_100%_stacked")

            # Pivot: Average Dwell Time by Visit Type (left column)
            with col2:
                with st.expander('Average Dwell Time by Visit Type'):
                    dwell_average_pivot = filtered_df.pivot_table(
                        values='Dwell Time',
                        index='Visit Type',
                        columns='Compliance',
                        aggfunc='mean',
                        fill_value=np.nan
                    ).reset_index()

                    dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)

                    # Calculate Grand Average row
                    grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
                    grand_avg_row['Visit Type'] = 'Grand Average'
                    dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)

                    # Make sure the rendering part is inside the expander
                    st.subheader('Average Dwell Time by Visit Type')
                    st.table(dwell_average_pivot)

                # Create a grouped bar chart to visualize dwell time by compliance (Late and On Time)
                if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
                    fig = go.Figure()
                    
                    # Add bars for Late
                    fig.add_trace(go.Bar(
                        x=dwell_average_pivot['Visit Type'],
                        y=dwell_average_pivot['Late'],
                        name='Late',
                        marker=dict(color='rgba(255, 0, 0, 0.7)'),  # Red color with transparency
                        text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],  # Add percentages as text
                        textposition='auto',
                        textfont=dict(color='white')  # Set text color to white
                    ))
                    
                    # Add bars for On Time
                    fig.add_trace(go.Bar(
                        x=dwell_average_pivot['Visit Type'],
                        y=dwell_average_pivot['On Time'],
                        name='On Time',
                        marker=dict(color='rgba(0, 128, 0, 0.7)'),  # Green color with transparency
                        text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],  # Add percentages as text
                        textposition='auto',
                        textfont=dict(color='white')  # Set text color to white
                    ))

                    # Update layout for better readability
                    fig.update_layout(
                        title='Average Dwell Time by Visit Type and Compliance',
                        xaxis_title='Visit Type',
                        yaxis_title='Average Dwell Time',
                        barmode='group',
                        xaxis_tickangle=-45,
                        legend_title='Compliance',
                        height=500,
                        width=800
                    )

                    # Display the chart in Streamlit
                    st.plotly_chart(fig, use_container_width=True, key="monthly_grouped_bar")

with tabs[5]:
    st.header("Yearly Dashboard")
    st.write("YTD Dashboard")
    if not st.session_state['dwell_and_ontime_compliance'].empty:
        ytd_df = st.session_state['dwell_and_ontime_compliance']
        
        # Create two columns for layout
        col1, col2 = st.columns([1, 1])

        # Pivot: On Time Compliance by Week (left column)
        with col1:
            with st.expander('YTD On Time Compliance'):
                compliance_pivot = ytd_df.pivot_table(
                    values='Shipment ID', 
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index(drop=True)
                compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
                compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
                st.subheader('YTD On Time Compliance')
                st.table(compliance_pivot)

            # Aggregate data by month for the trend
            trend_data = ytd_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()
            trend_data['Scheduled Date'] = pd.to_datetime(trend_data['Scheduled Date'])

            # Extract month and year for grouping
            trend_data['Month'] = trend_data['Scheduled Date'].dt.to_period('M')

            # Group by month to calculate average counts for each compliance category
            monthly_avg = trend_data.groupby('Month').mean(numeric_only=True).reset_index()
            monthly_avg['Month'] = monthly_avg['Month'].dt.to_timestamp()

            # Round values for displaying in text
            monthly_avg['On Time Rounded'] = monthly_avg['On Time'].round()
            monthly_avg['Late + No Show'] = monthly_avg.get('Late', 0) + monthly_avg.get('No Show', 0)
            monthly_avg['Late + No Show Rounded'] = monthly_avg['Late + No Show'].round()

            # Create line chart
            fig = go.Figure()

            # Add 'On Time' line to the chart
            if 'On Time' in monthly_avg.columns:
                fig.add_trace(go.Scatter(
                    x=monthly_avg['Month'], 
                    y=monthly_avg['On Time'], 
                    mode='lines+markers+text',
                    name='On Time',
                    line=dict(color='green'),
                    text=monthly_avg['On Time Rounded'],
                    textposition='top center'
                ))

            # Add 'Late + No Show' line to the chart
            if 'Late' in monthly_avg.columns or 'No Show' in monthly_avg.columns:
                fig.add_trace(go.Scatter(
                    x=monthly_avg['Month'], 
                    y=monthly_avg['Late + No Show'], 
                    mode='lines+markers+text',
                    name='Late + No Show',
                    line=dict(color='red'),
                    text=monthly_avg['Late + No Show Rounded'],
                    textposition='top center'
                ))

            fig.update_layout(
                title='Average Compliance Trend Per Month',
                xaxis_title='Month',
                yaxis_title='Average Number of Shipments',
                xaxis=dict(type='category'),
                template='plotly_white'
            )

            st.plotly_chart(fig, use_container_width=True, key="ytd_line_chart")

        # Weekly Count by Dwell Time (right column)
        with col1:
            with st.expander('Weekly Count by Dwell Time'):
                if 'Dwell Time' in ytd_df.columns:
                    ytd_df['Dwell Time Category'] = pd.cut(
                        ytd_df['Dwell Time'],
                        bins=[0, 2, 3, 4, 5, np.inf],
                        labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                    )
                else:
                    st.error("'Dwell Time' column is missing from the dataset.")

                dwell_count_pivot = ytd_df.pivot_table(
                    values='Shipment ID',
                    index='Dwell Time Category',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                
                st.subheader('Weekly Count by Dwell Time')
                st.table(dwell_count_pivot)

            categories = dwell_count_pivot['Dwell Time Category']
            late_percentages = dwell_count_pivot['Late % of Total']
            on_time_percentages = dwell_count_pivot['On Time % of Total']
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=categories,
                y=on_time_percentages,
                name='On Time',
                marker_color='green',
                text=on_time_percentages,
                textposition='inside'
            ))
            fig.add_trace(go.Bar(
                x=categories,
                y=late_percentages,
                name='Late',
                marker_color='red',
                text=late_percentages,
                textposition='inside'
            ))
            fig.update_layout(
                barmode='stack',
                title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
                xaxis_title='Dwell Time Category',
                yaxis_title='% of Total Shipments',
                legend_title='Compliance',
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True, key="ytd_100%_stacked")

        # Average Dwell Time by Visit Type (right column)
        with col1:
            with st.expander('Average Dwell Time by Visit Type'):
                dwell_average_pivot = ytd_df.pivot_table(
                    values='Dwell Time',
                    index='Visit Type',
                    columns='Compliance',
                    aggfunc='mean',
                    fill_value=np.nan
                ).reset_index()

                dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)

                grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
                grand_avg_row['Visit Type'] = 'Grand Average'
                dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)

                st.subheader('Average Dwell Time by Visit Type')
                st.table(dwell_average_pivot)

            if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Visit Type'],
                    y=dwell_average_pivot['Late'],
                    name='Late',
                    marker=dict(color='rgba(255, 0, 0, 0.7)'),
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],
                    textposition='auto',
                    textfont=dict(color='white')
                ))
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Visit Type'],
                    y=dwell_average_pivot['On Time'],
                    name='On Time',
                    marker=dict(color='rgba(0, 128, 0, 0.7)'),
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],
                    textposition='auto',
                    textfont=dict(color='white')
                ))
                fig.update_layout(
                    title='Average Dwell Time by Visit Type and Compliance',
                    xaxis_title='Visit Type',
                    yaxis_title='Average Dwell Time',
                    barmode='group',
                    xaxis_tickangle=-45,
                    legend_title='Compliance',
                    height=500,
                    width=800
                )
                st.plotly_chart(fig, use_container_width=True, key="ytd_grouped_bar")

        # Pivot: On Time Compliance by Carrier (right column)
        with col2:
            with st.expander('On Time Compliance by Carrier'):
                carrier_pivot = ytd_df.pivot_table(
                    values='Shipment ID',
                    index='Carrier',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                
                carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
                
                st.subheader('On Time Compliance by Carrier')
                st.table(carrier_pivot)

            # Heat map for On Time Compliance by Carrier
            heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
            fig = go.Figure(data=go.Heatmap(
                z=heatmap_data['On Time %'].values.reshape(-1, 1),
                x=['On Time %'],
                y=heatmap_data.index,
                colorscale='RdYlGn',
                colorbar=dict(title="On Time %"),
                text=heatmap_data['On Time %'].values.reshape(-1, 1),
                texttemplate="%{text:.2f}%",
                showscale=True
            ))
            fig.update_layout(
                title='On Time Compliance Percentage by Carrier',
                xaxis_title='',
                yaxis_title='Carrier',
                yaxis_autorange='reversed',
                height=len(heatmap_data) * 40 + 100
            )
            st.plotly_chart(fig, use_container_width=True, key="ytd_heatmap")
    else:
        st.error("No data available for the selected date range.")