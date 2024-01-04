import streamlit as st
import requests
import json
import time
import ast
import datetime
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd
import logging
from datetime import datetime
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import streamlit_option_menu
from streamlit_option_menu import option_menu


# Set up logging to console and logs.log
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('logs.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


def get_data():
    scope = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]

    creds_dict = {
        "type": "service_account",
        "project_id": "still-chassis-377404",
        "private_key_id": "cc534c7298640abd0cc1f14ea00bbcf06c4bf2e4",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCwDjkn9fGkKNsj\nliGdhTvvsGz2Kr0iAb0nRNeh6F3pR5rE+qU9SrigU+wvi7BzC6UuPOF8/vN993eo\ntJYldcjNnfEac/sJSzxY0syVdJGSaNYNgtjLN3i0qVi+Mj5MI7XbhcN9RB7JrYyE\nZJTuibIP0h98AETTJ4Nga5S0/hVGxjxN99ceGyjUoINPc6kCf3KZ0mu0z5wWCmoX\ntwQCzZu4B0Z3CVcvSYmZ0DNgxS557E8vj7z1Z5wZGSZdH4T5HYt75kHpGEGb6Mit\nEgGVjlOg9n5ZQukY3JajZW1aHVnPw+QyL7s/2/cpmLONZCEknEGzWdcWtLqXE693\n6/jqQkPhAgMBAAECggEAAbMWyNSjpoq2bYYvgOPJC6v6VqNcUWMO6RLNuhm8068F\n8XeHTNYsfhpmofbelzMPlH4F9zzxgM/OGeoptjwy2g6iReJvaUw5TZTgtg36scfN\nTogVgQYMu5cUQjxzRCzmvohwtgQy/+NrAy7YDo10aJlbWx5ti6mf/Q2iJop6uMTW\nXmeTz6hjVIW86ZoweCK6klM7tFgmpTjYi38MCA+h76Hh+/4WPVcPhNKo+NjYUDJM\n9KAswY21YOdFsyeLvfwiYARcI3fTkzk/9j0p+2kTs5nTBfGFonTVhuj+C05JkpUB\n2RyCNbFGiuhnWVX7Tk4eFR/XMokervYe0ZGPaiNxGQKBgQDhGsIafDiN+JTB2T1y\nq8hsPI6rIyUIVqfSKyqJl4awUpIYKZEe5EYU6iGQzLud2z8+xSHQgNOhQWUNjl45\ndPvJfsxD7+vZGomHzzu6zZGhmgsksf9qFRhBOYKwaDogPnBQvubM5SyNVVH+TbV7\nt0wyWAi4xhqziePJYb2bfEXdIwKBgQDIOBZbjzD5ciN0CdzkvYwd8j/2sQRGlLMT\nVaZL15X1rL8/4ygCCKqgtDIXCxMTkgTPB4Ks5wl8nkBlhBFR6RI8Zb1m+olSf/WR\ndhB0aFWJBPP/6wWfZPW0vdcFqKYsiYnkmXI1fScXNYupfBvoewiv5kXtmzI3kzN+\noCTLnNjVKwKBgAeir7jUQ9rU9r7IjqQIHalGOJSngYcR4Wlom23FXQU4F8qIBmsk\n1Re6hV7zAt4s5K1NZIM9C3Cp1jKqhJmwVSYVjfoz9i0MEGQx7FW+yVTQ89XPz9ka\nVBNKTxuKvYYAeDDU+OE0WNmGblyQ6DoFEqmWgN07owY6BCb9kL/obDo/AoGAfaw2\n3GGUq/4oor1OGYWgXWuhGoch1+ueO5SXPSOkzQARS1iZ1Cz8bMKubihVYQNWYVyl\neUHLgp0SbqXm+TMCb0atC+ZWK6KF0Tf6PBcBGXAyd4Bdlx8X2ssOHE8vfuUV2Jc4\nSLq1vqutbkSof9Q6L66qVeoEKz4KJIEAl41rzOECgYEAo/akyxjrHs1TFm+6Tgf+\npAj+WKEXd4jk0hVuMsTUN5DcplatmMMf77CDLhKbIrATrTkpp5/ukmYRRpvB0NNp\nnNrIh39Ono60OvxC+ryLWqwJKOB92qUg0RPlZ4t64T3D1mRLq7/pkBToXkMCH4D4\nITVrP9C0GKr5BrBws01RI4I=\n-----END PRIVATE KEY-----\n",
        "client_email": "agney-422@still-chassis-377404.iam.gserviceaccount.com",
        "client_id": "105339784783518170979",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/agney-422%40still-chassis-377404.iam.gserviceaccount.com"
    }

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict,scope)
        client = gspread.authorize(creds)
        #Fetch the sheet
        logger.info("Opening the sheet")
        sh = client.open('Eblock Transportjobs')
        logger.info("Fetching the sheet")
        sheet_deliveries=sh.worksheet("Deliveries")
        logger.info("Fetching the data")
        db_deliveries=sheet_deliveries.get_all_values()
        deliveries_db=pd.DataFrame(db_deliveries[1:],columns=db_deliveries[0])
        logger.info("Data Retieved")
        return deliveries_db
    except Exception as e:
        logger.error("Error occured while fetching data")
        logger.error(e)
        return None
       

# Getting Recommendations
    
def convert_date_format(input_date):
    # Convert the input date string to a datetime object
    date_object = datetime.strptime(input_date, "%B %d, %Y")

    # Format the datetime object as a string in "yyyy-mm-dd" format
    formatted_date = date_object.strftime("%Y-%m-%d")

    return formatted_date

def to_date(df):
    date_columns = ['Estimated Pickup Date', 'Adjusted Pickup', 'Estimated Delivery', 'Adjusted Delivery', 'Final Delivery']
    for c in date_columns:
        df[c] =df[c].apply(lambda x: pd.to_datetime(x.split(" - ")[0], errors='coerce'))
    df['Estimated Pickup/Delivery']=df['Estimated Delivery']-df['Estimated Pickup Date']
    df['Final Pickup/Delivery']=df['Final Delivery']-df['Estimated Pickup Date']
    df['Time Difference']=df['Final Pickup/Delivery']-df['Estimated Pickup/Delivery']
    df['Time Difference']=df['Time Difference'].apply(lambda x: x.days)
    return df

def carrier_routes(df):
    # Step 1: Remove non-numeric characters from price values
    df['Final Amount'] = df['Final Amount'].str.replace('[^\d.]', '', regex=True)

    # Step 2: Convert price values to numeric format
    df['Final Amount'] = pd.to_numeric(df['Final Amount'])

    # Step 3: Group by carriers and calculate average price
    carrier_prices = df
    return carrier_prices

def custom_std(series):
    # Return 0 if there's only one data point, else calculate std
    return series.std() if len(series) > 1 else 0

def count_trips(df, carrier):
    return len(df[df["Carrier"] == carrier])

def count_unique_routes(df, carrier):
    
    t = df.copy()
    t["route"] = t["FromZIP"] + " " + t["ToZIP"]
    return len(np.unique(t[t["Carrier"] == carrier]["route"].values))

def TransportAnalysis(df):
    df = df.fillna('-')
    df = to_date(df)
    df = carrier_routes(df)
    df = df[['Carrier', 'FromZIP', 'ToZIP', 'Final Amount', 'Estimated Pickup/Delivery', 'Final Pickup/Delivery', 'Time Difference', "Quotes"]]
    
    # Convert 'Time Difference' to numeric and handle NaN values
    df['Time Difference'] = pd.to_numeric(df['Time Difference'], errors='coerce')
    
    df["Total Trips"] = df["Carrier"].apply(lambda x:count_trips(df, x))
    df["Unique Routes"] = df["Carrier"].apply(lambda x:count_unique_routes(df, x))
    # Group the data
    grouped = df.groupby(['Carrier', 'FromZIP', 'ToZIP'])

    # Calculate the mean and std using custom std function
    result = grouped.agg({'Final Amount': ['mean', custom_std, "min", "max"], 'Time Difference': ['mean', custom_std], 'Total Trips': 'max', 'Unique Routes': 'max'})
    result.columns = [' '.join(col).strip() for col in result.columns.values]
    # Sort the result
    result = result.sort_values(by='Final Amount mean')
    return result.reset_index()

def no_delay_ratio(df, carrier, FromZIP, ToZIP):
    filtered = df[(df['FromZIP'] == FromZIP) & (df['ToZIP'] == ToZIP) & (df['Carrier'] == carrier)]
    if len(filtered) == 0:
        return 0
    return len(filtered[filtered["Time Difference mean"] >= 0]) / len(filtered)

def get_carriers(FromZIP, ToZIP):
    logger.info(f"Getting Carriers from {FromZIP} to {ToZIP}")
    df = get_data()
    if df is None:
        return pd.DataFrame({})
    final_data = TransportAnalysis(df)
    final_data = final_data[(final_data['FromZIP'] == FromZIP) & (final_data['ToZIP'] == ToZIP)].sort_values('Final Amount mean')
    final_data["No Delay Ratio"] = final_data["Carrier"].apply(lambda carrier : no_delay_ratio(final_data, carrier, FromZIP, ToZIP))
    st.session_state.carrier_data = final_data.sort_values("No Delay Ratio", ascending=False)
    return final_data.sort_values("No Delay Ratio", ascending=False)


# Getting Quotes

def safe_literal_eval(s):
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return dict()

def create_quote_db():
    quotes = []
    df = get_data()
    if df is None:
        return pd.DataFrame({})
    df["Quotes"] = df["Quotes"].apply(lambda x : safe_literal_eval(x) if not x is np.nan else dict())
    for idx, row in df.iterrows():
        for key in row["Quotes"].keys():
            
            row["Quotes"][key] = pd.Series(row["Quotes"][key]).replace('[^\d.]', '', regex=True).values[0]
            row["Quotes"][key] = int(row["Quotes"][key])
            quotes.append({
                "FromZIP": row["FromZIP"],
                "ToZIP": row["ToZIP"],
                "Carrier": key,
                "Qoute Amount": row["Quotes"][key],
            })
    st.session_state.quotes_data = pd.DataFrame(quotes)
    return pd.DataFrame(quotes)

# UI 

def apply_filters(df):
    
    if st.session_state.get("enable_filtering", False):
        filtered_df = df.copy()
        for column in df.columns:
            if st.sidebar.checkbox(f"Enable {column} Filtering", key=f"enable_{column}_filtering"):
                if is_numeric_dtype(df[column]):
                    min_value, max_value = df[column].min(), df[column].max()
                    start, end = st.sidebar.slider(f"{column} Range", min_value, max_value, (min_value, max_value))
                    filtered_df = filtered_df[(filtered_df[column] >= start) & (filtered_df[column] <= end)]
                elif is_object_dtype(df[column]):
                    all_options = df[column].unique()
                    if st.sidebar.checkbox("Select All", key=f"select_all_{column}"):
                        default = all_options
                    else:
                        default = []
                    options = st.sidebar.multiselect(f"Select {column}", all_options, default=default)
                    filtered_df = filtered_df[filtered_df[column].isin(options)]  
                    
        return filtered_df
    else:
        return df


st.set_page_config(page_title="Transport Analysis", page_icon="📄", layout="centered")

if 'enable_filtering' not in st.session_state:
    st.session_state.enable_filtering = False

# Streamlit app layout
with st.sidebar:
    selected = option_menu(
    menu_title = "Transport Analysis",
    options = ["Time Delay Analysis","Quotes",],
    icons = ["clock-history","receipt"],
    menu_icon = "truck-front-fill",
    default_index = 0,
    orientation = "horizontal",
)

st.sidebar.checkbox("Enable Filtering", key="enable_filtering")

if selected == "Time Delay Analysis":
    st.title("Time Delay Analysis")

    try: 
        
        left, right = st.columns(2)
        with left:
            from_ = st.text_input("From ZIP", key="from", placeholder="Enter From ZIP")
            to = st.text_input("To ZIP", key="to", placeholder="Enter To ZIP")
            num = st.text_input("Number of Carriers", key="num", placeholder="Enter Number of Carriers")
        with right:
            # streamlit text box
            st.text_area("Note", "Additional Insights About the data`", key="note", height=210)
        c1, c2= st.columns(2)
        c3, c4= st.columns(2)
        c5, c6= st.columns(2)

        
        if 'carrier_data' not in st.session_state:
            carrier_place = st.empty()
            with c1:
                h1 = st.empty()
                p1 = st.empty()
            
            with c2:
                h2 = st.empty()
                p2 = st.empty()
                
            with c3:
                h3 = st.empty()
                p3 = st.empty()
                
            with c4:
                h4 = st.empty()
                p4 = st.empty()
            
            with c5:
                h5 = st.empty()
                p5 = st.empty()
            
            with c6:
                h6 = st.empty()
                p6 = st.empty()
        else:   
            carrier_df = apply_filters(st.session_state.carrier_data)
            carrier_place = st.dataframe(carrier_df)
            with c1:
                h1 = st.subheader("No Delay Ratio")
                p1 = st.bar_chart(data=carrier_df, y="No Delay Ratio" , x="Carrier")
            
            with c2:
                h2 = st.subheader("Time Difference mean")
                p2 = st.bar_chart(data=carrier_df, y="Time Difference mean" , x="Carrier")
            
            with c3:
                h3 = st.subheader("Time Difference Standard Deviation")
                p3 = st.bar_chart(data=carrier_df, y="Time Difference custom_std" , x="Carrier")
            
            with c4:
                h4 = st.subheader("Final Amount mean")
                p4 = st.bar_chart(data=carrier_df, y="Final Amount mean" , x="Carrier")
            
            with c5:
                h5 = st.subheader("Total Trips")
                p5 = st.bar_chart(data=carrier_df, y="Total Trips max" , x="Carrier")
            
            with c6:
                h6 = st.subheader("Unique Routes")
                p6 = st.bar_chart(data=carrier_df, y="Unique Routes max" , x="Carrier")
            
        if st.button("Get Carriers", key="get_carriers"):
            if from_ and to and num:
                with st.spinner('Getting Carriers...'):
                    carriers = get_carriers(from_, to)
                    carrier_df = apply_filters(carriers.head(int(num)))
                    st.success(f"{len(carrier_df)} Carriers Retrived", icon="✅")
                    carrier_place.dataframe(carrier_df)
                    
                    with c1:
                        h1.subheader("No Delay Ratio")
                        p1.bar_chart(data=carrier_df, y="No Delay Ratio" , x="Carrier")
                    
                    with c2:
                        h2.subheader("Time Difference mean")
                        p2.bar_chart(data=carrier_df, y="Time Difference mean" , x="Carrier")
                    
                    with c3:
                        h3.subheader("Time Difference Standard Deviation")
                        p3.bar_chart(data=carrier_df, y="Time Difference custom_std" , x="Carrier")
                    
                    with c4:
                        h4.subheader("Final Amount mean")
                        p4.bar_chart(data=carrier_df, y="Final Amount mean" , x="Carrier")
                    
                    with c5:
                        h5.subheader("Total Trips")
                        p5.bar_chart(data=carrier_df, y="Total Trips max" , x="Carrier")
                    
                    with c6:
                        h6.subheader("Unique Routes")
                        p6.bar_chart(data=carrier_df, y="Unique Routes max" , x="Carrier")
            else:
                st.warning("Please enter all fields", icon="⚠️")

    except Exception as e:
        logger.error("Error occured while getting carriers")
        logger.error(e)
        st.error("Something went wrong, please try again later")

elif selected == "Quotes":
    st.title("All Quotes")
    left, right = st.columns(2)
    with left:
        if "quotes_data" not in st.session_state:
            df_placeholder = st.warning("Data Loading", icon="ℹ")
            df_quotes = apply_filters(create_quote_db())
            df_placeholder.dataframe(df_quotes)
        else:
            df_quotes = apply_filters(st.session_state.quotes_data)
            df_placeholder = st.dataframe(df_quotes)
    with right:
        st.text_area("Note", "Additional Insights About the data`", key="note", height=210)