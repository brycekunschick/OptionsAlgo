import requests
import pandas as pd
from datetime import datetime, timedelta
import robin_stocks as rs
import robin_stocks.robinhood as rh
import os
from dotenv import load_dotenv
import sys
from robin_stocks.robinhood.helper import *
from robin_stocks.robinhood.urls import *


# Nasdaq API

all_data = []
base_url = "https://api.nasdaq.com/....."

# Use headers to imitate browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

# Starting date param = tomorrow
today = datetime.today()
start_date = today + timedelta(days=1)

# Loop for next 30 days
for i in range(30):
    current_date = start_date + timedelta(days=i)
    date_str = current_date.strftime('%Y-%m-%d')
    url = f"{base_url}{date_str}"

    response = None # set response to none

    try:
        # Get request, response and any errors
        response = requests.get(url, headers=headers, timeout=15)

        data = response.json()

        # Process data if request successful
        if 'data' in data and 'calendar' in data['data'] and 'rows' in data['data']['calendar']:
            nd_data = data['data']['calendar']['rows']
            if nd_data:
                all_data.extend(nd_data)
                #status tracking for console
                print(f"{len(nd_data)} entries for {date_str}")
            else:
                print(f"No items for {date_str}")
        else:
            # Print if data structure is wrong
            print(f"Incorrect data structure for {date_str}.")

    except: # Catch any other unexpected errors
        print(f"Error getting data for: {date_str}: {e}")


# Dataframe for collected data
if all_data:
    df = pd.DataFrame(all_data)
    print("\nNasdaq data successfully retrieved")
    print(f"\nRecords retrieved: {len(df)}")
else:
    print("\nNo data was collected")

# Convert dates to datetime fomrat
df['first_Date'] = pd.to_datetime(df['first_Date'])
df['second_Date'] = pd.to_datetime(df['second_Date'])

# Groupby symbol, sum a_Rate, max first_Date, min second_date
aggregated_df = df.groupby('symbol').agg(
    sum_arate = ('a_Rate', 'sum'),
    last_date1 = ('first_Date', 'max'),
    first_date2 = ('second_Date', 'min')
).reset_index() # set symbol index to column

# Filter for rdates after today
today = datetime.today()
aggregated_df = aggregated_df[aggregated_df['first_rdate'] > today]




# Robinhood API

# Load env variables
load_dotenv()

# API login
username = os.getenv("RH_USERNAME")
password = os.getenv("RH_PASSWORD")

login = rh.login(username, password)
del username, password

# Empty lists to store data
all_options_df = pd.DataFrame()  # for all options

options_det_df = pd.DataFrame()  # for evaluating joined data


# Loop through each symbol in aggregated_df
for symbol in aggregated_df['symbol']:
    try:
        # Quote data for current symbol
        quote_data = rh.get_quotes(symbol)

        # Quote data empty check
        if not quote_data:
            print(f"No quote data found for: {symbol}")
            continue

        # Fix numeric columns and remove other columns
        quote_df = pd.DataFrame(quote_data)
        quote_df['ask_price'] = pd.to_numeric(quote_df['ask_price'], errors='coerce')
        quote_df = quote_df[['ask_price', 'symbol']]


        # Join quote data and agg data for symbol
        nd_quote_df = pd.merge(quote_df, aggregated_df, on='symbol', how='left')

        # Private code with logic
        

        # Check if list is empty
        if not options_list:
            print(f"No options for: {symbol}")
            continue

        # Convert list to dataframe, join other info
        tradable_options_df = pd.DataFrame(options_list)

        tradable_options_df = pd.merge(tradable_options_df, nd_quote_df, how='left', left_on='chain_symbol',right_on='symbol')

        tradable_options_df['expiration_date'] = pd.to_datetime(tradable_options_df['expiration_date'])

        # Check for missing values or '0' column
        if tradable_options_df.isnull().values.any() or '0' in tradable_options_df.columns:
            print(f"{symbol} skipped for incomplete data.")
            continue

        # Private code with logic

        # Check if filtered df is empty
        if filtered_options_df.empty:
            print(f"No options meet the criteria for: {symbol}")
            continue

        # append data to all options df
        all_options_df = pd.concat([all_options_df, filtered_options_df], ignore_index=True)

        # Get id for each contract
        ids = filtered_options_df['id'].tolist()

        # split ids into batches of 200
        batch_size = 200
        id_batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]

        # empty list for results
        all_results = []

        # Get request for each batch
        for batch in id_batches:
            concatenated_ids = '%2C'.join(batch)
            url = f"https://api.robinhood.com/marketdata/options/?ids={concatenated_ids}"

            try:
                data = request_get(url, 'results')
                all_results.extend(data)
            except Exception as e:
                print(f"An error occurred while fetching data: {e}")

        # Correct occasional issues with json format
        if all_results and isinstance(all_results[0], dict) and '0' in all_results[0]:
            # take dict from 0 column
            all_results = [row['0'] for row in all_results if isinstance(row, dict) and '0' in row]

        # Remove empty rows
        all_results = [row for row in all_results if row]

        loop_df = pd.DataFrame(all_results)

        loop_df = pd.merge(loop_df, filtered_options_df, how="inner", left_on="instrument_id", right_on="id")

        loop_df['ask_price_x'] = loop_df['ask_price_x'].astype(float)
        loop_df['strike_price'] = loop_df['strike_price'].astype(float)

        # Option market data
        loop_df['o_strike'] = loop_df['strike_price']
        loop_df['o_expir'] = loop_df['expiration_date']
        loop_df['o_ask'] = loop_df['ask_price_x']
        loop_df['o_bid'] = loop_df['bid_price']


        # Private code with logic


        options_det_df = pd.concat([options_det_df, loop_df], ignore_index=True)

        print(f"\n{len(loop_df)} contracts found for: {symbol}")

    except Exception as e:
        print(f"An error occurred for {symbol}: {e}")
        continue

# Sort by desc return
options_det_df = options_det_df.sort_values(by='trade_score', ascending=False)

# Select columns
columns_to_keep = [
    # Private
]

options_det_df2 = options_det_df[columns_to_keep]

# Dynamic filename
current_time = datetime.now().strftime("%Y-%m-%d %H%M%S")
filename = f"data/options_data_{current_time}.csv"

# Save as csv
options_det_df2.to_csv(filename, index=False)
print(f"File saved: {filename}")