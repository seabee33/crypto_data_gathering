import requests, pandas as pd, os, numpy as np, mysql.connector
from mysql.connector import Error
from m_functions import *
from dotenv import load_dotenv

load_dotenv()

cq_api_key = os.getenv("CRYPTO_QUANT_API_KEY")


# API Key
BASE_URL = 'https://api.cryptoquant.com/v1'


def cq_btc_test_connection():
    url = f"{BASE_URL}/btc/status/entity-list?type=exchange"
    headers = {
        'Authorization': f"Bearer {cq_api_key}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        # Check if the request was successful
        if response.status_code == 200:
            return "Connection successful."
        else:
            return f"Connection failed. Status Code: {response.status_code}, Response: {response.text}"
    except Exception as e:
        return f"An error occurred: {e}"



    
def cq_btc_fetch_cryptoquant_data():
    # List of endpoints to call
    cq_btc_endpoints = [
        "/btc/market-data/price-ohlcv?window=day&market=spot&exchange=all_exchange&symbol=btc_usd&limit=100000",
        "/btc/market-indicator/realized-price?window=day&limit=100000",
        "/btc/market-data/capitalization?window=day&limit=100000",
        "/btc/network-data/supply?window=day&limit=100000",
        "/btc/network-data/addresses-count?window=day&limit=100000",
        "/btc/network-data/utxo-count?window=day&limit=100000",
        "/btc/network-data/transactions-count?window=day&limit=100000",
        "/btc/network-data/tokens-transferred?window=day&limit=100000",
        "/btc/network-data/fees?window=day&limit=100000",
        "/btc/network-data/fees-transaction?window=day&limit=100000",
        "/btc/network-data/block-bytes?window=day&limit=100000",
        "/btc/network-data/hashrate?window=day&limit=100000",
        "/btc/network-data/blockreward?window=day&limit=100000",
        "/btc/market-indicator/mvrv?window=day&limit=100000",
        "/btc/market-indicator/sopr-ratio?window=day&limit=100000",
        "/btc/network-indicator/nupl?window=day&limit=100000",
        "/btc/network-indicator/pnl-supply?window=day&limit=100000",
        "/btc/network-indicator/puell-multiple?window=day&limit=100000",
        "/btc/market-data/open-interest?window=day&exchange=all_exchange&symbol=all_symbol&limit=100000",
        "/btc/market-data/funding-rates?window=day&exchange=all_exchange&limit=100000"
    ]
    headers = {
        'Authorization': f"Bearer {cq_api_key}"
    }
    
    data = {}

    for endpoint in cq_btc_endpoints:
        url = f"{BASE_URL}{endpoint}"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                # Store the response JSON data
                data[endpoint] = response.json()
            else:
                data[endpoint] = f"Error {response.status_code}: {response.text}"
        except Exception as e:
            data[endpoint] = f"An error occurred: {e}"

    return data

def cq_btc_create_merged_dataframe_simplified(cryptoquant_data):
    # Dictionary to hold DataFrames for each endpoint
    df_dict = {}

    # Loop through the cryptoquant_data dictionary
    for endpoint, result in cryptoquant_data.items():
        if 'result' in result and 'data' in result['result']:
            # Create a DataFrame from the data list
            df = pd.DataFrame(result['result']['data'])
            # Set 'date' as the index if it's present
            if 'date' in df.columns:
                df.set_index('date', inplace=True)
            # Store the DataFrame in the dictionary
            df_dict[endpoint] = df

    # Merge all DataFrames on the 'date' index
    merged_df = pd.concat(df_dict.values(), axis=1, join='outer').reset_index()
    
    return merged_df

def cq_btc_rename_and_filter_columns(merged_df):
    # Define the column mapping based on the provided image and description
    column_mapping = {
        'date': 'datestamp',
        'blockreward_usd': 'block_reward',
        'block_bytes': 'block_size',
        'supply_total': 'circulating_supply',
        'addresses_count_active': 'daily_active_addresses',
        'fees_total': 'daily_fees',
        'fees_total_usd': 'daily_fees_usd',
        'transactions_count_total': 'daily_transactions',
        'tokens_transferred_total': 'daily_transfer_volume',
        'hashrate': 'hash_rate',
        'sopr_ratio': 'lth_sopr',
        'market_cap': 'mc',
        'fees_transaction_median': 'median_fees',
        'fees_transaction_median_usd': 'median_fees_usd',
        'tokens_transferred_median': 'median_transfer_size',
        'mvrv': 'mvrv',
        'supply_new': 'new_supply',
        'nupl': 'nupl',
        'funding_rates': 'perp_futures_funding_rate',
        'open_interest': 'perp_futures_open_interest',
        'close': 'price',
        'puell_multiple': 'puell_multiple',
        'realized_price': 'realized_price',
        'profit_percent': 'supply_in_profit',
        'utxo_count': 'total_utxos'
    }

    # Filter the columns to keep only those in the column_mapping
    filtered_df = merged_df[list(column_mapping.keys())]

    # Rename the columns
    renamed_filtered_df = filtered_df.rename(columns=column_mapping)
    
    return renamed_filtered_df


def cq_btc_calculate_specific_metrics(df):
    # Copy the DataFrame to avoid modifying the original
    temp_df = df.copy()
    # Sort by datestamp in ascending order to calculate rolling windows correctly
    temp_df = temp_df.sort_values(by='datestamp')
    # Update hashrate    
    temp_df['hash_rate'] = temp_df['hash_rate'] / 1000000
    # Perp futures funding rate: annualized by multiplying by 365
    temp_df['perp_futures_funding_rate'] = temp_df['perp_futures_funding_rate'] * 365
    # Inflation: new_supply (14-day avg) * 365 / circulating_supply
    temp_df['inflation'] = (temp_df['new_supply'].rolling(window=14, min_periods=1).mean() * 365) / temp_df['circulating_supply']
    # Daily transfer volume (USD): daily_transfer_volume * price
    temp_df['daily_transfer_volume_usd'] = temp_df['daily_transfer_volume'] * temp_df['price']
    # Annualised transfer volumes (USD): daily_transfer_volume_usd (30-day sum) * 12
    temp_df['annualised_transfer_volumes_usd'] = temp_df['daily_transfer_volume_usd'].rolling(window=30, min_periods=1).sum() * 12
    # Median transfer size (USD): median_transfer_size * price
    temp_df['median_transfer_size_usd'] = temp_df['median_transfer_size'] * temp_df['price']
    # Annualised fees (30-day): daily_fees (30-day sum) * 12
    temp_df['annualised_fees_30_day'] = temp_df['daily_fees'].rolling(window=30, min_periods=1).sum() * 12
    # Annualised fees (30-day - USD): daily_fees_usd (30-day sum) * 12
    temp_df['annualised_fees_30_day_usd'] = temp_df['daily_fees_usd'].rolling(window=30, min_periods=1).sum() * 12
    # Network value to address (NVA) ratio: mc / daily_active_addresses (14-day avg)
    temp_df['nva_ratio'] = temp_df['mc'] / temp_df['daily_active_addresses'].rolling(window=14, min_periods=1).mean()
    # Network value to transactions (NVT): mc / daily_transfer_volume_usd (90-day avg)
    temp_df['nvt'] = temp_df['mc'] / temp_df['daily_transfer_volume_usd'].rolling(window=90, min_periods=1).mean()

    return temp_df

def cq_btc_add_moving_averages(df):
    # Copy the DataFrame to avoid modifying the original
    temp_df = df.copy()

    # Sort by datestamp in ascending order to calculate rolling windows correctly
    temp_df = temp_df.sort_values(by='datestamp')

    # Dictionary to store all the moving average columns
    ma_dict = {}

    # Apply 7-day, 14-day, 30-day, and 90-day moving averages for all numeric columns
    for col in temp_df.select_dtypes(include=['float64', 'int64']).columns:
        if col != 'datestamp':
            ma_dict[f'{col}_ma_7'] = temp_df[col].rolling(window=7, min_periods=1).mean()
            ma_dict[f'{col}_ma_14'] = temp_df[col].rolling(window=14, min_periods=1).mean()
            ma_dict[f'{col}_ma_30'] = temp_df[col].rolling(window=30, min_periods=1).mean()
            ma_dict[f'{col}_ma_90'] = temp_df[col].rolling(window=90, min_periods=1).mean()

    # Convert the dictionary to a DataFrame
    ma_df = pd.DataFrame(ma_dict, index=temp_df.index)

    # Concatenate the original DataFrame with the moving averages DataFrame
    temp_df = pd.concat([temp_df, ma_df], axis=1)

    return temp_df


def cq_btc_calculate_z_scores(df):
    # Copy the DataFrame to avoid modifying the original
    temp_df = df.copy()

    # Sort by datestamp in ascending order to calculate rolling windows correctly
    temp_df = temp_df.sort_values(by='datestamp')

    # Z-score calculations using the moving averages
    # MA 14 columns for Z-score
    for z_column in ['mvrv', 'lth_sopr', 'nupl', 'puell_multiple']:
        temp_df[f'{z_column}_ma_14_z_score'] = (
            temp_df[f'{z_column}_ma_14'] - temp_df[f'{z_column}_ma_14'].rolling(window=365, min_periods=1).mean()
        ) / temp_df[f'{z_column}_ma_14'].rolling(window=365, min_periods=1).std()

    # MA 7 columns for Z-score
    for z_column in ['perp_futures_open_interest', 'perp_futures_funding_rate']:
        temp_df[f'{z_column}_ma_7_z_score'] = (
            temp_df[f'{z_column}_ma_7'] - temp_df[f'{z_column}_ma_7'].rolling(window=365, min_periods=1).mean()
        ) / temp_df[f'{z_column}_ma_7'].rolling(window=365, min_periods=1).std()

    # Z-score for NVA ratio MA 14 using a rolling 14-day window
    temp_df['nva_ratio_ma_14_z_score'] = (
        temp_df['nva_ratio_ma_14'] - temp_df['nva_ratio_ma_14'].rolling(window=365, min_periods=1).mean()
    ) / temp_df['nva_ratio_ma_14'].rolling(window=365, min_periods=1).std()

    temp_df['nvt_ma_90_z_score'] = (
        temp_df['nvt_ma_90'] - temp_df['nvt_ma_90'].rolling(window=365, min_periods=1).mean()
    ) / temp_df['nvt_ma_90'].rolling(window=365, min_periods=1).std()

    # Normal Z-score calc.
    temp_df['nvt_ma_14_z_score'] = (
        temp_df['nvt_ma_14'] - temp_df['nvt_ma_14'].rolling(window=365, min_periods=1).mean()
    ) / temp_df['nvt_ma_14'].rolling(window=365, min_periods=1).std()

    return temp_df

def cq_btc_add_pct_rank_columns(df):
    # Copy the DataFrame to avoid modifying the original
    temp_df = df.copy()

    # Filter numeric columns, excluding non-numeric and 'datestamp'
    numeric_columns_to_rank = temp_df.select_dtypes(include=['float64', 'int64']).columns.difference(['datestamp'])

    # Dictionary to store the percentile rank columns
    rank_dict = {}

    # Calculate historical ranks for all columns at once
    for col in numeric_columns_to_rank:
        # Calculate the historical percentile rank once
        col_rank = temp_df[col].rank(pct=True, na_option='keep')
        rank_dict[f'{col}_historical_pct_rank'] = col_rank

        # Calculate the rolling 4-year rank using a faster approach
        rolling_window = temp_df[col].rolling(window=1460, min_periods=1)

        # Use numpy to calculate the rank of the last element in the rolling window
        rank_dict[f'{col}_4y_pct_rank'] = rolling_window.apply(
            lambda x: np.sum(x < x[-1]) / len(x) if len(x) > 1 and not np.isnan(x[-1]) else np.nan, raw=True
        )

    # Convert the dictionary to a DataFrame
    rank_df = pd.DataFrame(rank_dict, index=temp_df.index)

    # Concatenate the original DataFrame with the rank DataFrame
    temp_df = pd.concat([temp_df, rank_df], axis=1)

    # Return the modified DataFrame with the new rank columns
    return temp_df

def cq_btc_reorder_columns(df):
    # Set the 'Date' column to be first and then sort other columns alphabetically
    cols = ['datestamp'] + sorted([col for col in df.columns if col != 'datestamp'])
    return df[cols]

def cq_btc_cq_update_bitcoin(conn):
    # Fetch the data from all endpoints
    cq_btc_cryptoquant_data = cq_btc_fetch_cryptoquant_data()
    print("Data Fetched")
    # Create the merged DataFrame
    merged_df = cq_btc_create_merged_dataframe_simplified(cq_btc_cryptoquant_data)
    # 1 Rename columns:
    renamed_filtered_df = cq_btc_rename_and_filter_columns(merged_df)
    # 2 Add calculations:
    metrics_df = cq_btc_calculate_specific_metrics(renamed_filtered_df)
    # 3 Add moving average:
    metrics_MA_df = cq_btc_add_moving_averages(metrics_df)
    # 4 Add Z-Score:
    metrics_MA_Zscore_df = cq_btc_calculate_z_scores(metrics_MA_df)
    # 5 Add percentile rank:
    enhanced_df_with_ranks = cq_btc_add_pct_rank_columns(metrics_MA_Zscore_df)
    print("Calculations added")
    # Reorder columns and save
    df_final = cq_btc_reorder_columns(enhanced_df_with_ranks)
    
    # Replace NaN, inf, and -inf values with None for SQL NULL handling
    df_final.replace([np.inf, -np.inf], None, inplace=True)
    df_final = df_final.replace({pd.NaT: None, np.nan: None}) 

    batch_size = 1000

    try:
        with conn.cursor() as cursor:
            # Get column names
            cols = df_final.columns
            cols_str = ", ".join([f"`{col}`" for col in cols])  # Add backticks for safety with column names
            placeholders = ", ".join(['%s'] * len(cols))
            update_cols = ", ".join([f"{col}=VALUES({col})" for col in cols if col not in ['datestamp']])

            data = [tuple(x) for x in df_final.to_numpy()]
            for i in range(0, len(data), batch_size):
                cursor.executemany(
                    f"""
                    INSERT INTO cq_bitcoin_data ({cols_str}) 
                    VALUES ({placeholders}) 
                    ON DUPLICATE KEY UPDATE {update_cols}
                    """, 
                    data[i:i + batch_size]
                )
            conn.commit()
            print("Data successfully inserted into F_Metrics_Universe")
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        conn.rollback()

