# import psycopg2
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import warnings
from pandas.errors import PerformanceWarning
import re
from m_functions import *


warnings.simplefilter(action='ignore', category=PerformanceWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Fetch data from j_raw table
def fetch_j_raw(conn):
    query = """
    SELECT 
    project_name,
    datestamp,
    sector,
    avg_txn_fee,
    active_developers,
    active_addresses_weekly,
    active_loans,
    circulating_supply,
    daa,
    daa_over_100,
    dex_volume,
    earnings,
    fees,
    fdmc,
    gross_profit,
    maa,
    mc,
    price,
    revenue,
    stablecoin_mc,
    stablecoin_transfer_volume,
    transactions,
    tokenholders,
    token_supply_circulating,
    token_incentives,
    token_supply_maximum,
    tvl,
    volume_24h_usd,
    weekly_commits_core,
    weekly_commits_sub,
    weekly_dev_core,
    weekly_dev_sub,
    weekly_contracts_deployed,
    weekly_contract_deployers,
    weekly_unique_contract_deployers
    FROM j_raw
    WHERE sector in ('blockchains-l1', 'blockchains-l2')
    ORDER BY datestamp DESC;
    """
    df_tt = pd.read_sql(query, conn)
    return df_tt

# Calculate metrics based on fetched data
def calculate_metrics(df):
    df = df.sort_values(by=['project_name', 'datestamp']).reset_index(drop=True)
    grouped = df.groupby('project_name')

    df['fees_mean_90d'] = grouped['fees'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['daa_mean_90d'] = grouped['daa'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['fdmc_mean_90d'] = grouped['fdmc'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['active_addresses_weekly_mean_90d'] = grouped['active_addresses_weekly'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['dex_volume_mean_90d'] = grouped['dex_volume'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['tvl_mean_90d'] = grouped['tvl'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    
    df['fees_sum_90d'] = grouped['fees'].rolling(window=90, min_periods=1).sum().reset_index(level=0, drop=True)
    df['dex_volume_sum_90d'] = grouped['dex_volume'].rolling(window=90, min_periods=1).sum().reset_index(level=0, drop=True)
    df['stablecoin_transfer_volume_mean_90d'] = grouped['stablecoin_transfer_volume'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)

    df['Fee_Value_Density'] = (df['fees_mean_90d'] * 90 / 365) / df['fdmc']
    df['User_Value_Density'] = df['active_addresses_weekly_mean_90d'] / df['fdmc']

    for period in [30, 90]:
        avg_fees_last = grouped['fees'].rolling(window=period, min_periods=1).mean().reset_index(level=0, drop=True)
        avg_fees_prev = avg_fees_last.groupby(df['project_name']).shift(period)
        avg_market_cap_last = grouped['fdmc'].rolling(window=period, min_periods=1).mean().reset_index(level=0, drop=True)
        avg_market_cap_prev = avg_market_cap_last.groupby(df['project_name']).shift(period)
        fee_growth = (avg_fees_last - avg_fees_prev) / avg_fees_prev.replace(0, np.nan)
        market_cap_growth = (avg_market_cap_last - avg_market_cap_prev) / avg_market_cap_prev.replace(0, np.nan)
        df[f'Real_Fee_Growth_{period}D'] = (fee_growth - market_cap_growth).fillna(0)

    for period in [30, 90]:
        tvl_last = df['tvl']
        tvl_prev = grouped['tvl'].shift(period)
        market_cap_last = df['fdmc']
        market_cap_prev = grouped['fdmc'].shift(period)
        tvl_growth = (tvl_last - tvl_prev) / tvl_prev.replace(0, np.nan)
        market_cap_growth = (market_cap_last - market_cap_prev) / market_cap_prev.replace(0, np.nan)
        df[f'Real_TVL_Growth_{period}D'] = (tvl_growth - market_cap_growth).fillna(0)

    df['Supply_Overhang'] = df['mc'] / df['fdmc']
    df['DEX_Value_Density'] = df['dex_volume_mean_90d'] / df['fdmc']
    df['Stablecoing_Density'] = df['stablecoin_mc'] / df['fdmc']
    df['TVL_Density'] = df['tvl'] / df['fdmc']
    df['Average_Fee_per_User'] = df['fees_mean_90d'] / df['daa_mean_90d']
    df['DEX_Volume_per_User'] = df['dex_volume_mean_90d'] / df['daa_mean_90d']
    df['TVL_per_User'] = df['tvl_mean_90d'] / df['daa_mean_90d']
    df['Stablecoin Value Transfer-per-User'] = df['fdmc'] / (df['fees_sum_90d'] * (90 / 365))
    df['Network_Value_to_Adresses_Ratio'] = df['fdmc'] / df['daa_mean_90d']
    df['Network_Value_to_DEX_Ratio'] = df['fdmc'] / (df['dex_volume_sum_90d'] * (90 / 365))
    df['Network_Value_to_Stablecoin_Ratio'] = df['stablecoin_transfer_volume_mean_90d'] / df['daa_mean_90d']
    df['Network_Value_to_TVL_Ratio'] = df['fdmc'] / df['tvl']
    df['Stablecoin_Velocity'] = (df['stablecoin_transfer_volume_mean_90d'] * (90 / 365)) / df['mc']

    df.drop(columns=['fees_mean_90d', 'fdmc_mean_90d', 'active_addresses_weekly_mean_90d', 'daa_mean_90d',
                     'dex_volume_mean_90d', 'tvl_mean_90d', 'fees_sum_90d', 'dex_volume_sum_90d', 
                     'stablecoin_transfer_volume_mean_90d'], inplace=True)

    return df

# Add moving averages and percentage changes
def add_moving_averages(df):
    # Sort the dataframe by 'project_name' and 'datestamp'
    df = df.sort_values(by=['project_name', 'datestamp']).reset_index(drop=True)
    
    # Define periods for moving averages and percentage changes
    periods_ma = [7, 14, 30, 60, 90, 180, 365]
    periods_pc = [7, 30, 90, 180, 365, 730]
    
    # Select all columns except 'project_name', 'datestamp', and 'sector'
    cols_to_process = [col for col in df.columns if col not in ['project_name', 'datestamp', 'sector']]
    
    # Group the data by 'project_name'
    grouped = df.groupby('project_name')

    # Iterate over columns to process
    for col in cols_to_process:
        # Calculate moving averages
        for period in periods_ma:
            df[f'{col} MA {period}D'] = grouped[col].transform(lambda x: x.rolling(window=period, min_periods=1).mean())
        # Calculate percentage changes
        for period in periods_pc:
            df[f'{col} Perc_Change {period}D'] = grouped[col].transform(lambda x: x.pct_change(periods=period) * 100)

    return df

# Calculate momentum based on percentage changes
def calculate_momentum(df):
    base_columns = [
        'avg_txn_fee', 'active_developers', 'active_addresses_weekly', 'active_loans', 'circulating_supply', 'daa', 
        'daa_over_100', 'dex_volume', 'earnings', 'fees', 'fdmc', 'gross_profit', 'maa', 'mc', 'price', 'revenue', 
        'stablecoin_mc', 'stablecoin_transfer_volume', 'transactions', 'tokenholders', 'token_supply_circulating', 
        'token_incentives', 'token_supply_maximum', 'tvl', 'volume_24h_usd', 'weekly_commits_core', 'weekly_commits_sub', 
        'weekly_dev_core', 'weekly_dev_sub', 'weekly_contracts_deployed', 'weekly_contract_deployers', 
        'weekly_unique_contract_deployers'
    ]

    # Calculate momentum for each base column
    for col in base_columns:
        # Check if percentage change columns exist for each period (30D, 90D, 180D, 365D)
        if all([f'{col} Perc_Change {period}D' in df.columns for period in [30, 90, 180, 365]]):
            df[f'{col} Momentum'] = (
                df[f'{col} Perc_Change 30D'] +
                df[f'{col} Perc_Change 90D'] +
                df[f'{col} Perc_Change 180D'] +
                df[f'{col} Perc_Change 365D']
            ) / 4
    
    return df

def save_to_fact_table(conn):
    df = fetch_j_raw(conn)  # Fetch the data
    print("Data Fetched")
    df = calculate_metrics(df)  # Calculate metrics
    print("Metrics Added")
    df = add_moving_averages(df)  # Add moving averages and percentage changes
    print("MA/Perc change added")
    df_final = calculate_momentum(df)
    print("Momentum added")

    # Convert column names to lowercase and replace spaces with underscores
    df_final.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_final.columns]

    # Ensure the first three columns are ordered, and the rest sorted alphabetically
    ordered_columns = ['project_name', 'datestamp', 'sector'] + sorted(
        [col for col in df_final.columns if col not in ['project_name', 'datestamp', 'sector']]
    )
    df_final = df_final[ordered_columns]
    
    # Convert 'datestamp' to datetime and filter by the last 30 days
    # df_final['datestamp'] = pd.to_datetime(df_final['datestamp'])
    # last_30_days = pd.Timestamp.now() - pd.DateOffset(days=30)
    # df_final = df_final[df_final['datestamp'] >= last_30_days]
    
    # Replace NaN, inf, and -inf values with None for SQL NULL handling
    df_final.replace([np.inf, -np.inf], None, inplace=True)
    df_final = df_final.replace({pd.NaT: None, np.nan: None})   

    # udb(conn, "update", "F_Metrics_Universe", 2, df_final)

    print("Updated f table ")
    
    batch_size = 1000

    try:
        with conn.cursor() as cursor:
            # Get column names
            cols = df_final.columns
            cols_str = ", ".join([f"`{col}`" for col in cols])  # Add backticks for safety with column names
            placeholders = ", ".join(['%s'] * len(cols))
            update_cols = ", ".join([f"{col}=VALUES({col})" for col in cols if col not in ['datestamp', 'project_name']])

            data = [tuple(x) for x in df_final.to_numpy()]
            for i in range(0, len(data), batch_size):
                cursor.executemany(
                    f"""
                    INSERT INTO F_Metrics_Universe ({cols_str}) 
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
