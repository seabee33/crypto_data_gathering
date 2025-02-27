import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import warnings
from pandas.errors import PerformanceWarning
import re

warnings.simplefilter(action='ignore', category=PerformanceWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)


# Fetch data from j_raw table
def fetch_j_raw_scp_agg(conn):
    query = """
    
SELECT 
    project_name,
    datestamp,
    sector,
    active_addresses_weekly,
    active_developers,
    active_loans,
    address_density,
    average_fee_per_address,
    avg_txn_fee,
    circulating_supply,
    daa,
    daa_over_100,
    daa_per_tvl,
    daily_fees_native,
    dex_density,
    dex_volume,
    dex_volume_per_address,
    dl_chain_fees_1000,
    dl_chain_fees_raw,
    dl_dapp_count_1000,
    dl_dapp_count_raw,
    earnings,
    fdmc,
    fee_density,
    fees,
    fees_per_tvl,
    gross_profit,
    maa,
    mc,
    network_value_to_address_ratio,
    network_value_to_dex_ratio,
    network_value_to_fee_ratio,
    network_value_to_revenue_ratio,
    network_value_to_stablecoin_ratio,
    network_value_to_tvl_ratio,
    p2p_swap_count,
    price,
    revenue,
    sr_active_validators,
    sr_annualized_rewards_usd,
    sr_circulating_percentage,
    sr_circulating_supply,
    sr_daily_trading_volume,
    sr_delegated_tokens,
    sr_inflation_rate,
    sr_real_reward_rate,
    sr_reward_rate,
    sr_staked_tokens,
    sr_staking_marketcap,
    sr_staking_ratio,
    sr_total_staking_wallets,
    sr_total_validators,
    stablecoin_mc,
    stablecoin_transfer_volume,
    stablecoin_value_transfer_per_address,
    stablecoin_velocity,
    stablecoing_density,
    supply_overhang,
    token_incentives,
    token_supply_circulating,
    token_supply_maximum,
    tokenholders,
    transactions,
    tvl,
    tvl_density,
    tvl_per_address,
    volume_24h_usd,
    weekly_commits_core,
    weekly_commits_sub,
    weekly_contract_deployers,
    weekly_contracts_deployed,
    weekly_dev_core,
    weekly_dev_sub,
    weekly_unique_contract_deployers
FROM f_scp_assets_metrics
ORDER BY datestamp DESC;
    """
    df_tt = pd.read_sql(query, conn)
    return df_tt

def convert_df_remove_btc_scp_agg(df):    
    # Convert 'datestamp' to datetime format
    df['datestamp'] = pd.to_datetime(df['datestamp'], errors='coerce')

    # Identify non-numeric and numeric columns
    non_numeric_cols = ['project_name', 'sector', 'datestamp']
    numeric_cols = [col for col in df.columns if col not in non_numeric_cols]

    # Convert all numeric columns to numeric types and fill NaNs with 0
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Ensure 'project_name' and 'datestamp' exist
    required_cols = ['project_name', 'datestamp']
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Required column '{col}' not found in DataFrame.")

    # Convert 'project_name' to string to avoid issues during grouping
    df['project_name'] = df['project_name'].astype(str)
    df = df[df["project_name"] != "Bitcoin"]
    return df

def group_by_df_scp_agg(df):
    # Define the columns to drop
    columns_to_drop = ['project_name', 'sector']


    # Define base metrics (columns with raw data)
    # List of columns that are considered "base metrics" to be aggregated via summing.
    columns_to_sum = [
        'active_addresses_weekly',
        'active_developers',
        'active_loans',
        'circulating_supply',
        'daa',
        'daa_over_100',
        'dex_volume',
        'earnings',
        'fdmc',
        'fees',
        'gross_profit',
        'maa',
        'mc',
        'price',
        'revenue',
        'stablecoin_mc',
        'stablecoin_transfer_volume',
        'token_incentives',
        'token_supply_maximum',
        'tokenholders',
        'transactions',
        'tvl',
        'volume_24h_usd',
        'weekly_commits_core',
        'weekly_commits_sub',
        'weekly_contract_deployers',
        'weekly_contracts_deployed',
        'weekly_dev_core',
        'weekly_dev_sub',
        'weekly_unique_contract_deployers',
        'sr_active_validators',
        'sr_annualized_rewards_usd',
        'sr_daily_trading_volume',
        'sr_delegated_tokens',
        'sr_staked_tokens',
        'sr_staking_marketcap',
        'sr_total_staking_wallets',
        'sr_total_validators',
        'dl_chain_fees_1000',
        'dl_dapp_count_1000',
        'dl_chain_fees_raw',
        'dl_dapp_count_raw',
        'p2p_swap_count',
        'token_supply_circulating',
        'sr_circulating_supply'
    ]
    # List of columns that are ratios or densities and thus need to be aggregated via weighted averages.
    columns_to_average = [
        'address_density',
        'average_fee_per_address',
        'avg_txn_fee',
        'daa_per_tvl',
        'daily_fees_native',
        'dex_density',
        'dex_volume_per_address',
        'fee_density',
        'fees_per_tvl',
        'network_value_to_address_ratio',
        'network_value_to_dex_ratio',
        'network_value_to_fee_ratio',
        'network_value_to_revenue_ratio',
        'network_value_to_stablecoin_ratio',
        'network_value_to_tvl_ratio',
        'sr_circulating_percentage',
        'sr_inflation_rate',
        'sr_real_reward_rate',
        'sr_reward_rate',
        'sr_staking_ratio',
        'stablecoin_value_transfer_per_address',
        'stablecoin_velocity',
        'stablecoing_density',
        'supply_overhang',
        'tvl_density',
        'tvl_per_address'
    ]
    # First, group by 'datestamp' and calculate the sum for the columns that need summing
    df_grouped = df.groupby('datestamp').agg({col: 'sum' for col in columns_to_sum})

    # Calculate the count of distinct project_name for each datestamp
    df_grouped['distinct_project_count'] = df.groupby('datestamp')['project_name'].nunique()

    # Now, calculate the weighted averages using the 'mc' (market cap) column
    # We directly use the grouped market cap for efficiency
    for col in columns_to_average:
        df_grouped[col] = (df[col] * df['mc']).groupby(df['datestamp']).sum() / df.groupby('datestamp')['mc'].sum()

    # Drop the unnecessary columns
    df_grouped = df_grouped.drop(columns=columns_to_drop, errors='ignore')

    # Reset index to have 'datestamp' back as a column
    df_grouped = df_grouped.reset_index()
    
    return df_grouped

def add_ma_and_percentage_changes_scp_agg(df, ma_windows=[7, 14]):
    # Loop through all columns except 'datestamp'
    for col in df.columns:
        if col in ('datestamp', 'distinct_project_count'):
            continue  # Skip the 'datestamp' column

        # Add Moving Averages (MA)
        if col not in ('datestamp', 'distinct_project_count', 'address_density',
    'average_fee_per_address',
    'avg_txn_fee',
    'daa_per_tvl',
    'daily_fees_native',
    'dex_density',
    'dex_volume_per_address',
    'fee_density',
    'fees_per_tvl',
    'network_value_to_address_ratio',
    'network_value_to_dex_ratio',
    'network_value_to_fee_ratio',
    'network_value_to_revenue_ratio',
    'network_value_to_stablecoin_ratio',
    'network_value_to_tvl_ratio',
    'sr_circulating_percentage',
    'sr_inflation_rate',
    'sr_real_reward_rate',
    'sr_reward_rate',
    'sr_staking_ratio',
    'stablecoin_value_transfer_per_address',
    'stablecoin_velocity',
    'stablecoing_density',
    'supply_overhang',
    'tvl_density',
    'tvl_per_address'):
            for window in ma_windows:
                ma_col_name = f'{col}_MA_{window}'
                df[ma_col_name] = df[col].rolling(window=window).mean()

    return df


def save_to_fact_table_agg_scp_agg(conn):
    df = fetch_j_raw_scp_agg(conn)  # Fetch the data
    print("Data Fetched")
    df = convert_df_remove_btc_scp_agg(df)
    print("DF converted")
    df_group_by = group_by_df_scp_agg(df)
    print("Group By")
    df_final = add_ma_and_percentage_changes_scp_agg(df_group_by)

    # Convert column names to lowercase and replace spaces with underscores
    df_final.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_final.columns]

    # Ensure the first three columns are ordered, and the rest sorted alphabetically
    ordered_columns = [ 'datestamp'] + sorted(
        [col for col in df_final.columns if col not in [ 'datestamp']]
    )
    df_final = df_final[ordered_columns]
    
    
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
            update_cols = ", ".join([f"{col}=VALUES({col})" for col in cols if col not in ['datestamp', 'project_name']])

            data = [tuple(x) for x in df_final.to_numpy()]
            for i in range(0, len(data), batch_size):
                cursor.executemany(
                    f"""
                    INSERT INTO f_scp_agg_metrics ({cols_str}) 
                    VALUES ({placeholders}) 
                    ON DUPLICATE KEY UPDATE {update_cols}
                    """, 
                    data[i:i + batch_size]
                )
            conn.commit()
            print("Data successfully inserted into f_scp_agg_metrics")
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        conn.rollback()
