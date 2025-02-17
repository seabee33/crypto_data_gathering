import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import warnings
from pandas.errors import PerformanceWarning
import re

warnings.simplefilter(action='ignore', category=PerformanceWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

def fetch_j_raw_defi(conn):
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
    weekly_unique_contract_deployers,
    sr_active_validators,
    sr_annualized_rewards_usd,
    sr_circulating_percentage,
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
    dl_chain_fees_1000,
    dl_dapp_count_1000
    FROM j_raw
    WHERE project_name IN ('Aerodrome', 'Pancakeswap', 'Jupiter','Sushiswap','Uniswap',
        'Aave','Compound','Lido-finance', 'Makerdao','Pendle','Synthetix','1inch','Curve',
        'Dydx','Osmosis','Jito','Rocket-pool','0x','Thorchain','JustLend DAO','UMA',
        'WOO','Raydium','Ondo Finance','Aevo')

    ORDER BY datestamp DESC;
    """
    df_tt = pd.read_sql(query, conn)
    return df_tt


def calculate_metrics_defi(df):
    df = df.sort_values(by=['project_name', 'datestamp']).reset_index(drop=True)
    grouped = df.groupby('project_name')

    # Rolling calculations with checks for valid values
    df['fees_mean_90d'] = grouped['fees'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['daa_mean_90d'] = grouped['daa'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['fdmc_mean_90d'] = grouped['fdmc'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['active_addresses_weekly_mean_90d'] = grouped['active_addresses_weekly'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['dex_volume_mean_90d'] = grouped['dex_volume'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    df['tvl_mean_90d'] = grouped['tvl'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)
    
    df['fees_sum_90d'] = grouped['fees'].rolling(window=90, min_periods=1).sum().reset_index(level=0, drop=True)
    df['dex_volume_sum_90d'] = grouped['dex_volume'].rolling(window=90, min_periods=1).sum().reset_index(level=0, drop=True)
    df['stablecoin_transfer_volume_mean_90d'] = grouped['stablecoin_transfer_volume'].rolling(window=90, min_periods=1).mean().reset_index(level=0, drop=True)

    # Metrics updated to 30-day rolling windows
    df['daa_mean_30d'] = grouped['daa'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)
    df['fees_sum_30d'] = grouped['fees'].rolling(window=30, min_periods=1).sum().reset_index(level=0, drop=True)
    df['tvl_sum_30d'] = grouped['tvl'].rolling(window=30, min_periods=1).sum().reset_index(level=0, drop=True)
    df['dex_volume_sum_30d'] = grouped['dex_volume'].rolling(window=30, min_periods=1).sum().reset_index(level=0, drop=True)
    df['stablecoin_transfer_volume_mean_30d'] = grouped['stablecoin_transfer_volume'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)


    # Metrics using 30-day rolling windows
    df['Network_Value_to_Fee_Ratio'] = np.where(df[['fees_sum_30d', 'fdmc']].notna().all(axis=1) & (df['fees_sum_30d'] != 0),
                                                (df['fdmc'] / df['fees_sum_30d']) * (30 / 365), np.nan)
    df['Network_Value_to_address_Ratio'] = np.where(df[['daa_mean_30d', 'fdmc']].notna().all(axis=1) & (df['daa_mean_30d'] != 0),
                                                 df['fdmc'] / df['daa_mean_30d'], np.nan)
    df['Network_Value_to_DEX_Ratio'] = np.where(df[['dex_volume_sum_30d', 'fdmc']].notna().all(axis=1) & (df['dex_volume_sum_30d'] != 0),
                                                (df['fdmc'] / df['dex_volume_sum_30d']) * (30 / 365), np.nan)
    df['Network_Value_to_tvl_Ratio'] = np.where(df[['tvl_sum_30d', 'fdmc']].notna().all(axis=1) & (df['tvl_sum_30d'] != 0),
                                                (df['fdmc'] / df['tvl_sum_30d']) * (30 / 365), np.nan)


    # Metrics using 14-day rolling windows
    df['fees_mean_14d'] = grouped['fees'].rolling(window=14, min_periods=1).mean().reset_index(level=0, drop=True)
    df['tvl_mean_14d'] = grouped['tvl'].rolling(window=14, min_periods=1).mean().reset_index(level=0, drop=True)
    df['dex_volume_mean_14d'] = grouped['dex_volume'].rolling(window=14, min_periods=1).mean().reset_index(level=0, drop=True)
    df['stablecoin_transfer_volume_mean_14d'] = grouped['stablecoin_transfer_volume'].rolling(window=14, min_periods=1).mean().reset_index(level=0, drop=True)
    df['daa_mean_14d'] = grouped['daa'].rolling(window=14, min_periods=1).mean().reset_index(level=0, drop=True)

    df['Average_Fee_per_address'] = np.where(df[['fees_mean_14d', 'daa_mean_14d']].notna().all(axis=1) & (df['daa_mean_14d'] != 0),
                                          df['fees_mean_14d'] / df['daa_mean_14d'], np.nan)
    df['DEX_Volume_per_address'] = np.where(df[['dex_volume_mean_14d', 'daa_mean_14d']].notna().all(axis=1) & (df['daa_mean_14d'] != 0),
                                         df['dex_volume_mean_14d'] / df['daa_mean_14d'], np.nan)
    df['TVL_per_address'] = np.where(df[['tvl_mean_14d', 'daa_mean_14d']].notna().all(axis=1) & (df['daa_mean_14d'] != 0),
                                  df['tvl_mean_14d'] / df['daa_mean_14d'], np.nan)
    df['Stablecoin_Value_Transfer_per_address'] = np.where(df[['stablecoin_transfer_volume_mean_14d', 'daa_mean_14d']].notna().all(axis=1) & (df['daa_mean_14d'] != 0),
                                                        df['stablecoin_transfer_volume_mean_14d'] / df['daa_mean_14d'], np.nan)


    # Retain other metrics unchanged
    df['Fee_Density'] = np.where(df[['fees_sum_90d', 'fdmc']].notna().all(axis=1) & (df['fdmc'] != 0),
                                 (df['fees_sum_90d'] * 90 / 365) / df['fdmc'], np.nan)
    df['address_Density'] = np.where(df[['daa_mean_90d', 'fdmc']].notna().all(axis=1) & (df['fdmc'] != 0),
                                    df['daa_mean_90d'] / df['fdmc'], np.nan)
    df['DEX_Density'] = np.where(df[['dex_volume_mean_90d', 'fdmc']].notna().all(axis=1) & (df['fdmc'] != 0),
                                 df['dex_volume_mean_90d'] / df['fdmc'], np.nan)
    df['tvl_Density'] = np.where(df[['tvl_mean_90d', 'fdmc']].notna().all(axis=1) & (df['fdmc'] != 0),
                                 df['tvl_mean_90d'] / df['fdmc'], np.nan)
    
    df['Supply_Overhang'] = np.where(df[['mc', 'fdmc']].notna().all(axis=1) & (df['fdmc'] != 0), 
                                     (df['mc'] / df['fdmc']) - 1, np.nan)

    df['Stablecoin_Velocity'] = np.where(df[['stablecoin_transfer_volume_mean_90d', 'mc']].notna().all(axis=1) & (df['mc'] != 0), 
                                         (df['stablecoin_transfer_volume_mean_90d'] * (90 / 365)) / df['mc'], np.nan)
    
    df['Network_Value_to_Stablecoin_Ratio'] = np.where(df[['stablecoin_mc', 'fdmc']].notna().all(axis=1) & (df['stablecoin_mc'] != 0), 
                                                       df['fdmc'] / df['stablecoin_mc'], np.nan)
        
    df['Stablecoing_Density'] = np.where(df[['stablecoin_mc', 'fdmc']].notna().all(axis=1) & (df['fdmc'] != 0), 
                                         df['stablecoin_mc'] / df['fdmc'], np.nan)

    df['Daily_Fees_Native'] = np.where(df[['fees', 'price']].notna().all(axis=1) & (df['price'] != 0), 
                                         df['fees'] / df['price'], np.nan)
        
    df['Fees_per_TVL'] = np.where(df[['fees', 'tvl']].notna().all(axis=1) & (df['tvl'] != 0), 
                                         df['fees'] / df['price'], np.nan)
        
    df['Daa_per_TVL'] = np.where(df[['daa', 'tvl']].notna().all(axis=1) & (df['tvl'] != 0), 
                                         df['daa'] / df['tvl'], np.nan)
    
    
    # Drop temporary columns after calculations
    df.drop(columns=[
        'fees_mean_90d', 'fdmc_mean_90d', 'active_addresses_weekly_mean_90d', 'daa_mean_90d',
        'dex_volume_mean_90d', 'tvl_mean_90d', 'fees_sum_90d', 'dex_volume_sum_90d',  'tvl_sum_30d',
        'stablecoin_transfer_volume_mean_90d', 'daa_mean_30d', 'fees_sum_30d', 'tvl_mean_14d',
        'dex_volume_sum_30d', 'stablecoin_transfer_volume_mean_30d',
        'fees_mean_14d', 'dex_volume_mean_14d', 'stablecoin_transfer_volume_mean_14d', 
        'daa_mean_14d'
    ], inplace=True)

    return df


# Add moving averages and percentage changes
def add_moving_averages_defi(df):
    # Sort the dataframe by 'project_name' and 'datestamp'
    df = df.sort_values(by=['project_name', 'datestamp']).reset_index(drop=True)
    
    periods_ma = [7, 14, 30, 60, 90, 180, 365]
    
    # Select all columns except 'project_name', 'datestamp', and 'sector'
    cols_to_process = [col for col in df.columns if col not in ['project_name', 'datestamp', 'sector']]
    
    # Group the data by 'project_name'
    grouped = df.groupby('project_name')

    # Iterate over columns to process
    for col in cols_to_process:
        # Calculate moving averages
        for period in periods_ma:
            df[f'{col} MA {period}D'] = grouped[col].transform(lambda x: x.rolling(window=period, min_periods=1).mean())

    return df


def save_to_fact_table_defi(conn):
    df = fetch_j_raw_defi(conn)  # Fetch the data
    print("Data Fetched")
    df = calculate_metrics_defi(df)  # Calculate metrics
    print("Metrics Added")
    df_final = add_moving_averages_defi(df)  # Add moving averages and percentage changes
    print("MA added")

    # Convert column names to lowercase and replace spaces with underscores
    df_final.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_final.columns]

    # Ensure the first three columns are ordered, and the rest sorted alphabetically
    ordered_columns = ['project_name', 'datestamp', 'sector'] + sorted(
        [col for col in df_final.columns if col not in ['project_name', 'datestamp', 'sector']]
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
                    INSERT INTO f_defi_assets_metrics ({cols_str}) 
                    VALUES ({placeholders}) 
                    ON DUPLICATE KEY UPDATE {update_cols}
                    """, 
                    data[i:i + batch_size]
                )
            conn.commit()
            print("Data successfully inserted into f_defi_assets_metrics")
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        conn.rollback()
