# Gets data from artemis snowflake db and inserts into local db

import requests, mysql.connector, json, time, os, snowflake.connector, concurrent.futures
from datetime import datetime, timedelta
from mysql.connector import Error
from dotenv import load_dotenv
from m_functions import *
load_dotenv()
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")

sf_db_pw = os.getenv("SNOWFLAKE_PW")
sf_acc_id = os.getenv("SNOWFLAKE_ACC_ID")

# Gets last update date for project
def art_sf_get_last_update(project, col):
	conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password)
	with conn.cursor() as cursor:
		cursor.execute(f"SELECT datestamp FROM art_sf_raw_data WHERE project_name='{project}' and {col} IS NOT NULL ORDER BY datestamp DESC LIMIT 1")
		raw_data = cursor.fetchone()

		if raw_data is None or raw_data[0] is None:
			return "2010-01-01"
		else:
			date_m7 = (raw_data[0] - timedelta(days=7)).strftime("%Y-%m-%d")
			return date_m7

# Updates data by getting the metric from the SF db and copying it to local db with custom names
def sf_art_update_raw_data(conn_sf):
	project_list = {
		"akash": {
			"sf_tables": {
				"EZ_METRICS":["date", "active_leases", "active_providers", "new_leases", "compute_fees_native","compute_fees_total_usd","compute_fees_usdc","validator_fees_native","validator_fees","total_fees","revenue","total_burned_native","mints"]
						},
			"local_cols":["project_name", "datestamp", "active_leases", "active_providers", "new_leases", "compute_fees_native", "compute_fees_total_usd", "compute_fees_usdc", "validator_fees_native", "validator_fees", "total_fees", "revenue", "total_burned_native", "mints"]
		},
		"aptos":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "MARKET_CAP", "PRICE", "REVENUE", "REVENUE_NATIVE", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "dex_volumes", "fdmc", "fees", "fees_native", "market_cap", "price", "revenue", "revenue_native", "tvl", "txns","weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"arbitrum":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DAU_OVER_100", "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "L1_DATA_COST", "L1_DATA_COST_NATIVE", "LOW_SLEEP_USERS", "MARKET_CAP", "NEW_USERS", "NFT_TRADING_VOLUME", "NON_P2P_STABLECOIN_TRANSFER_VOLUME", "NON_SYBIL_USERS", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PRICE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "SYBIL_USERS", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_CONTRACTS_DEPLOYED", "WEEKLY_CONTRACT_DEPLOYERS", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "dau_over_100", "deduped_stablecoin_transfer_volume", "dex_volumes", "fdmc", "fees", "fees_native", "high_sleep_users", "l1_data_cost", "l1_data_cost_native", "low_sleep_users", "market_cap", "new_users", "nft_trading_volume", "non_p2p_stablecoin_transfer_volume", "non_sybil_users", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "price", "returning_users", "revenue", "revenue_native", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "sybil_users", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_contracts_deployed", "weekly_contract_deployers", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"avalanche":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DAU_OVER_100", "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "ISSUANCE",  "LOW_SLEEP_USERS", "MARKET_CAP", "NEW_USERS", "NFT_TRADING_VOLUME", "NON_P2P_STABLECOIN_TRANSFER_VOLUME", "NON_SYBIL_USERS", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PRICE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "SYBIL_USERS", "TOTAL_STAKED_NATIVE", "TOTAL_STAKED_USD", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_CONTRACTS_DEPLOYED", "WEEKLY_CONTRACT_DEPLOYERS", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "dau_over_100", "deduped_stablecoin_transfer_volume", "dex_volumes", "fdmc", "fees", "fees_native", "high_sleep_users", "issuance",  "low_sleep_users", "market_cap", "new_users", "nft_trading_volume", "non_p2p_stablecoin_transfer_volume", "non_sybil_users", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "price", "returning_users", "revenue", "revenue_native", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "sybil_users", "total_staked_native", "total_staked_usd", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_contracts_deployed", "weekly_contract_deployers", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"base":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "L1_DATA_COST", "L1_DATA_COST_NATIVE", "LOW_SLEEP_USERS", "MAU", "NEW_USERS", "NFT_TRADING_VOLUME", "NON_P2P_STABLECOIN_TRANSFER_VOLUME", "NON_SYBIL_USERS", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "SYBIL_USERS", "TVL", "TXNS", "WEEKLY_CONTRACTS_DEPLOYED", "WEEKLY_CONTRACT_DEPLOYERS"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "deduped_stablecoin_transfer_volume", "dex_volumes", "fees", "fees_native", "high_sleep_users", "l1_data_cost", "l1_data_cost_native", "low_sleep_users", "mau", "new_users", "nft_trading_volume", "non_p2p_stablecoin_transfer_volume", "non_sybil_users", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "returning_users", "revenue", "revenue_native", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "sybil_users", "tvl", "txns", "weekly_contracts_deployed", "weekly_contract_deployers"]
		},
		"bitcoin":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "CIRCULATING_SUPPLY", "DAU", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "ISSUANCE", "MARKET_CAP", "MAU", "NFT_TRADING_VOLUME", "PRICE", "REVENUE", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "circulating_supply", "dau", "dex_volumes", "fdmc", "fees", "fees_native", "issuance", "market_cap", "mau", "nft_trading_volume", "price", "revenue", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"cardano":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "DAU", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "MARKET_CAP", "NFT_TRADING_VOLUME", "PRICE", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "dau", "dex_volumes", "fdmc", "fees", "fees_native", "market_cap", "nft_trading_volume", "price", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"celestia":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_COST_PER_MIB", "AVG_COST_PER_MIB_NATIVE", "AVG_MIB_PER_SECOND", "BLOB_FEES", "BLOB_FEES_NATIVE", "BLOB_SIZE_MIB", "FDMC", "FEES", "FEES_NATIVE", "MARKET_CAP", "MINTS", "PRICE", "SUBMITTERS", "TXNS"]
			},
			"local_cols":["project_name", "datestamp", "avg_cost_per_mib", "avg_cost_per_mib_native", "avg_mib_per_second", "blob_fees", "blob_fees_native", "blob_size_mib", "fdmc", "fees", "fees_native", "market_cap", "mints", "price", "submitters", "txns"]
		},
		"chainlink":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AUTOMATION_FEES", "CCIP_FEES", "DIRECT_FEES", "EARNINGS", "FDMC", "FEES", "FM_FEES", "MARKET_CAP", "OCR_FEES", "OPERATING_EXPENSES", "PRICE", "PRIMARY_SUPPLY_SIDE_REVENUE", "PROTOCOL_REVENUE", "SECONDARY_SUPPLY_SIDE_REVENUE", "TOKENHOLDER_COUNT", "TOKEN_INCENTIVES", "TOKEN_TURNOVER_CIRCULATING", "TOKEN_TURNOVER_FDV", "TOKEN_VOLUME", "TOTAL_EXPENSES", "TOTAL_SUPPLY_SIDE_REVENUE", "TREASURY_LINK", "TREASURY_USD", "TVL", "TVL_LINK", "VRF_FEES"]
			},
			"local_cols":["project_name", "datestamp", "automation_fees", "ccip_fees", "direct_fees", "earnings", "fdmc", "fees", "fm_fees", "market_cap", "ocr_fees", "operating_expenses", "price", "primary_supply_side_revenue", "protocol_revenue", "secondary_supply_side_revenue", "tokenholder_count", "token_incentives", "token_turnover_circulating", "token_turnover_fdv", "token_volume", "total_expenses", "total_supply_side_revenue", "treasury_link", "treasury_usd", "tvl", "tvl_link", "vrf_fees"]
		},
		"cosmoshub":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "DAU", "FDMC", "FEES", "MARKET_CAP", "PRICE", "REVENUE", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "dau", "fdmc", "fees", "market_cap", "price", "revenue", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"ethereum":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_COST_PER_MIB", "AVG_COST_PER_MIB_GWEI", "AVG_MIB_PER_SECOND", "AVG_TXN_FEE", "BLOB_FEES", "BLOB_FEES_NATIVE", "BLOB_SIZE_MIB", "CENSORED_BLOCKS", "DAU", "DAU_OVER_100", "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "LOW_SLEEP_USERS", "MARKET_CAP", "MAU", "NEW_USERS", "NFT_TRADING_VOLUME", "NON_CENSORED_BLOCKS", "NON_P2P_STABLECOIN_TRANSFER_VOLUME", "NON_SYBIL_USERS", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PERCENT_CENSORED", "PERCENT_NON_CENSORED", "PERCENT_SEMI_CENSORED", "PRICE", "PRIORITY_FEE_NATIVE", "PRIORITY_FEE_USD", "QUEUE_ACTIVE_AMOUNT", "QUEUE_ENTRY_AMOUNT", "QUEUE_EXIT_AMOUNT", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SEMI_CENSORED_BLOCKS", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "SUBMITTERS", "SYBIL_USERS", "TOTAL_BLOCKS_PRODUCED", "TOTAL_STAKED_NATIVE", "TOTAL_STAKED_USD", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_cost_per_mib", "avg_cost_per_mib_gwei", "avg_mib_per_second", "avg_txn_fee", "blob_fees", "blob_fees_native", "blob_size_mib", "censored_blocks", "dau", "dau_over_100", "deduped_stablecoin_transfer_volume", "dex_volumes", "fdmc", "fees", "fees_native", "high_sleep_users", "low_sleep_users", "market_cap", "mau", "new_users", "nft_trading_volume", "non_censored_blocks", "non_p2p_stablecoin_transfer_volume", "non_sybil_users", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "percent_censored", "percent_non_censored", "percent_semi_censored", "price", "priority_fee_native", "priority_fee_usd", "queue_active_amount", "queue_entry_amount", "queue_exit_amount", "returning_users", "revenue", "revenue_native", "semi_censored_blocks", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "submitters", "sybil_users", "total_blocks_produced", "total_staked_native", "total_staked_usd", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem"]
		},
		"injective":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "LOW_SLEEP_USERS", "MINTS", "NEW_USERS", "NON_SYBIL_USERS", "RETURNING_USERS", "SYBIL_USERS", "TXNS"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "fees", "fees_native", "high_sleep_users", "low_sleep_users", "mints", "new_users", "non_sybil_users", "returning_users", "sybil_users", "txns"]
		},
		"near":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_COST_PER_MIB", "AVG_MIB_PER_SECOND", "AVG_TXN_FEE", "BLOB_FEES", "BLOB_FEES_NATIVE", "BLOB_SIZE_MIB", "DAU", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "LOW_SLEEP_USERS", "MARKET_CAP", "MAU", "NEW_USERS", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PRICE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SUBMITTERS", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_CONTRACTS_DEPLOYED", "WEEKLY_CONTRACT_DEPLOYERS", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_cost_per_mib", "avg_mib_per_second", "avg_txn_fee", "blob_fees", "blob_fees_native", "blob_size_mib", "dau", "dex_volumes", "fdmc", "fees", "fees_native", "high_sleep_users", "low_sleep_users", "market_cap", "mau", "new_users", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "price", "returning_users", "revenue", "revenue_native", "submitters", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_contracts_deployed", "weekly_contract_deployers", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"optimism":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DAU_OVER_100", "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "LOW_SLEEP_USERS", "L1_DATA_COST", "L1_DATA_COST_NATIVE", "MARKET_CAP", "MAU", "NEW_USERS", "NFT_TRADING_VOLUME", "NON_P2P_STABLECOIN_TRANSFER_VOLUME", "NON_SYBIL_USERS", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PRICE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "SYBIL_USERS", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_CONTRACTS_DEPLOYED", "WEEKLY_CONTRACT_DEPLOYERS", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "dau_over_100", "deduped_stablecoin_transfer_volume", "dex_volumes", "fdmc", "fees", "fees_native", "high_sleep_users", "low_sleep_users", "l1_data_cost", "l1_data_cost_native", "market_cap", "mau", "new_users", "nft_trading_volume", "non_p2p_stablecoin_transfer_volume", "non_sybil_users", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "price", "returning_users", "revenue", "revenue_native", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "sybil_users", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_contracts_deployed", "weekly_contract_deployers", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"polkadot":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "DAU", "FDMC", "FEES", "FEES_NATIVE", "MARKET_CAP", "MAU",  "PRICE", "REVENUE", "REVENUE_NATIVE", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM",  "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "dau", "fdmc", "fees", "fees_native", "market_cap", "mau", "price",  "revenue", "revenue_native", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem",  "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"polygon":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DAU_OVER_100", "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "HIGH_SLEEP_USERS", "LOW_SLEEP_USERS", "L1_DATA_COST", "L1_DATA_COST_NATIVE", "MARKET_CAP", "MAU", "NEW_USERS", "NFT_TRADING_VOLUME", "NON_P2P_STABLECOIN_TRANSFER_VOLUME", "NON_SYBIL_USERS", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PRICE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "SYBIL_USERS", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_CONTRACTS_DEPLOYED", "WEEKLY_CONTRACT_DEPLOYERS", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "dau_over_100", "deduped_stablecoin_transfer_volume", "dex_volumes", "fdmc", "fees", "fees_native", "high_sleep_users", "low_sleep_users", "l1_data_cost", "l1_data_cost_native", "market_cap", "mau", "new_users", "nft_trading_volume", "non_p2p_stablecoin_transfer_volume", "non_sybil_users", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "price", "returning_users", "revenue", "revenue_native", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "sybil_users", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_contracts_deployed", "weekly_contract_deployers", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"solana":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "BASE_FEE", "BASE_FEE_NATIVE", "DAU",  "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "ISSUANCE",  "MARKET_CAP",  "MAU", "NEW_USERS", "NFT_TRADING_VOLUME", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PRICE", "PRIORITY_FEE", "PRIORITY_FEE_NATIVE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "TOTAL_STAKED_NATIVE", "TOTAL_STAKED_USD", "TVL", "TXNS", "VOTE_TX_FEE_NATIVE", "VOTE_TX_FEE_USD", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_CONTRACTS_DEPLOYED", "WEEKLY_CONTRACT_DEPLOYERS", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "base_fee", "base_fee_native", "dau",  "deduped_stablecoin_transfer_volume", "dex_volumes", "fdmc", "fees", "fees_native", "issuance",  "market_cap", "mau", "new_users", "nft_trading_volume", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "price", "priority_fee", "priority_fee_native", "returning_users", "revenue", "revenue_native", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "total_staked_native", "total_staked_usd", "tvl", "txns", "vote_tx_fee_native", "vote_tx_fee_usd", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_contracts_deployed", "weekly_contract_deployers", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"sui":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "MARKET_CAP", "MAU", "NEW_USERS", "PRICE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM",  "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau",  "dex_volumes", "fdmc", "fees", "fees_native", "market_cap", "mau", "new_users", "price", "returning_users", "revenue", "revenue_native", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem",  "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"ton":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE",   "MARKET_CAP", "PRICE", "REVENUE", "REVENUE_NATIVE", "TRANSACTION_NODES", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM",  "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "dex_volumes", "fdmc", "fees", "fees_native",   "market_cap", "price", "revenue", "revenue_native", "transaction_nodes", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem",  "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		},
		"tron":{
			"sf_tables":{
				"EZ_METRICS":["DATE", "AVG_TXN_FEE", "DAU", "DEDUPED_STABLECOIN_TRANSFER_VOLUME", "DEX_VOLUMES", "FDMC", "FEES", "FEES_NATIVE", "MARKET_CAP", "MAU", "NEW_USERS", "NON_P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_NATIVE_TRANSFER_VOLUME", "P2P_STABLECOIN_TRANSFER_VOLUME", "P2P_TOKEN_TRANSFER_VOLUME", "P2P_TRANSFER_VOLUME", "PRICE", "RETURNING_USERS", "REVENUE", "REVENUE_NATIVE", "SETTLEMENT_VOLUME", "STABLECOIN_DAU", "STABLECOIN_TOTAL_SUPPLY", "STABLECOIN_TRANSFER_VOLUME", "STABLECOIN_TXNS", "TVL", "TXNS", "WEEKLY_COMMITS_CORE_ECOSYSTEM", "WEEKLY_COMMITS_SUB_ECOSYSTEM", "WEEKLY_DEVELOPERS_CORE_ECOSYSTEM", "WEEKLY_DEVELOPERS_SUB_ECOSYSTEM"]
			},
			"local_cols":["project_name", "datestamp", "avg_txn_fee", "dau", "deduped_stablecoin_transfer_volume", "dex_volumes", "fdmc", "fees", "fees_native", "market_cap", "mau", "new_users", "non_p2p_stablecoin_transfer_volume", "p2p_native_transfer_volume", "p2p_stablecoin_transfer_volume", "p2p_token_transfer_volume", "p2p_transfer_volume", "price", "returning_users", "revenue", "revenue_native", "settlement_volume", "stablecoin_dau", "stablecoin_total_supply", "stablecoin_transfer_volume", "stablecoin_txns", "tvl", "txns", "weekly_commits_core_ecosystem", "weekly_commits_sub_ecosystem", "weekly_developers_core_ecosystem", "weekly_developers_sub_ecosystem"]
		}
	}

	with conn_sf.cursor() as cursor:
		date_two_days_ago = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
		for project, params in project_list.items():
			for table, sf_cols in params["sf_tables"].items():

				last_update_m7 = art_sf_get_last_update(project, params["local_cols"][3])
				print(f"Updating {project} with data from {last_update_m7} to {date_two_days_ago}")
				data_to_update = []
				sf_cols_string = ", ".join(sf_cols)
				cursor.execute(f"SELECT {sf_cols_string} from {project}.{table} WHERE DATE BETWEEN '{last_update_m7}' AND '{date_two_days_ago}' ORDER BY DATE DESC")
				data = cursor.fetchall()
				for row in data:
					data_to_update.append((project,) + row)
				
				update_local_db(data_to_update, params["local_cols"])


def update_local_db(data_to_update, local_cols):
	conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password)

	with conn.cursor() as cursor:
		placeholders = ", ".join(["%s"] * len(local_cols))
		local_cols_string = ", ".join(local_cols)
		update_parts = [f"{col} = VALUES({col})" for col in local_cols]
		update_string = ", ".join(update_parts)

		cursor.executemany(f"INSERT INTO art_sf_raw_data ({local_cols_string}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_string}", data_to_update)
		conn.commit()



