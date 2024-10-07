import requests, mysql.connector, json, time, os, sys
from sqlalchemy import create_engine
from datetime import datetime
from mysql.connector import Error
from dotenv import load_dotenv
from m_functions import *
from collections import deque
load_dotenv()
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("LOCAL_DB_PASSWORD")


def custom_project_names(project_name):
	custom_name_mapping = {
		# Left is taking in, right is being returned
		"akash":"akash-network",
		"arbitrum_one_bridge": "arbitrum-bridge",
		"axelar":"axelarnetwork",
		"benqi_finance":"Benqi",
		"binance-smart-chain":"BSC",
		"bsc":"BSC",
		"cosmoshub":"Cosmos",
		"crypto-com-coin":"Cronos",
		"dfinity":"internetcomputer",
		"drift":"drift-protocol",
		"ethereum-2-0":"Ethereum",
		"fuse-network-token":"Fuse",
		"frax":"frax-finance",
		"immutable_x":"immutable",
		"internet-computer":"internetcomputer",
		"injective-protocol":"Injective",
		"lido":"lido-finance",
		"near":"NEAR",
		"near-protocol":"NEAR",
		"maverick_protocol":"maverick",
		"matic-network":"Polygon",
		"rabbit-x":"rabbitx",
		"the-open-network":"The Open Network",
		"ton":"The Open Network",
		"rocketpool":"Rocket-pool",
		"sei":"Sei",
		"sei-network":"Sei",
		"trader_joe":"trader-joe",
		"zksync":"zksync-era",
		"zksync_era_bridge":"zksync-era-bridge"
	}
	if project_name in custom_name_mapping:
		return custom_name_mapping.get(project_name)

	if project_name[0].isalpha():
		return project_name[0].upper() + project_name[1:]

	else:
		return project_name


# Update raw table
def calc_update_raw_table(conn, engine):
	try:
		with conn.cursor() as cursor:
			# ===================================================================== #
			# Token Terminal table
			tt_query = """SELECT datestamp, project_id as project_name, 
			user_dau as daa, user_dau as daa_t, 
			fees, fees as fees_t, 
			market_cap_circulating as mc, market_cap_circulating as mc_t, 
			market_cap_fully_diluted as fdmc, market_cap_fully_diluted as fdmc_t, 
			price, price as price_t, 
			transaction_count as transactions, transaction_count as transactions_t, 
			revenue, revenue as revenue_t, 
			transaction_fee_average as avg_txn_fee, transaction_fee_average as avg_txn_fee_t, 
			user_mau as maa, user_mau as maa_t, 
			tokenholders, tokenholders as tokenholders_t, 
			tvl, tvl as tvl_t, 
			token_trading_volume as volume_24h_usd, token_trading_volume as volume_24h_usd_t, 
			active_developers, active_developers as active_developers_t, 
			active_loans, active_loans as active_loans_t, 
			earnings, earnings as earnings_t, 
			gross_profit, gross_profit as gross_profit_t, 
			token_supply_circulating, token_supply_circulating as token_supply_circulating_t, 
			token_incentives, token_incentives as token_incentives_t, 
			token_supply_maximum, token_supply_maximum as token_supply_maximum_t, 
			active_addresses_weekly, active_addresses_weekly as active_addresses_weekly_t 
			FROM tt_all_metrics_data WHERE user_dau > 0 AND fees > 0 ORDER BY datestamp DESC"""

			tt_df = pd.read_sql(tt_query, engine)

			tt_df["project_name"] = tt_df["project_name"].apply(custom_project_names)

			udb(conn, "update", "j_raw", 2, tt_df)
			print("Updated token terminal")

			# ===================================================================== #
			# Artemis API table
			art_query = """SELECT datestamp, project_name, 
			dau as daa, dau as daa_a, 
			fees, fees as fees_a, 
			mc, mc as mc_a, 
			fdmc, fdmc as fdmc_a, 
			price, price as price_a, 
			daily_txns as transactions, daily_txns as transactions_a, 
			revenue, revenue as revenue_a, 
			avg_txn_fees as avg_txn_fee, avg_txn_fees as avg_txn_fee_a, 
			dau_over_100 as daa_over_100, dau_over_100 as daa_over_100_a, 
			dex_volumes as dex_volume, dex_volumes as dex_volume_a, 
			tvl, tvl as tvl_a, 
			stablecoin_mc, stablecoin_mc as stablecoin_mc_a, 
			volume_24h as volume_24h_usd, volume_24h as volume_24h_usd_a, 
			circulating_supply, circulating_supply as circulating_supply_a
			FROM art_metric_data WHERE dau > 0 AND fees > 0 ORDER BY datestamp DESC"""

			art_df = pd.read_sql(art_query, engine)

			art_df["project_name"] = art_df["project_name"].apply(custom_project_names)

			udb(conn, "update", "j_raw", 2, art_df)
			print("Updated artemis (API)")

			# ===================================================================== #
			# Artemis SF table
			art_sf_query = """SELECT datestamp, project_name, 
			stablecoin_total_supply as stablecoin_mc, stablecoin_total_supply as stablecoin_mc_a, 
			stablecoin_transfer_volume, stablecoin_transfer_volume as stablecoin_transfer_volume_a, 
			weekly_commits_core_ecosystem as weekly_commits_core, weekly_commits_core_ecosystem as weekly_commits_core_a, 
			weekly_commits_sub_ecosystem as weekly_commits_sub, weekly_commits_sub_ecosystem as weekly_commits_sub_a, 
			weekly_contracts_deployed, weekly_contracts_deployed as weekly_contracts_deployed_a, 
			weekly_contract_deployers, weekly_contract_deployers as weekly_contract_deployers_a, 
			weekly_developers_core_ecosystem as weekly_dev_core, weekly_developers_core_ecosystem as weekly_dev_core_a, 
			weekly_developers_sub_ecosystem as weekly_dev_sub, weekly_developers_sub_ecosystem as weekly_dev_sub_a 
			FROM art_sf_raw_data WHERE dau > 0 AND fees > 0 ORDER BY datestamp DESC"""

			art_sf_sf = pd.read_sql(art_sf_query, engine)
			art_sf_sf["project_name"] = art_sf_sf["project_name"].apply(custom_project_names)
			udb(conn, "update", "j_raw", 2, art_sf_sf)

			print("Updated artemis (SF)")
			

			# ===================================================================== #
			# Staking rewards
			sr_query = """SELECT datestamp, project_id as project_name, 
			active_validators,
			annualized_rewards_usd,
			circulating_percentage,
			daily_trading_volume, 
			delegated_tokens,
			inflation_rate,
			net_staking_flow_7d,
			real_reward_rate,
			reward_rate,
			staked_tokens,
			staking_marketcap,
			staking_ratio,
			total_staking_wallets,
			total_validators
			FROM sr_raw_data ORDER BY datestamp DESC"""

			# sr_df = pd.read_sql(sr_query, engine)
			# sr_df["project_name"] = sr_df["project_name"].apply(custom_project_names)
			# udb(conn, "update", "j_raw", 2, sr_df)
			# print("Updated Staking rewards")



			# ===================================================================== #
			# Update sector info
			update_sectors = """
				UPDATE j_raw r
				JOIN v_all_unique_projects p ON p.project_id = r.project_name
				SET r.sector = p.sector
				WHERE r.sector IS NULL OR r.sector <> p.sector
			"""
			cursor.execute(update_sectors)
			conn.commit()



			cursor.execute("UPDATE j_raw SET sector = 'blockchains-l1' WHERE project_name = 'The Open Network'")
			cursor.execute("UPDATE j_raw SET sector = 'lending' WHERE project_name = 'Moonwell'")
			cursor.execute("UPDATE j_raw SET sector = 'exchange' WHERE project_name = 'Thruster'")
			conn.commit()

	except Error as e:
		print(f"Error: {e}")
