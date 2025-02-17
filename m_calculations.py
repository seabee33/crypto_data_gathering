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
		"maker":"Makerdao",
		"rabbit-x":"rabbitx",
		"the-open-network":"The Open Network",
		"ton":"The Open Network",
		"rocketpool":"Rocket-pool",
		"sei":"Sei",
		"sei-network":"Sei",
		"trader_joe":"trader-joe",
		"zksync":"zksync-era",
		"Zksync-era":"zksync-era",
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
			print("Adding TT to DF")
			tt_query = """SELECT  datestamp, project_id as project_name,
			user_dau as daa_t, 
			fees as fees_t, 
			market_cap_circulating as mc_t, 
			market_cap_fully_diluted as fdmc_t, 
			price as price_t, 
			transaction_count as transactions_t, 
			revenue as revenue_t, 
			transaction_fee_average as avg_txn_fee_t, 
			user_mau as maa_t, 
			tokenholders as tokenholders_t, 
			tvl as tvl_t, 
			token_trading_volume as volume_24h_usd_t, 
			active_developers as active_developers_t, 
			active_loans as active_loans_t, 
			earnings as earnings_t, 
			gross_profit as gross_profit_t, 
			token_supply_circulating as circulating_supply_t, 
			token_incentives as token_incentives_t, 
			token_supply_maximum as token_supply_maximum_t, 
			active_addresses_weekly as active_addresses_weekly_t,
			p2p_swap_count as p2p_swap_count_t
			FROM tt_raw_data
			WHERE active_addresses_monthly is not null or fees is not null or market_cap_circulating is not null
			ORDER BY datestamp DESC"""

			tt_df = pd.read_sql(tt_query, engine)

			tt_df["project_name"] = tt_df["project_name"].apply(custom_project_names)

			# ===================================================================== #
			# Artemis API table
			print("Updating j_raw with art(API)")
			art_query = """SELECT  datestamp, project_name, 
			dau as daa_a, 
			fees as fees_a, 
			mc as mc_a, 
			fdmc as fdmc_a, 
			price as price_a, 
			daily_txns as transactions_a, 
			revenue as revenue_a, 
			avg_txn_fees as avg_txn_fee_a, 
			dau_over_100 as daa_over_100_a, 
			dex_volumes as dex_volume_a, 
			tvl as tvl_a, 
			stablecoin_mc as stablecoin_mc_a, 
			volume_24h as volume_24h_usd_a, 
			circulating_supply as circulating_supply_a
			FROM art_metric_data
			WHERE fees is not null or mc is not null or dau is not null 
			ORDER BY datestamp DESC"""

			art_df = pd.read_sql(art_query, engine)

			art_df["project_name"] = art_df["project_name"].apply(custom_project_names)

			# ===================================================================== #
			# Artemis SF table
			print("Updating j_raw with art(SF)")
			art_sf_query = """SELECT  datestamp, project_name, 
			stablecoin_transfer_volume, stablecoin_transfer_volume as stablecoin_transfer_volume_a, 
			weekly_commits_core_ecosystem as weekly_commits_core, weekly_commits_core_ecosystem as weekly_commits_core_a, 
			weekly_commits_sub_ecosystem as weekly_commits_sub, weekly_commits_sub_ecosystem as weekly_commits_sub_a, 
			weekly_contracts_deployed, weekly_contracts_deployed as weekly_contracts_deployed_a, 
			weekly_contract_deployers, weekly_contract_deployers as weekly_contract_deployers_a, 
			weekly_developers_core_ecosystem as weekly_dev_core, weekly_developers_core_ecosystem as weekly_dev_core_a, 
			weekly_developers_sub_ecosystem as weekly_dev_sub, weekly_developers_sub_ecosystem as weekly_dev_sub_a 
			FROM art_sf_raw_data ORDER BY datestamp DESC"""

			art_sf_df = pd.read_sql(art_sf_query, engine)
			art_sf_df["project_name"] = art_sf_df["project_name"].apply(custom_project_names)
			

			# ===================================================================== #
			# Staking rewards
			print("Updating j_raw with SR")
			sr_query = """SELECT  datestamp, project_id as project_name, 
			active_validators,
			annualized_rewards_usd,
			circulating_percentage,
			daily_trading_volume, 
			delegated_tokens,
			inflation_rate,
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



			# j_raw
			merged_df = pd.merge(tt_df, art_df, on=["datestamp", "project_name"], how="outer")
			merged_df = pd.merge(merged_df, art_sf_df, on=["datestamp", "project_name"], how="outer")
			merged_df = merged_df.sort_values(by=["project_name", "datestamp"])

			merged_df["daa"] = merged_df["daa_t"].combine_first(merged_df["daa_a"])
			merged_df["fees"] = merged_df["fees_t"].combine_first(merged_df["fees_a"])
			merged_df["mc"] = merged_df["mc_t"].combine_first(merged_df["mc_a"])
			merged_df["fdmc"] = merged_df["fdmc_t"].combine_first(merged_df["fdmc_a"])
			merged_df["price"] = merged_df["price_t"].combine_first(merged_df["price_a"])
			merged_df["transactions"] = merged_df["transactions_t"].combine_first(merged_df["transactions_a"])
			merged_df["revenue"] = merged_df["revenue_t"].combine_first(merged_df["revenue_a"])
			merged_df["avg_txn_fee"] = merged_df["avg_txn_fee_t"].combine_first(merged_df["avg_txn_fee_a"])
			merged_df["maa"] = merged_df["maa_t"]
			merged_df["tokenholders"] = merged_df["tokenholders_t"]
			merged_df["tvl"] = merged_df["tvl_t"].combine_first(merged_df["tvl_a"])
			merged_df["volume_24h_usd"] = merged_df["volume_24h_usd_t"].combine_first(merged_df["volume_24h_usd_a"])
			merged_df["active_developers"] = merged_df["active_developers_t"]
			merged_df["active_loans"] = merged_df["active_loans_t"]
			merged_df["earnings"] = merged_df["earnings_t"]
			merged_df["gross_profit"] = merged_df["gross_profit_t"]
			merged_df["circulating_supply"] = merged_df["circulating_supply_t"].combine_first(merged_df["circulating_supply_a"])
			merged_df["token_incentives"] = merged_df["token_incentives_t"]
			merged_df["token_supply_maximum"] = merged_df["token_supply_maximum_t"]
			merged_df["active_addresses_weekly"] = merged_df["active_addresses_weekly_t"]
			merged_df["stablecoin_mc"] = merged_df["stablecoin_mc_a"]
			merged_df["dex_volume"] = merged_df["dex_volume_a"]
			merged_df["daa_over_100"] = merged_df["daa_over_100_a"]
			merged_df["p2p_swap_count"] = merged_df["p2p_swap_count_t"]

			# Fill forward the weekly data
			merged_df["stablecoin_transfer_volume"]	= merged_df.groupby("project_name")["stablecoin_transfer_volume"].ffill()
			merged_df["stablecoin_transfer_volume_a"] = merged_df.groupby("project_name")["stablecoin_transfer_volume_a"].ffill()
			merged_df["weekly_commits_core"] = merged_df.groupby("project_name")["weekly_commits_core"].ffill()
			merged_df["weekly_commits_core_a"] = merged_df.groupby("project_name")["weekly_commits_core_a"].ffill()
			merged_df["weekly_commits_sub"]	= merged_df.groupby("project_name")["weekly_commits_sub"].ffill()
			merged_df["weekly_commits_sub_a"] = merged_df.groupby("project_name")["weekly_commits_sub_a"].ffill()
			merged_df["weekly_contracts_deployed"] = merged_df.groupby("project_name")["weekly_contracts_deployed"].ffill()
			merged_df["weekly_contracts_deployed_a"] = merged_df.groupby("project_name")["weekly_contracts_deployed_a"].ffill()
			merged_df["weekly_contract_deployers"] = merged_df.groupby("project_name")["weekly_contract_deployers"].ffill()
			merged_df["weekly_contract_deployers_a"] = merged_df.groupby("project_name")["weekly_contract_deployers_a"].ffill()
			merged_df["weekly_dev_core"] = merged_df.groupby("project_name")["weekly_dev_core"].ffill()
			merged_df["weekly_dev_core_a"] = merged_df.groupby("project_name")["weekly_dev_core_a"].ffill()
			merged_df["weekly_dev_sub"] = merged_df.groupby("project_name")["weekly_dev_sub"].ffill()
			merged_df["weekly_dev_sub_a"] = merged_df.groupby("project_name")["weekly_dev_sub_a"].ffill()

			# merged_df.to_csv("merged.csv", index=False)
			udb(conn, "update", "j_raw", 2, merged_df)
			print("j_raw updated")

			# ===================================================================== #
			# Update sector info
			print("updating sectors for j_raw")
			t_sector = "SELECT  project_id as project_name, market_sector as sector FROM tt_all_projects"
			sector_t = pd.read_sql(t_sector, engine)
			sector_t["project_name"] = sector_t["project_name"].apply(custom_project_names)

			update_sectors = "UPDATE j_raw SET sector= %s WHERE project_name=%s"

			for index, row in sector_t.iterrows():
				sector = row["sector"]
				project_name = row["project_name"]
				cursor.execute(update_sectors, (sector, project_name))
			conn.commit()


			cursor.execute("UPDATE j_raw SET sector = 'blockchains-l1' WHERE project_name = 'The Open Network'")
			cursor.execute("UPDATE j_raw SET sector = 'lending' WHERE project_name = 'Moonwell'")
			cursor.execute("UPDATE j_raw SET sector = 'exchange' WHERE project_name = 'Thruster'")
			cursor.execute("UPDATE j_raw SET sector = 'blockchains-l1' WHERE project_name = 'Aleph-zero'")
			cursor.execute("UPDATE j_raw SET sector = 'lending' WHERE project_name = 'Zerolend'")
			cursor.execute("UPDATE j_raw SET sector = 'blockchains-l1' WHERE project_name = 'Beam'")
			cursor.execute("UPDATE j_raw SET sector = 'depin' WHERE project_name = 'Geodnet'")
			cursor.execute("UPDATE j_raw SET sector = 'marketing' WHERE project_name = 'Layer3'")
			cursor.execute("UPDATE j_raw SET sector = 'gaming' WHERE project_name = 'Metaplex'")
			cursor.execute("UPDATE j_raw SET sector = 'exchange' WHERE project_name = 'Raydium'")
			cursor.execute("UPDATE j_raw SET sector = 'gaming' WHERE project_name = 'defi-kingdoms'")
			cursor.execute("UPDATE j_raw SET sector = 'lending' WHERE project_name = 'Makerdao'")
			cursor.execute("UPDATE j_raw SET sector = 'exchange' WHERE project_name = 'Woo'")
			cursor.execute("UPDATE j_raw SET sector = 'liquidity' WHERE project_name = 'Acala'")
			cursor.execute("UPDATE j_raw SET sector = 'blockchains-l1' WHERE project_name = 'Fuse'")
			cursor.execute("UPDATE j_raw SET sector = 'blockchains-l1' WHERE project_name = 'Polygon_zk'")
			cursor.execute("UPDATE j_raw SET sector = 'blockchains-l1' WHERE project_name = 'Multiversx'")

			conn.commit()






			new_log_entry(conn, ("g", "core", "j_raw updated"))

	except Error as e:
		print(f"Error: {e}")
