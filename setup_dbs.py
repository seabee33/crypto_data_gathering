import mysql.connector, os
from mysql.connector import Error
from dotenv import load_dotenv
from m_art import *
from m_bitformance import *

load_dotenv()
db_password = os.getenv("LOCAL_DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")

table_data = {
	"tt_all_metrics_data":"""
	CREATE TABLE IF NOT EXISTS tt_all_metrics_data (
		id INT AUTO_INCREMENT PRIMARY KEY,
		datestamp DATE,
		project_name VARCHAR(255),
		project_id VARCHAR(255),
		active_addresses_daily INT DEFAULT NULL,
		active_addresses_weekly INT DEFAULT NULL,
		active_addresses_monthly INT DEFAULT NULL,
		active_developers INT DEFAULT NULL,
		active_internet_identities INT DEFAULT NULL,
		active_loans DOUBLE DEFAULT NULL,
		afpu DOUBLE DEFAULT NULL,
		annualized_estimated_rewards DOUBLE DEFAULT NULL,
		arpu DOUBLE DEFAULT NULL,
		assets_staked DOUBLE DEFAULT NULL,
		blocks_per_second DOUBLE DEFAULT NULL,
		bridged_supply DOUBLE DEFAULT NULL,
		block_time DOUBLE DEFAULT NULL,
		bridge_deposits DOUBLE DEFAULT NULL,
		buidl_tokenholders INT DEFAULT NULL,
		capital_deployed DOUBLE DEFAULT NULL,
		ckbtc_total_supply DOUBLE DEFAULT NULL,
		code_commits INT DEFAULT NULL,
		contracts_deployed INT DEFAULT NULL,
		contract_deployers INT DEFAULT NULL,
		cost_of_revenue DOUBLE DEFAULT NULL,
		cycles_burn_rate DOUBLE DEFAULT NULL,
		earnings DOUBLE DEFAULT NULL,
		eurc_outstanding_supply DOUBLE DEFAULT NULL,
		expenses DOUBLE DEFAULT NULL,
		fees DOUBLE DEFAULT NULL,
		fees_supply_side DOUBLE DEFAULT NULL,
		flash_loan_volume DOUBLE DEFAULT NULL,
		gas_used DOUBLE DEFAULT NULL,
		gross_profit DOUBLE DEFAULT NULL,
		liquidity_turnover DOUBLE DEFAULT NULL,
		market_cap_circulating DOUBLE DEFAULT NULL,
		market_cap_fully_diluted DOUBLE DEFAULT NULL,
		message_count INT DEFAULT NULL,
		net_deposits DOUBLE DEFAULT NULL,
		nns_proposals INT DEFAULT NULL,
		number_of_icp_transactions INT DEFAULT NULL,
		notional_trading_volume DOUBLE DEFAULT NULL,
		number_of_neurons INT DEFAULT NULL,
		operating_expenses DOUBLE DEFAULT NULL,
		outstanding_supply DOUBLE DEFAULT NULL,
		pf_circulating DOUBLE DEFAULT NULL,
		pf_fully_diluted DOUBLE DEFAULT NULL,
		price DOUBLE DEFAULT NULL,
		ps_circulating DOUBLE DEFAULT NULL,
		ps_fully_diluted DOUBLE DEFAULT NULL,
		revenue DOUBLE DEFAULT NULL,
		stablecoin_transfer_count DOUBLE DEFAULT NULL,
		stablecoin_dau INT DEFAULT NULL,
		stablecoin_holders INT DEFAULT NULL,
		stablecoin_mau INT DEFAULT NULL,
		stablecoin_transfer_volume DOUBLE DEFAULT NULL,
		stablecoin_mints DOUBLE DEFAULT NULL,
		stablecoin_redemptions DOUBLE DEFAULT NULL,
		stablecoin_wau INT DEFAULT NULL,
		superchain_fees DOUBLE DEFAULT NULL,
		tokenholders INT DEFAULT NULL,
		token_incentives DOUBLE DEFAULT NULL,
		token_supply_circulating DOUBLE DEFAULT NULL,
		token_supply_maximum DOUBLE DEFAULT NULL,
		token_trading_volume DOUBLE DEFAULT NULL,
		token_turnover_circulating DOUBLE DEFAULT NULL,
		token_turnover_fully_diluted DOUBLE DEFAULT NULL,
		total_canister_state DOUBLE DEFAULT NULL,
		total_icp_burned DOUBLE DEFAULT NULL,
		total_icp_staked_on_nns DOUBLE DEFAULT NULL,
		total_internet_identities INT DEFAULT NULL,
		total_transaction_fees DOUBLE DEFAULT NULL,
		tradeable_assets INT DEFAULT NULL,
		tradeable_pairs INT DEFAULT NULL,
		trade_count INT DEFAULT NULL,
		trading_volume DOUBLE DEFAULT NULL,
		trading_volume_avg_per_trade_count DOUBLE DEFAULT NULL,
		trading_volume_avg_per_user DOUBLE DEFAULT NULL,
		transactions_per_second DOUBLE DEFAULT NULL,
		transaction_count INT DEFAULT NULL,
		transaction_count_contracts INT DEFAULT NULL,
		transaction_fee_average DOUBLE DEFAULT NULL,
		transaction_volume DOUBLE DEFAULT NULL,
		transfer_volume DOUBLE DEFAULT NULL,
		treasury DOUBLE DEFAULT NULL,
		treasury_net DOUBLE DEFAULT NULL,
		tvl DOUBLE DEFAULT NULL,
		unique_transacting_wallets INT DEFAULT NULL,
		usdc_outstanding_supply  DOUBLE DEFAULT NULL,
		usdt_outstanding_supply DOUBLE DEFAULT NULL,
		usdt_transfer_volume DOUBLE DEFAULT NULL,
		user_dau INT DEFAULT NULL,
		user_mau INT DEFAULT NULL,
		user_wau INT DEFAULT NULL
		)
	""",
	"tt_all_projects": """
	CREATE TABLE IF NOT EXISTS tt_all_projects (
		id INT AUTO_INCREMENT PRIMARY KEY,
		project_name VARCHAR(255),
		project_id VARCHAR(255),
		symbol VARCHAR(255) DEFAULT NULL,
		project_url VARCHAR(255),
		market_sector VARCHAR(64) DEFAULT NULL,
		date_added DATE
		)
	""",
	"tt_available_project_metrics":"""
	CREATE TABLE IF NOT EXISTS tt_available_project_metrics (
		id INT AUTO_INCREMENT PRIMARY KEY,
		project_metric VARCHAR(64),
		date_added DATE
		)
	""",
	"cb_metric_sma":"""
		CREATE TABLE IF NOT EXISTS cb_metric_sma (
		id INT AUTO_INCREMENT PRIMARY KEY,
		project_id VARCHAR(64) NOT NULL,
		sector VARCHAR (32),
		datestamp DATE NOT NULL,
		metric_name VARCHAR(32) NOT NULL,
		sma_time_period INT NOT NULL,
		sma_value DOUBLE NOT NULL,
		UNIQUE (project_id, datestamp, sma_time_period, metric_name)
		)
	""",
	"tt_available_market_sectors":"""
		CREATE TABLE IF NOT EXISTS tt_available_market_sectors (
		id INT AUTO_INCREMENT PRIMARY KEY,
		market_sector_id VARCHAR(64),
		sector_name VARCHAR(64),
		url VARCHAR(128),
		date_added DATE
		)
	""",
	"art_supported_projects":"""
		CREATE TABLE IF NOT EXISTS art_supported_projects (
		id INT AUTO_INCREMENT PRIMARY KEY,
		date_added DATE,
		artemis_id VARCHAR(64),
		symbol VARCHAR(32)
	)
	""",
	"art_available_project_metrics":"""
		CREATE TABLE IF NOT EXISTS art_available_project_metrics (
		id INT AUTO_INCREMENT PRIMARY KEY,
		project_name VARCHAR(64),
		metric_name VARCHAR(256),
		date_added DATE
		)
	""",
	"art_unique_metrics":"""
		CREATE TABLE IF NOT EXISTS art_unique_metrics (
		id INT AUTO_INCREMENT PRIMARY KEY,
		metric_name VARCHAR(256),
		date_added DATE
		)
		""",
	"art_metric_data":"""
		CREATE TABLE IF NOT EXISTS art_metric_data (
		id INT AUTO_INCREMENT PRIMARY KEY,
		datestamp DATE,
		project_name VARCHAR(128),
		avg_txn_fees DOUBLE DEFAULT NULL,
		daily_txns INT DEFAULT NULL,
		dau INT DEFAULT NULL,
		dau_over_100 DOUBLE DEFAULT NULL,
		dex_volumes DOUBLE DEFAULT NULL,
		circulating_supply DOUBLE DEFAULT NULL,
		fdmc DOUBLE DEFAULT NULL,
		fees DOUBLE DEFAULT NULL,
		mc DOUBLE DEFAULT NULL,
		price DOUBLE DEFAULT NULL,
		revenue DOUBLE DEFAULT NULL,
		stablecoin_mc DOUBLE DEFAULT NULL,
		tvl DOUBLE DEFAULT NULL,
		twitter_followers INT DEFAULT NULL,
		volume_24h DOUBLE DEFAULT NULL,
		UNIQUE (project_name, datestamp)
		)
	""",
	"art_selected_metrics":"""
		CREATE TABLE IF NOT EXISTS art_selected_metrics (
		id INT AUTO_INCREMENT PRIMARY KEY,
		selected_metric VARCHAR(128)
		)
	""",
	"cb_log":"""
		CREATE TABLE IF NOT EXISTS cb_log (
		id INT AUTO_INCREMENT PRIMARY KEY,
		timestamp DATETIME,
		from_source VARCHAR(32),
		warning_lvl VARCHAR(1),
		message TEXT
		)	
	""",
	"bf_sources":"""
		CREATE TABLE IF NOT EXISTS bf_sources (
			id INT AUTO_INCREMENT PRIMARY KEY,
			url VARCHAR(256),
			sheet_name VARCHAR(128),
			UNIQUE(url)
		)
	""",
	"bf_data":"""
		CREATE TABLE IF NOT EXISTS bf_data (
			id INT AUTO_INCREMENT PRIMARY KEY,
			datestamp DATE,
			sheet_name VARCHAR(128),
			total_value DOUBLE,
			total_marketcap DOUBLE,
			UNIQUE (datestamp, sheet_name)
		)
	""",
	"us_fred_data":"""
		CREATE TABLE IF NOT EXISTS us_data (
			id INT AUTO_INCREMENT PRIMARY KEY,
			datestamp DATE,
			unrate DOUBLE DEFAULT NULL,
			m2real DOUBLE DEFAULT NULL,
			m2sl DOUBLE DEFAULT NULL,
			m1real DOUBLE DEFAULT NULL,
			pce DOUBLE DEFAULT NULL,
			pcec96 DOUBLE DEFAULT NULL,
			pcepilfe DOUBLE DEFAULT NULL,
			dspic96 DOUBLE DEFAULT NULL,
			psavert DOUBLE DEFAULT NULL,
			pmsave DOUBLE DEFAULT NULL
		)
	""",
	"art_sf_raw_data":"""
		CREATE TABLE IF NOT EXISTS art_sf_raw_data (
			id INT AUTO_INCREMENT PRIMARY KEY,
			project_name VARCHAR(32),
			datestamp DATE,
			active_leases INT DEFAULT NULL,
			active_providers INT DEFAULT NULL,
			avg_txn_fee DOUBLE DEFAULT NULL,
			avg_cost_per_mib DOUBLE DEFAULT NULL,
			avg_cost_per_mib_gwei DOUBLE DEFAULT NULL,
			avg_cost_per_mib_native DOUBLE DEFAULT NULL,
			avg_mib_per_second DOUBLE DEFAULT NULL,
			automation_fees DOUBLE DEFAULT NULL,
			base_fee DOUBLE DEFAULT NULL,
			base_fee_native DOUBLE DEFAULT NULL,
			blob_fees DOUBLE DEFAULT NULL,
			blob_fees_native DOUBLE DEFAULT NULL,
			blob_size_mib DOUBLE DEFAULT NULL,
			compute_fees_native DOUBLE DEFAULT NULL,
			compute_fees_total_usd DOUBLE DEFAULT NULL,
			compute_fees_usdc DOUBLE DEFAULT NULL,
			ccip_fees DOUBLE DEFAULT NULL,
			circulating_supply DOUBLE DEFAULT NULL,
			censored_blocks INT DEFAULT NULL,
			direct_fees DOUBLE DEFAULT NULL,
			dau INT DEFAULT NULL,
			dau_over_100 INT DEFAULT NULL,
			deduped_stablecoin_transfer_volume DOUBLE DEFAULT NULL,
			dex_volumes DOUBLE DEFAULT NULL,
			earnings DOUBLE DEFAULT NULL,
			fm_fees DOUBLE DEFAULT NULL,
			fdmc DOUBLE DEFAULT NULL,
			fees DOUBLE DEFAULT NULL,
			fees_native DOUBLE DEFAULT NULL,
			high_sleep_users INT DEFAULT NULL,
			issuance DOUBLE DEFAULT NULL,
			l1_data_cost DOUBLE DEFAULT NULL,
			l1_data_cost_native DOUBLE DEFAULT NULL,
			low_sleep_users INT DEFAULT NULL,
			market_cap DOUBLE DEFAULT NULL,
			mau INT DEFAULT NULL,
			mints DOUBLE DEFAULT NULL,
			new_leases INT DEFAULT NULL,
			new_users INT DEFAULT NULL,
			nft_trading_volume DOUBLE DEFAULT NULL,
			non_p2p_stablecoin_transfer_volume DOUBLE DEFAULT NULL,
			non_sybil_users INT DEFAULT NULL,
			non_censored_blocks INT DEFAULT NULL,
			ocr_fees DOUBLE DEFAULT NULL,
			operating_expenses DOUBLE DEFAULT NULL,
			priority_fee DOUBLE DEFAULT NULL,
			priority_fee_native DOUBLE DEFAULT NULL,
			priority_fee_usd DOUBLE DEFAULT NULL,
			percent_censored DOUBLE DEFAULT NULL,
			percent_non_censored DOUBLE DEFAULT NULL,
			percent_semi_censored DOUBLE DEFAULT NULL,
			primary_supply_side_revenue DOUBLE DEFAULT NULL,
			protocol_revenue DOUBLE DEFAULT NULL,
			p2p_native_transfer_volume DOUBLE DEFAULT NULL,
			p2p_stablecoin_transfer_volume DOUBLE DEFAULT NULL,
			p2p_token_transfer_volume DOUBLE DEFAULT NULL,
			p2p_transfer_volume DOUBLE DEFAULT NULL,
			price DOUBLE DEFAULT NULL,
			queue_active_amount DOUBLE DEFAULT NULL,
			queue_entry_amount DOUBLE DEFAULT NULL,
			queue_exit_amount DOUBLE DEFAULT NULL,
			returning_users INT DEFAULT NULL,
			revenue DOUBLE DEFAULT NULL,
			revenue_native DOUBLE DEFAULT NULL,
			semi_censored_blocks INT DEFAULT NULL,
			settlement_volume DOUBLE DEFAULT NULL,
			stablecoin_dau DOUBLE DEFAULT NULL,
			stablecoin_total_supply DOUBLE DEFAULT NULL,
			stablecoin_transfer_volume DOUBLE DEFAULT NULL,
			stablecoin_txns INT DEFAULT NULL,
			sybil_users INT DEFAULT NULL,
			submitters INT DEFAULT NULL,
			secondary_supply_side_revenue DOUBLE DEFAULT NULL,
			transaction_nodes INT DEFAULT NULL,
			total_blocks_produced INT DEFAULT NULL,
			total_burned_native DOUBLE DEFAULT NULL,
			total_fees DOUBLE DEFAULT NULL,
			total_staked_usd DOUBLE DEFAULT NULL,
			total_staked_native DOUBLE DEFAULT NULL,
			tvl DOUBLE DEFAULT NULL,
			txns INT DEFAULT NULL,
			tokenholder_count INT DEFAULT NULL,
			token_incentives INT DEFAULT NULL,
			token_turnover_circulating DOUBLE DEFAULT NULL,
			token_turnover_fdv DOUBLE DEFAULT NULL,
			token_volume DOUBLE DEFAULT NULL,
			total_expenses DOUBLE DEFAULT NULL,
			total_supply_side_revenue DOUBLE DEFAULT NULL,
			treasury_link DOUBLE DEFAULT NULL,
			treasury_usd DOUBLE DEFAULT NULL,
			tvl_link DOUBLE DEFAULT NULL,
			vrf_fees DOUBLE DEFAULT NULL,
			vote_tx_fee_native DOUBLE DEFAULT NULL,
			vote_tx_fee_usd DOUBLE DEFAULT NULL,
			weekly_commits_core_ecosystem INT DEFAULT NULL,
			weekly_commits_sub_ecosystem INT DEFAULT NULL,
			weekly_contracts_deployed INT DEFAULT NULL,
			weekly_contract_deployers INT DEFAULT NULL,
			weekly_developers_core_ecosystem INT DEFAULT NULL,
			weekly_developers_sub_ecosystem INT DEFAULT NULL,
			validator_fees_native DOUBLE DEFAULT NULL,
			validator_fees DOUBLE DEFAULT NULL,
			UNIQUE(project_name, datestamp)
		)
	""",
	"j_raw":"""
		CREATE TABLE IF NOT EXISTS j_raw (
			id INT AUTO_INCREMENT PRIMARY KEY,
			project_name VARCHAR(32),
			datestamp DATE,
			sector VARCHAR(32) DEFAULT NULL,
			avg_txn_fee DOUBLE DEFAULT NULL,
			active_developers INT DEFAULT NULL,
			active_addresses_weekly INT DEFAULT NULL,
			active_loans INT DEFAULT NULL,
			circulating_supply DOUBLE DEFAULT NULL,
			daa INT DEFAULT NULL,
			daa_over_100 INT DEFAULT NULL,
			dex_volume DOUBLE DEFAULT NULL,
			earnings INT DEFAULT NULL,
			fees INT DEFAULT NULL,
			fdmc DOUBLE DEFAULT NULL,
			gross_profit DOUBLE DEFAULT NULL,
			maa INT DEFAULT NULL,
			mc DOUBLE DEFAULT NULL,
			price DOUBLE DEFAULT NULL,
			revenue DOUBLE DEFAULT NULL,
			stablecoin_mc DOUBLE DEFAULT NULL,
			transactions INT DEFAULT NULL,
			tokenholders INT DEFAULT NULL,
			token_supply_circulating DOUBLE DEFAULT NULL,
			token_incentives DOUBLE DEFAULT NULL,
			token_supply_maximum DOUBLE DEFAULT NULL,
			tvl DOUBLE DEFAULT NULL,
			volume_24h_usd DOUBLE DEFAULT NULL,
			weekly_commits_core INT DEFAULT NULL,
			weekly_commits_sub INT DEFAULT NULL,
			weekly_dev_core INT DEFAULT NULL,
			weekly_dev_sub INT DEFAULT NULL,
			weekly_contracts_deployed INT DEFAULT NULL,
			weekly_contract_deployers INT DEFAULT NULL,
			weekly_unique_contract_deployers INT DEFAULT NULL,
			UNIQUE (project_name, datestamp)
	)
	""",
	"sr_projects":"""
		CREATE TABLE IF NOT EXISTS sr_projects (
			id INT AUTO_INCREMENT PRIMARY KEY,
   			date_added DATE DEFAULT CURRENT_TIMESTAMP,
			project_name VARCHAR(64) DEFAULT NULL,
			project_slug VARCHAR(64) DEFAULT NULL,
			project_symbol VARCHAR(64) DEFAULT NULL,
			UNIQUE (project_slug)
		)
	""",
	"sr_selected_projects":"""
		CREATE TABLE IF NOT EXISTS sr_selected_projects (
			id INT AUTO_INCREMENT PRIMARY KEY,
			project_slug VARCHAR(64),
			UNIQUE(project_slug)
		)
	""",
	"sr_raw_data":"""
		CREATE TABLE IF NOT EXISTS sr_raw_data (
			id INT AUTO_INCREMENT PRIMARY KEY,
			project_slug VARCHAR(64),
			datestamp DATE,
			active_validators INT DEFAULT NULL,
			annualized_rewards_usd DOUBLE DEFAULT NULL,
			circulating_percentage DOUBLE DEFAULT NULL,
			daily_trading_volume DOUBLE DEFAULT NULL,
			delegated_tokens DOUBLE DEFAULT NULL,
			inflation_rate DOUBLE DEFAULT NULL,
			net_staking_flow_7d DOUBLE DEFAULT NULL,
			real_reward_rate DOUBLE DEFAULT NULL,
			reward_rate DOUBLE DEFAULT NULL,
			staked_tokens DOUBLE DEFAULT NULL,
			staking_marketcap DOUBLE DEFAULT NULL,
			staking_ratio DOUBLE DEFAULT NULL,
			total_staking_wallets INT DEFAULT NULL,
			total_validators INT DEFAULT NULL
		)
	"""
}

table_data_cq = {
	"exchanges":"""
		CREATE TABLE IF NOT EXISTS cq_exchanges (
		id INT AUTO_INCREMENT PRIMARY KEY,
		exchange_name VARCHAR(32) DEFAULT NULL,
		symbol VARCHAR(32) UNIQUE,
		is_validated INT,
		market_type INT,
		is_spot INT,
		is_derivative INT
		)
	""",
	"btc_exchange_flows_raw_data":"""
		CREATE TABLE IF NOT EXISTS cq_btc_exchange_flows_raw_data (
			id INT AUTO_INCREMENT PRIMARY KEY,
			datestamp DATE,
			exchange_symbol VARCHAR(32),
			reserve_btc DOUBLE DEFAULT NULL,
			reserve_usd DOUBLE DEFAULT NULL,
			netflow_total DOUBLE DEFAULT NULL,
			inflow_total DOUBLE DEFAULT NULL,
			inflow_top10 DOUBLE DEFAULT NULL,
			inflow_mean DOUBLE DEFAULT NULL,
			inflow_mean_ma7 DOUBLE DEFAULT NULL,
			outflow_total DOUBLE DEFAULT NULL,
			outflow_top10 DOUBLE DEFAULT NULL,
			outflow_mean DOUBLE DEFAULT NULL,
			outflow_mean_ma7 DOUBLE DEFAULT NULL,
			transactions_count_inflow DOUBLE DEFAULT NULL,
			transactions_count_outflow DOUBLE DEFAULT NULL,
			addresses_count_inflow DOUBLE DEFAULT NULL,
			addresses_count_outflow DOUBLE DEFAULT NULL,
			flow_total DOUBLE DEFAULT NULL,
			flow_mean DOUBLE DEFAULT NULL,
			transactions_count_flow DOUBLE DEFAULT NULL,
			exchange_whale_ratio DOUBLE DEFAULT NULL,
			fund_flow_ratio DOUBLE DEFAULT NULL,
			stablecoins_ratio DOUBLE DEFAULT NULL,
			stablecoins_ratio_usd DOUBLE DEFAULT NULL,
			UNIQUE (datestamp, exchange_symbol)
		)
	""",
	"btc_miner_raw_data":"""
		CREATE TABLE IF NOT EXISTS cq_btc_miner_raw_data (
			datestamp DATE PRIMARY KEY,
			mpi DOUBLE DEFAULT NULL
		)
	""",
}


view_setup = {
	"v_art_sf_core_1":"""
	CREATE VIEW AS
	SELECT project_name, datestamp, fees, avg_txn_fee, dau, stablecoin_dau, stablecoin_total_supply, stablecoin_transfer_volume, stablecoin_txns, dex_volumes, weekly_commits_core_ecosystem, weekly_contracts_deployed, weekly_developers_core_ecosystem FROM art_sf_raw_data ORDER BY datestamp DESC;
	"""
}



try:
	conn = mysql.connector.connect(host="localhost", database=db_name, user=db_username, password=db_password)
	if conn.is_connected():
		cursor = conn.cursor()
		print("Connected to DB ^.^")

		for table_name, table_def in table_data.items():
			table_check = f"SHOW TABLES LIKE '{table_name}'"
			cursor.execute(table_check)
			result = cursor.fetchone()

			if result:
				print(f"Table '{table_name}' already exists")
			else:
				cursor.execute(table_def)
				conn.commit()
				print(f"Attempting to create new table '{table_name}'")
				cursor.execute(table_check)
				result = cursor.fetchone()
				if result:
					print(f"Table '{table_name}' created successfully")
		
		# art_add_selected_metrics(cursor, conn)
		# bf_first_setup(conn)

	setup_cq = False
	
	# conn_cq = mysql.connector.connect(host="localhost", database="helios-cq", user=db_username, password=db_password)
	# if conn_cq.is_connected() and setup_cq:
	# 	cursor = conn_cq.cursor()
	# 	print("Connected to cq DB ^.^")

	# 	for table_name_cq, table_def_cq in table_data_cq.items():
	# 		table_check = f"SHOW TABLES LIKE '{table_name_cq}'"
	# 		cursor.execute(table_check)
	# 		result = cursor.fetchone()

	# 		if result:
	# 			print(f"Table '{table_name_cq}' already exists")
	# 		else:
	# 			cursor.execute(table_def)
	# 			conn_cq.commit()
	# 			print(f"Attempting to create new table '{table_name_cq}'")
	# 			cursor.execute(table_check)
	# 			result = cursor.fetchone()
	# 			if result:
	# 				print(f"Table '{table_name_cq}' created successfully")



except Error as e:
	print("Error: ", e)

finally:
	if conn.is_connected():
		cursor.close()
		conn.close()
		print("Connection closed")

