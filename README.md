# crypto_data_gathering

This is a bunch of python scripts to get data from multiple sources and gather them into a single database for furthur analysis.

This was a project I worked on with an analyst from bloomberg

Data References:
- Artemis API = m_art.py (Pull data from the API, store in local DB)
- Artemis SnowFlake DB = m_art_snowflake.py (Pull data from db, store in local DB)
- Bitformance CSV Sheets = m_bitformance.py (Pull data from multiple CSV sheet URLs, store in DB)
- CryptoQuant = m_cq.py (WIP)
- FRED (Federal Reserve Economic Data) = m_fred.py (Pull data from the Fed API, store in local DB)


Other Files
- Custom functions = m_functions.py (Calculate simple moving averages, aggregate all important metrics into 1 table)
- Database Setup = setup_dbs.py (Script to set up all tables with the necessary columns)
- Bitformance Data Sources = Bitformance_urls.txt (URLs used for initial import into DB)
