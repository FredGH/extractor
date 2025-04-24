import dlt

from historical_data_resources import HistoricalDataResources as hist_dr
from analysts_data_resources import AnalystsDataSource as analysts_dr

dict_assets = {}
dict_assets["index_data_extraction_pipeline"] = ["^GSPC","^IXIC","^DJI","^N225", "^FTSE","^GDAXI","^HSI", "^NSEI","^STOXX50E", "^FCHI"]
dict_assets["stock_data_extraction_pipeline"] = ["AAPL","TSLA","NVDA","AMZN", "MSFT","GOOG","GOOGL", "META","TSM", "AMD"]
dict_assets["bond_data_extraction_pipeline"] = ["SHY","BIL","BLV","TLT", "LQD","HYG","BND","MUB", "EMB","CBND", "VETA", "EUNH"]
dict_assets["commodity_data_extraction_pipeline"] = ["CL","NG","GC","ZS", "ZC","ZW","HG", "SI","ALI", "LIT", "NICKEL", "COBALT","MNXXF","GPHOF" ]
dict_assets["currency_data_extraction_pipeline"] = ["GBPUSD=X","GBPEUR=X","JPY=X","EUR=X", "CNYUSD=X","AUDUSD=X","CADUSD=X", "CHFUSD=X","HKDUSD=X"]
dict_assets["crypto_data_extraction_pipeline"] = ["USDT","BTC","ETH","USDC", "XRP","SOL","BNB", "DOGE","ADA", "TRX"]

for key in dict_assets.keys():
    landing_prefix = key.split("_")[0] 
    tickers =  dict_assets[key]
    pipeline_name = key 
    schema_contract = "evolve"
    write_disposition="replace"
    progress="log"
    destination="postgres"

    resource1 = hist_dr.get_historical_data(tickers,period="max" )
    #resource = AccountingDataDataSource().get_balance_sheet(ticker)
    resource2 = analysts_dr.get_analyst_price_targets(tickers)
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, 
                            progress=progress,
                            destination=destination,
                            dataset_name ="landing_"+landing_prefix)

    load_info = pipeline.run(data=[resource1, resource2],
                            schema_contract=schema_contract,
                            write_disposition=write_disposition)


#for key in resource:
#    print(key)
#    #yield {key, value}

    
    
    