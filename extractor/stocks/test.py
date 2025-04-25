import dlt

from historical_data_resources import HistoricalDataResources as hist_dr
from info_data_resources import InfoDataResources as info_dr
from analysts_data_resources import AnalystsDataSource as analysts_dr
from utils import Utils as utl

dict_assets = {}
dict_assets["crypto_data_extraction_pipeline"] = ["USDT","BTC","ETH","USDC", "XRP","SOL","BNB", "DOGE","ADA", "TRX"]
dict_assets["index_data_extraction_pipeline"] = ["^GSPC","^IXIC","^DJI","^N225", "^FTSE","^GDAXI","^HSI", "^NSEI","^STOXX50E", "^FCHI"]
dict_assets["stock_data_extraction_pipeline"] = ["AAPL","TSLA","NVDA","AMZN", "MSFT","GOOG","GOOGL", "META","TSM", "AMD"]
dict_assets["bond_data_extraction_pipeline"] = ["SHY","BIL","BLV","TLT", "LQD","HYG","BND","MUB", "EMB","CBND", "VETA", "EUNH"]
dict_assets["commodity_data_extraction_pipeline"] = ["CL","NG","GC","ZS", "ZC","ZW","HG", "SI","ALI", "LIT", "NICKEL", "COBALT","MNXXF","GPHOF" ]
dict_assets["currency_data_extraction_pipeline"] = ["GBPUSD=X","GBPEUR=X","JPY=X","EUR=X", "CNYUSD=X","AUDUSD=X","CADUSD=X", "CHFUSD=X","HKDUSD=X"]

#2. create a class for updated_date, updated_by

updated_at = utl.get_utc_now
for key in dict_assets.keys():
    landing_prefix = key.split("_")[0] 
    tickers =  dict_assets[key]
    pipeline_name = key 
    schema_contract = "evolve"
    write_disposition="replace"
    progress="log"
    destination="postgres"

    res_historical_data = hist_dr.get_historical_data(updated_at=updated_at,names=tickers,period="max")
    res_info_data = info_dr.get_info_data(updated_at=updated_at,names=tickers,period="max")
    #resource = AccountingDataDataSource().get_balance_sheet(updated_at=updated_at,names=tickers)
    res_analyst_price_targets = analysts_dr.get_analyst_price_targets(updated_at=updated_at,names=tickers)
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, 
                            progress=progress,
                            destination=destination,
                            dataset_name ="landing_"+landing_prefix)

    load_info = pipeline.run(data=[
                                  #res_historical_data,
                                   res_info_data, 
                                   #res_analyst_price_targets
                                  ],
                            schema_contract=schema_contract,
                            write_disposition=write_disposition)


#for key in resource:
#    print(key)
#    #yield {key, value}

    
    
    