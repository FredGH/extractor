import dlt

from historical_data_resources import HistoricalDataResources as hist_dr
from analysts_data_resources import AnalystsDataSource as analysts_dr

dict_assets = {}
dict_assets["index_data_extraction_pipeline"] = ["^GSPC","^IXIC","^DJI","^N225", "^FTSE","^GDAXI","^HSI", "^NSEI","^STOXX50E", "^FCHI"]
dict_assets["stock_data_extraction_pipeline"] = ["AAPL","TSLA","NVDA","AMZN", "MSFT","GOOG","GOOGL", "META","TSM", "AMD"]

for key in dict_assets.keys():
    tickers =  dict_assets[key]
    pipeline_name = key 
    schema_contract = "evolve"
    write_disposition="replace"
    progress="log"
    destination="postgres"

    if "stock" in pipeline_name:
        resource1 = hist_dr.get_stock_historical_data(tickers,period="max" )
    elif "index" in pipeline_name:
        resource1= hist_dr.get_index_historical_data(tickers,period="max" )
    else:
        raise Exception("asset class not supported")
    #resource = AccountingDataDataSource().get_balance_sheet(ticker)
    #resource2 = analysts_dr.get_analyst_price_targets(tickers)
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, 
                            progress=progress,
                            destination=destination,
                            dataset_name ="landing")

    load_info = pipeline.run(#data=[resource1, resource2],
                            data=[resource1],
                            schema_contract=schema_contract,
                            write_disposition=write_disposition)


#for key in resource:
#    print(key)
#    #yield {key, value}

    
    
    