import dlt

from historical_data_resources import HistoricalDataResources as hist_dr
from analysts_data_resources import AnalystsDataSource as analysts_dr
   

tickers = ["MSFT", "VOD.L"]
pipeline_name = "stocks_data_extraction_pipeline"
schema_contract = "evolve"
write_disposition="replace"
progress="log"
destination="postgres"

resource1= hist_dr.get_historical_data(tickers)
#resource = AccountingDataDataSource().get_balance_sheet(ticker)
resource2 = analysts_dr.get_analyst_price_targets(tickers)
pipeline = dlt.pipeline(pipeline_name=pipeline_name, 
                        progress=progress,
                        destination=destination,
                        dataset_name ="landing")

load_info = pipeline.run(data=[resource1, resource2], 
                        schema_contract=schema_contract,
                        write_disposition=write_disposition)

#for key in resource:
#    print(key)
#    #yield {key, value}

    
    
    