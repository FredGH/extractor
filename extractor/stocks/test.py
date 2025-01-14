import dlt

from historical_data_resources import HistoricalDataResources as hist_dr
from analysts_data_resources import AnalystsDataSource as analysts_dr
   
if __name__ == "__main__":
    ticker = "MSFT"
    table_name = "stock_balance1"
    pipeline_name = table_name + "_pipeline"
    schema_contract = "evolve"
    write_disposition="replace"
    progress="log"
    destination="postgres"

    # resolve 1
    #https://stackoverflow.com/questions/28079221/json-serializing-non-string-dictionary-keys
    # resolve 2
    #https://dlthub.com/docs/general-usage/resource

    #ticker = td.TickerData(ticker).get_ticker
    #accounting_data = ad.AccountingData(ticker)
    #res = accounting_data.get_balance_sheet
    #res = pd.json_normalize(res)
    #res = res.to_dict(orient='records')

    
    #ticker = td.TickerData(ticker).get_ticker
    #analysts_data = analysis_data.AnalystsData(ticker)
    #print(analysts_data)
    #res = analysts_data.get_ranalyst_price_targets
    ##res = pd.json_normalize(res)
    ##res = res.to_dict(orient='records')
    #print(res)

    resource1= hist_dr.get_historical_data(ticker)
    #resource = AccountingDataDataSource().get_balance_sheet(ticker)
    resource2 = analysts_dr.get_analyst_price_targets(ticker)
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, 
                            progress=progress,
                            destination=destination,
                            dataset_name ="landing")
    load_info = pipeline.run(data=[resource1, resource2], 
                            #table_name=table_name,
                            schema_contract=schema_contract,
                            write_disposition=write_disposition)

    #for key in resource:
    #    print(key)
    #    #yield {key, value}

    
    
    