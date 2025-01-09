import data_providers.stocks.historical_data as hd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd

from typing import Generator, Dict, Any

class StockHistoricalDataSource():

    @classmethod
    @dlt.resource(name="get_stock_historical_data")
    def get_stock_historical_data(cls, name:str, period:str="1mo")->Generator[Dict[str, Any], None, None]:
        ticker = td.TickerData(name).get_ticker
        historical_data = hd.HistoricalData(ticker)
        res = historical_data.get_historical_data(period)
        res = res.reset_index()
        res["Date"] = pd.to_datetime(res["Date"]).dt.date
        res = res.to_dict(ortient='records')
        yield res

if __name__ == "__main__":
    pipeline_name = "stock_historical_data_pipeline"
    ticker = "MSFT"
    resource = StockHistoricalDataSource().get_stock_historical_data(ticker)
    for key, value in resource.items():
        print(key, value)
        #yield {key, value}
