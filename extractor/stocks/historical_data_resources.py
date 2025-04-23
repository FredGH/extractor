import data_providers.stocks.historical_data as hd
import data_providers.stocks.ticker_data as td
import utils as u
import dlt
import pandas as pd

from typing import Generator, Dict, Any
from utils import Utils as utl

class HistoricalDataResources():
    
    @classmethod
    def __get_historical_data(cls, names:list, period:str="1mo")->Generator[Dict[str, Any], None, None]:
        try:
            res = pd.DataFrame()
            for name in names:
                ticker = td.TickerData(name).get_ticker
                historical_data = hd.HistoricalData(ticker)
                sub_res = historical_data.get_historical_data(period)
                sub_res["name"] = name
                res = utl.concat_dataframes(dest=res, source=sub_res)
            res = res.reset_index()
            res = res.to_dict(orient='records')
            if len(res) == 0:
                raise Exception(f"No record found for {name}")
            print("get_historical_data is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print("get_historical_data is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")
    
    @classmethod
    @dlt.resource(name="stock_historical_data", parallelized=True)
    def get_stock_historical_data(cls, names:list, period:str="1mo")->Generator[Dict[str, Any], None, None]:
        yield cls.__get_historical_data( names = names,
                                          period = period)
    
    @classmethod
    @dlt.resource(name="index_historical_data", parallelized=True)
    def get_index_historical_data(cls, names:list, period:str="1mo")->Generator[Dict[str, Any], None, None]:
        yield cls.__get_historical_data( names = names,
                                         period = period)
 
