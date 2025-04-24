import data_providers.stocks.info_data as id
import data_providers.stocks.ticker_data as td
import utils as u
import dlt
import pandas as pd

from typing import Generator, Dict, Any
from utils import Utils as utl

class InfoDataResources():
    
    @classmethod
    @dlt.resource(name="info_data", parallelized=True)
    def get_info_data(cls, names:list, period:str="1mo")->Generator[Dict[str, Any], None, None]:
        try:
            res = {}
            for name in names:
                ticker = td.TickerData(name).get_ticker
                info_data = id.InfoData(ticker)
                sub_res = info_data.get_info
                sub_res["name"] = name
                #res = utl.concat_dataframes(dest=res, source=sub_res)
                res = utl.concat_dicts(dest=res, source=sub_res)
            #res = res.reset_index()
            #res = res.to_dict(orient='records')
            if len(res) == 0:
                raise Exception(f"No record found for {name}")
            print("get_info_data is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print("get_info_data is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")