import data_providers.stocks.info_data as id
import data_providers.stocks.ticker_data as td
import datetime
import dlt
import pandas as pd

from typing import Generator, Dict, Any
from utils import Utils as utl

class InfoDataResources():
    
    @classmethod
    @dlt.resource(name="info_data", parallelized=True)
    def get_info_data(cls, names:list, period:str="1mo", updated_at:datetime=None, time_:str="", updated_by:str="")->Generator[Dict[str, Any], None, None]:
        try:
            res = {}
            for name in names:
                ticker = td.TickerData(name).get_ticker
                info_data = id.InfoData(yfTickerData=ticker)
                sub_res = info_data.get_info
                if sub_res is not None:
                    sub_res["name"] = name
                    # parent dic decoration  
                    sub_res = utl.add_audit_info(dest= sub_res, updated_at=updated_at,updated_by=updated_by )
                    # nested child dics decoration  
                    sub_res = utl.add_audit_info_nested_dictionaries(name=name, dict=sub_res, updated_at=updated_at, updated_by=updated_by)
                    # concat
                    res = utl.concat_dicts(dest=res, source=sub_res)
                else:
                    print(f"get_info_data -> None, i.e. not supported for the ticker: {ticker}")    
            if len(res) == 0:
                print(f"get_info_data -> No record found for ticker: {name}")
            print("get_info_data is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print("get_info_data is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")