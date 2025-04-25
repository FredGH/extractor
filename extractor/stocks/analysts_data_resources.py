import data_providers.stocks.analysts_data as ad
import data_providers.stocks.ticker_data as td
import datetime
import dlt
import pandas as pd

from typing import Generator, Dict, Any
from utils import Utils as utl

class AnalystsDataSource():
    @classmethod
    @dlt.resource(name="analyst_price_targets",parallelized=True )
    def get_analyst_price_targets(cls,updated_at:datetime, names:list)->Generator[Dict[str, Any], None, None]:
        try:
            res = {}
            for name in names:
                ticker = td.TickerData(name).get_ticker
                analysts_data = ad.AnalystsData(ticker)
                sub_res = analysts_data.get_analyst_price_targets
                sub_res["name"] = name
                if sub_res is not None:
                    sub_res = utl.add_audit_info(dest= sub_res, updated_at=updated_at)
                    res = utl.concat_dicts(dest=res, source=sub_res)
                else:
                    print(f"get_analyst_price_targets -> None, i.e. not supported for the ticker: {ticker}")    
            if len(res) == 0:
                print(f"get_analyst_price_targets -> No record found for ticker: {name}")
            print("get_analyst_price_targets is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print("get_analyst_price_targets is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")
            

         


