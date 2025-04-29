import data_providers.stocks.news_data as nd
import data_providers.stocks.ticker_data as td
import datetime
import dlt
import pandas as pd

from typing import Generator, Dict, Any
from utils import Utils as utl

class NewsDataResources():
    
    @classmethod
    @dlt.resource(name="news_data", parallelized=True)
    def get_news_data(cls, names:list, updated_at:datetime=None, updated_by:str="")->Generator[Dict[str, Any], None, None]:
        try:
            res = {}
            for name in names:
                ticker = td.TickerData(name).get_ticker
                news_data = nd.NewsData(yfTickerData=ticker)
                sub_res = news_data.get_news
                if len(sub_res)>0:
                    for sub_res_item in sub_res:
                        if sub_res_item is not None:
                            sub_res_item["name"] = name
                            # parent dic decoration  
                            sub_res_item = utl.add_audit_info(dest= sub_res_item, updated_at=updated_at,updated_by=updated_by )
                            # nested child dics decoration  
                            sub_res_item = utl.add_audit_info_nested_dictionaries(name=name, dict=sub_res_item, updated_at=updated_at, updated_by=updated_by)
                            # concat
                            res = utl.concat_dicts(dest=res, source=sub_res_item)
                        else:
                            print(f"get_news_data -> None, i.e. not supported for the ticker: {ticker}")    
                    if len(res) == 0:
                        print(f"get_news_data -> No record found for ticker: {name}")
                print("get_news_data is complete with SUCCESS")
                yield res
        except Exception as e:
            # swallow
            print("get_news_data is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")