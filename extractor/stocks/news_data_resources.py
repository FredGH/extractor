import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.news_data as nd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from utils import Utils as utl

class NewsDataResources:

    @classmethod
    @dlt.resource(name="news_data", parallelized=True)
    def get_news_data(
        cls, names: list, updated_at: datetime = None, updated_by: str = ""
    ) -> Generator[Dict[str, Any], None, None]:
        try:
            res = {}
            for name in names:
                ticker = td.TickerData(name).get_ticker
                news_data = nd.NewsData(yfTickerData=ticker)
                sub_res = news_data.get_news
                res = utl.collect_dict_data(name=name, 
                                            res=res, sub_res=sub_res, 
                                            tag="get_news_data", 
                                            updated_at=updated_at, updated_by=updated_by)
                yield res
        except Exception as e:
            # swallow
            print("get_news_data is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")