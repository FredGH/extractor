import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.news_data as nd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from  extractor.assets.utils import Utils as utl

class NewsDataResources:

    @classmethod
    @dlt.resource(name="news_data", parallelized=True)
    def get_news_data(
        cls, names: list, updated_at: datetime = None, updated_by: str = ""
    ) -> Generator[Dict[str, Any], None, None]:
        """
        The function `get_news_data` retrieves news data for a list of names and yields a dictionary
        containing the collected data.
        
        :param cls: In the provided function `get_news_data`, the parameter `cls` appears to be a class
        method. It is commonly used in Python as the first parameter in a class method to refer to the
        class itself. This parameter allows the method to access class variables and methods
        :param names: The `names` parameter in the `get_news_data` function is a list of names for which
        news data needs to be retrieved. Each name in the list corresponds to a specific entity or topic
        for which news data will be fetched
        :type names: list
        :param updated_at: The `updated_at` parameter in the `get_news_data` function is a datetime
        parameter that specifies the date and time when the news data was last updated. It has a default
        value of `None`, which means if no value is provided when calling the function, it will default
        to `None`
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `get_news_data` function is a string
        parameter that specifies the name of the user who updated the news data. It has a default value
        of an empty string, which means if no value is provided when calling the function, it will
        default to an empty string
        :type updated_by: str
        """
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