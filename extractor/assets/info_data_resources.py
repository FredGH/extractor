import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.info_data as id
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from  extractor.assets.utils import Utils as utl

class InfoDataResources:

    @classmethod
    @dlt.resource(name="info_data", parallelized=True)
    def get_info_data(
        cls,
        names: list,
        period: str = "1mo",
        updated_at: datetime = None,
        time_: str = "",
        updated_by: str = "",
    ) -> Generator[Dict[str, Any], None, None]:
        """
        The function `get_info_data` retrieves information data for a list of names using external APIs
        and yields the results.
        
        :param cls: The `cls` parameter in the `get_info_data` function is a reference to the class
        itself. In Python, the `cls` parameter is used in class methods to refer to the class object. It
        is similar to the `self` parameter used in instance methods, but `cls` is
        :param names: The `names` parameter in the `get_info_data` function is a list of names for which
        information data needs to be retrieved. Each name in the list corresponds to a specific entity
        or item for which information data will be fetched
        :type names: list
        :param period: The `period` parameter in the `get_info_data` function specifies the time period
        for which you want to retrieve information data. By default, it is set to "1mo", which stands
        for 1 month. You can change this parameter to specify a different time period such as "1d,
        defaults to 1mo
        :type period: str (optional)
        :param updated_at: The `updated_at` parameter in the `get_info_data` function is used to specify
        the date and time at which the data was last updated. It is of type `datetime` and has a default
        value of `None`, which means if no value is provided, it will default to `None
        :type updated_at: datetime
        :param time_: The `time_` parameter in the `get_info_data` function appears to be a string
        parameter that is currently not being used within the function. It is defined as a parameter but
        not utilized in the function body
        :type time_: str
        :param updated_by: The `updated_by` parameter in the `get_info_data` function is a string
        parameter that represents the entity or user who updated the information. It is used to track
        and record who made changes or updates to the data
        :type updated_by: str
        """
        try:
            res = {}
            for name in names:
                ticker = td.TickerData(name).get_ticker
                info_data = id.InfoData(yfTickerData=ticker)
                sub_res = info_data.get_info
                res = utl.collect_dict_data(name=name, 
                                            res=res, sub_res=sub_res, 
                                            tag="get_info_data", 
                                            updated_at=updated_at, updated_by=updated_by)
                yield res
        except Exception as e:
            # swallow
            print("get_info_data is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")