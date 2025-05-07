import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.analysts_data as ad
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from extractor.assets.utils import Utils as utl

class AnalystsDataSource:
    @classmethod
    @dlt.resource(name="analyst_price_targets", parallelized=True)
    def get_analyst_price_targets(
        cls, names: list, updated_at: datetime = None, updated_by: str = ""
    ) -> Generator[Dict[str, Any], None, None]:
        """
        This Python function retrieves analyst price targets for a list of stock names and yields the
        results.
        
        :param cls: The `cls` parameter in the `get_analyst_price_targets` function refers to the class
        itself. In Python, the first parameter of a class method is always a reference to the class
        itself, conventionally named `cls`. This parameter is used to access class variables and methods
        within the method
        :param names: The `names` parameter in the `get_analyst_price_targets` function is expected to
        be a list of strings representing the names of stocks or companies for which you want to
        retrieve analyst price targets
        :type names: list
        :param updated_at: The `updated_at` parameter in the `get_analyst_price_targets` function is
        used to specify the date and time at which the analyst price targets were last updated. It is an
        optional parameter that defaults to `None` if not provided. This parameter allows you to track
        when the analyst price
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `get_analyst_price_targets` function is a
        string parameter that represents the name or identifier of the user who updated the analyst
        price targets. It is used to track who made the update to the analyst price targets data
        :type updated_by: str
        """
        res = {}
        try:
            for name in names:
                ticker = td.TickerData(name).get_ticker
                analysts_data = ad.AnalystsData(yfTickerData=ticker)
                sub_res = analysts_data.get_analyst_price_targets
                res = utl.collect_dict_data(name=name, 
                                            res=res, sub_res=sub_res, 
                                            tag="get_analyst_price_targets", 
                                            updated_at=updated_at, updated_by=updated_by)
                yield res
        except Exception as e:
            # swallow
            print("get_analyst_price_targets is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")