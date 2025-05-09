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
        cls, names: list, updated_at: datetime = None, updated_by: str = "", batch_id:str="",
    ) -> Generator[Dict[str, Any], None, None]:
        """
        This Python function retrieves analyst price targets for a list of stock names and yields the
        results.
        
        :param cls: The `cls` parameter in the `get_analyst_price_targets` function refers to the class
        itself. In this context, it is a conventional naming convention to use `cls` as the first
        parameter in a class method, which represents the class itself when the method is called
        :param names: The `names` parameter in the `get_analyst_price_targets` function is a list of
        names for which you want to retrieve analyst price targets. Each name in the list represents a
        stock or company for which you want to fetch analyst price targets
        :type names: list
        :param updated_at: The `updated_at` parameter in the `get_analyst_price_targets` function is a
        datetime parameter that specifies the date and time when the analyst price targets were last
        updated. It is an optional parameter, meaning it can be provided with a specific datetime value
        or left empty (None) if not
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `get_analyst_price_targets` function is a
        string parameter that represents the name or identifier of the user who updated the analyst
        price targets. It is used to track who made the update in the system
        :type updated_by: str
        :param batch_id: The `batch_id` parameter  is a
        string parameter that is used to identify a specific batch of requests or operations. It can be
        used to group related tasks together or to track a specific set of operations within a larger
        process. This parameter allows for
        :type batch_id: str
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
                                            updated_at=updated_at, updated_by=updated_by, batch_id=batch_id)
                yield res
        except Exception as e:
            # swallow
            print("get_analyst_price_targets is complete with FAILURE - Exception: {0}".format(e))
        finally:
            print("continue")