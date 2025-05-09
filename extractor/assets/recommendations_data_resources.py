import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.recommendations_data as rd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from  extractor.assets.utils import Utils as utl

class RecommendationsDataResources:

    @classmethod
    @dlt.resource(name="recommendations_data", parallelized=True)
    def get_recommendations_data(
        cls, names: list, updated_at: datetime = None, updated_by: str = "", batch_id:str=""
    ) -> Generator[Dict[str, Any], None, None]:
        """
        The function `get_recommendations_data` retrieves recommendations data for a list of stock names
        and adds audit information before yielding the results.
        
        :param cls: In the provided function `get_recommendations_data`, the parameter `cls` appears to
        be a class reference. It is commonly used in class methods in Python to refer to the class
        itself. This parameter allows the method to access class-level variables and methods
        :param names: The `names` parameter in the `get_recommendations_data` function is a list of
        strings representing the names of tickers for which you want to retrieve recommendations data
        :type names: list
        :param updated_at: The `updated_at` parameter in the `get_recommendations_data` function is a
        datetime object that specifies the timestamp at which the data was last updated. It is an
        optional parameter, meaning it can be provided with a specific datetime value or left as `None`
        if not applicable. This parameter is
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `get_recommendations_data` function is a
        string that represents the user or entity who updated the recommendations data. It is used to
        track and log information about who made changes or updates to the data
        :type updated_by: str
        :param batch_id: The `batch_id` parameter  is a
        string parameter that is used to identify a specific batch of requests or operations. It can be
        used to group related tasks together or to track a specific set of operations within a larger
        process. This parameter allows for
        :type batch_id: str
        """
        try:
            res = pd.DataFrame()
            for name in names:
                ticker = td.TickerData(name).get_ticker
                recommendations_data = rd.RecommendationsData(yfTickerData=ticker)
                sub_res = recommendations_data.get_recommendations
                res = utl.collect_dataframe_data(name=name, 
                                                res=res, sub_res=sub_res, 
                                                tag="recommendations_data", 
                                                updated_at=updated_at, updated_by=updated_by, batch_id=batch_id)
            res = res.reset_index()
            res = res.to_dict(orient="records")
            print("get_recommendations_data is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print(
                "get_recommendations_data is complete with FAILURE - Exception: {0}".format(
                    e
                )
            )
        finally:
            print("continue")

    @classmethod
    @dlt.resource(name="recommendations_summary_data", parallelized=True)
    def get_recommendations_summary_data(
        cls, names: list, updated_at: datetime = None, updated_by: str = "", batch_id:str=""
    ) -> Generator[Dict[str, Any], None, None]:
        """
        This function retrieves recommendations summary data for a list of stock tickers and adds audit
        information before yielding the results.
        
        :param cls: The `cls` parameter in the `get_recommendations_summary_data` function is a
        conventional name used in Python to represent the class itself. It is used as the first
        parameter in class methods to refer to the class itself within the method
        :param names: The `names` parameter in the `get_recommendations_summary_data` function is a list
        of strings representing the names of tickers for which you want to retrieve recommendations
        summary data
        :type names: list
        :param updated_at: The `updated_at` parameter in the `get_recommendations_summary_data` function
        is a datetime object that specifies the timestamp at which the data was last updated. It is an
        optional parameter, meaning if no value is provided, it defaults to `None`. This parameter is
        used to track when the data
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `get_recommendations_summary_data` function
        is a string parameter that represents the user or entity who updated the recommendations summary
        data. It is used to track and store information about who made the last update to the data
        :type updated_by: str
        :param batch_id: The `batch_id` parameter  is a
        string parameter that is used to identify a specific batch of requests or operations. It can be
        used to group related tasks together or to track a specific set of operations within a larger
        process. This parameter allows for
        :type batch_id: str

        """
        try:
            res = pd.DataFrame()
            for name in names:
                ticker = td.TickerData(name).get_ticker
                recommendations_data = rd.RecommendationsData(yfTickerData=ticker)
                sub_res = recommendations_data.get_recommendations_summary
                res = utl.collect_dataframe_data(name=name, 
                                                res=res, sub_res=sub_res, 
                                                tag="recommendations_summary_data", 
                                                updated_at=updated_at, updated_by=updated_by, batch_id=batch_id)
            res = res.reset_index()
            res = res.to_dict(orient="records")
            print("get_recommendations_summary_data is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print(
                "get_recommendations_summary_data is complete with FAILURE - Exception: {0}".format(
                    e
                )
            )
        finally:
            print("continue")
