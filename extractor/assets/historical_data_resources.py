import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.historical_data as hd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from extractor.assets.utils import Utils as utl

class HistoricalDataResources:

    @classmethod
    @dlt.resource(name="historical_data", parallelized=True)
    def get_historical_data(
        cls,
        names: list,
        period: str = "1mo",
        updated_at: datetime = None,
        updated_by: str = "",
        batch_id:str=""
    ) -> Generator[Dict[str, Any], None, None]:
        """
        The function `get_historical_data_old` retrieves historical data for a list of stock tickers and
        yields the results as a generator.
        
        :param cls: The `cls` parameter in the `get_historical_data_old` method refers to the class
        itself. In this case, it is a class method, and `cls` is used as a conventional name for the
        class itself. It is not used within the method implementation provided, so it can be
        :param names: The `names` parameter in the `get_historical_data_old` function is a list of stock
        ticker symbols for which you want to retrieve historical data. For example, if you want
        historical data for Apple Inc., you would include the ticker symbol "AAPL" in the `names` list
        :type names: list
        :param period: The `period` parameter in the `get_historical_data_old` function is used to
        specify the time period for which historical data should be retrieved. It has a default value of
        "1mo", which stands for 1 month. This parameter allows the user to define the duration of
        historical data they, defaults to 1mo
        :type period: str (optional)
        :param updated_at: The `updated_at` parameter in the `get_historical_data_old` method is a
        datetime object that represents the timestamp indicating when the historical data was last
        updated. It is an optional parameter that defaults to `None` if not provided. This parameter
        allows you to specify the date and time of the
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `get_historical_data_old` function is a
        string parameter that specifies the user or entity who updated the historical data. It is used
        to track and record the user responsible for any updates made to the historical data during the
        function execution
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
                historical_data = hd.HistoricalData(yfTickerData=ticker)
                sub_res = historical_data.get_historical_data(period)
                res = utl.collect_dataframe_data(name=name, 
                                                res=res, sub_res=sub_res, 
                                                tag="historical_data", 
                                                updated_at=updated_at, updated_by=updated_by, batch_id=batch_id)
            res = res.reset_index()
            res = res.to_dict(orient="records")
            print("get_historical_data is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print(
                "get_historical_data is complete with FAILURE - Exception: {0}".format(
                    e
                )
            )
        finally:
            print("continue")
