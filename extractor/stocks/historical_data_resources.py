import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.historical_data as hd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from utils import Utils as utl

class HistoricalDataResources:

    @classmethod
    @dlt.resource(name="historical_data", parallelized=True)
    def get_historical_data(
        cls,
        names: list,
        period: str = "1mo",
        updated_at: datetime = None,
        updated_by: str = "",
    ) -> Generator[Dict[str, Any], None, None]:
        try:
            res = pd.DataFrame()
            for name in names:
                ticker = td.TickerData(name).get_ticker
                historical_data = hd.HistoricalData(yfTickerData=ticker)
                sub_res = historical_data.get_historical_data(period)
                if sub_res is not None:
                    sub_res["name"] = name
                    sub_res = utl.add_audit_info(
                        dest=sub_res, updated_at=updated_at, updated_by=updated_by
                    )
                    res = utl.concat_dataframes(dest=res, source=sub_res)
                else:
                    print(
                        f"get_historical_data ->  None, i.e. not supported for the ticker: {ticker}"
                    )
            res = res.reset_index()
            res = res.to_dict(orient="records")
            if len(res) == 0:
                print(f"get_historical_data -> No record found for ticker: {name}")
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
