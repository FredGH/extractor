import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.recommendations_data as rd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from utils import Utils as utl


class RecommendationsDataResources:

    @classmethod
    @dlt.resource(name="recommendations_data", parallelized=True)
    def get_recommendations_data(
        cls, names: list, updated_at: datetime = None, updated_by: str = ""
    ) -> Generator[Dict[str, Any], None, None]:
        try:
            res = pd.DataFrame()
            for name in names:
                ticker = td.TickerData(name).get_ticker
                recommendations_data = rd.RecommendationsData(yfTickerData=ticker)
                sub_res = recommendations_data.get_recommendations
                if sub_res is not None:
                    sub_res["name"] = name
                    sub_res = utl.add_audit_info(
                        dest=sub_res, updated_at=updated_at, updated_by=updated_by
                    )
                    res = utl.concat_dataframes(dest=res, source=sub_res)
                else:
                    print(
                        f"get_recommendations_data ->  None, i.e. not supported for the ticker: {ticker}"
                    )
            res = res.reset_index()
            res = res.to_dict(orient="records")
            if len(res) == 0:
                print(f"get_recommendations_data -> No record found for ticker: {name}")
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
        cls, names: list, updated_at: datetime = None, updated_by: str = ""
    ) -> Generator[Dict[str, Any], None, None]:
        try:
            res = pd.DataFrame()
            for name in names:
                ticker = td.TickerData(name).get_ticker
                recommendations_data = rd.RecommendationsData(yfTickerData=ticker)
                sub_res = recommendations_data.get_recommendations_summary
                if sub_res is not None:
                    sub_res["name"] = name
                    sub_res = utl.add_audit_info(
                        dest=sub_res, updated_at=updated_at, updated_by=updated_by
                    )
                    res = utl.concat_dataframes(dest=res, source=sub_res)
                else:
                    print(
                        f"recommendations_summary_data ->  None, i.e. not supported for the ticker: {ticker}"
                    )
            res = res.reset_index()
            res = res.to_dict(orient="records")
            if len(res) == 0:
                print(
                    f"recommendations_summary_data -> No record found for ticker: {name}"
                )
            print("recommendations_summary_data is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print(
                "recommendations_summary_data is complete with FAILURE - Exception: {0}".format(
                    e
                )
            )
        finally:
            print("continue")
