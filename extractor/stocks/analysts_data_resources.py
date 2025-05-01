import datetime
from typing import Any, Dict, Generator

import data_providers.stocks.analysts_data as ad
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd
from utils import Utils as utl

class AnalystsDataSource:
    @classmethod
    @dlt.resource(name="analyst_price_targets", parallelized=True)
    def get_analyst_price_targets(
        cls, names: list, updated_at: datetime = None, updated_by: str = ""
    ) -> Generator[Dict[str, Any], None, None]:
        try:
            res = {}
            for name in names:
                ticker = td.TickerData(name).get_ticker
                analysts_data = ad.AnalystsData(yfTickerData=ticker)
                sub_res = analysts_data.get_analyst_price_targets
                if sub_res is not None:
                    sub_res["name"] = name
                    sub_res = utl.add_audit_info(
                        dest=sub_res, updated_at=updated_at, updated_by=updated_by
                    )
                    res = utl.concat_dicts(dest=res, source=sub_res)
                else:
                    print(
                        f"get_analyst_price_targets -> None, i.e. not supported for the ticker: {ticker}"
                    )
            if len(res) == 0:
                print(
                    f"get_analyst_price_targets -> No record found for ticker: {name}"
                )
            print("get_analyst_price_targets is complete with SUCCESS")
            yield res
        except Exception as e:
            # swallow
            print(
                "get_analyst_price_targets is complete with FAILURE - Exception: {0}".format(
                    e
                )
            )
        finally:
            print("continue")
