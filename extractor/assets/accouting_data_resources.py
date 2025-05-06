import datetime

# from dlt.sources import DltResources
from typing import Any, Dict, Generator

import data_providers.stocks.accounting_data as ad
import data_providers.stocks.analysis_data as analysis_data
import data_providers.stocks.historical_data as hd
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd

class AccountingDataDataResources:
    @classmethod
    @dlt.resource(name="get_balance_sheet_data")
    def get_balance_sheet(cls, name: str) -> Generator[Dict[str, Any], None, None]:
        def dictRecursiveFormat(d):
            for key, val in list(d.items()):
                if isinstance(key, datetime.datetime):
                    val = d.pop(key)
                    d[str(key)] = val
                if isinstance(val, datetime.datetime) and isinstance(
                    key, datetime.datetime
                ):
                    d[str(key)] = str(val)
                elif isinstance(val, datetime.datetime):
                    d[key] = str(val)
                if type(val) is dict:
                    dictRecursiveFormat(val)

        ticker = td.TickerData(name).get_ticker
        accounting_data = ad.AccountingData(yfTickerData=ticker)
        res = accounting_data.get_balance_sheet
        res = dictRecursiveFormat(res)
        res = res.to_dict(orient="records")
        yield res
