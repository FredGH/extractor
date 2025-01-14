import data_providers.stocks.analysis_data as ad
import data_providers.stocks.ticker_data as td
import dlt
import pandas as pd

from typing import Generator, Dict, Any

class AnalystsDataSource():
    @classmethod
    @dlt.resource(name="analyst_price_targets",parallelized=True )
    def get_analyst_price_targets(cls, name:str)->Generator[Dict[str, Any], None, None]:
        ticker = td.TickerData(name).get_ticker
        analysts_data = ad.AnalystsData(ticker)
        res = analysts_data.get_ranalyst_price_targets
        yield res

