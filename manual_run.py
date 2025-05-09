import datetime
from datetime import timezone

import dlt
import uuid
from extractor.assets.analysts_data_resources import AnalystsDataSource as analysts_dr
from extractor.assets.historical_data_resources import HistoricalDataResources as hist_dr
from extractor.assets.info_data_resources import InfoDataResources as info_dr
from extractor.assets.news_data_resources import NewsDataResources as news_data_dr
from extractor.assets.recommendations_data_resources import (
    RecommendationsDataResources as recommendations_data_dr,
)
from extractor.assets.utils import Utils as utl

dict_assets = {}
dict_assets["crypto_data_extraction_pipeline"] = ["USDT","BTC","ETH","USDC", "XRP","SOL","BNB", "DOGE","ADA", "TRX"]
dict_assets["index_data_extraction_pipeline"] = ["^GSPC","^IXIC","^DJI","^N225", "^FTSE","^GDAXI","^HSI", "^NSEI","^STOXX50E", "^FCHI"]
dict_assets["stock_data_extraction_pipeline"] = [
    "AAPL",
    "TSLA",
    "NVDA",
    "AMZN",
    "MSFT",
    "GOOG",
    "GOOGL",
    "META",
    "TSM",
    "AMD",
]
dict_assets["bond_data_extraction_pipeline"] = ["SHY","BIL","BLV","TLT", "LQD","HYG","BND","MUB", "EMB","CBND", "VETA", "EUNH"]
dict_assets["commodity_data_extraction_pipeline"] = ["CL","NG","GC","ZS", "ZC","ZW","HG", "SI","ALI", "LIT", "NICKEL", "COBALT","MNXXF","GPHOF" ]
dict_assets["currency_data_extraction_pipeline"] = ["GBPUSD=X","GBPEUR=X","JPY=X","EUR=X", "CNYUSD=X","AUDUSD=X","CADUSD=X", "CHFUSD=X","HKDUSD=X"]

updated_at = datetime.datetime.now(timezone.utc)
updated_by = "system"
batch_id = str(uuid.uuid4())
for key in dict_assets.keys():
    landing_prefix = key.split("_")[0]
    tickers = dict_assets[key]
    pipeline_name = key
    schema_contract = "evolve"
    write_disposition = "replace"
    progress = "log"
    destination = "postgres"

    res_historical_data = hist_dr.get_historical_data(names=tickers,period="max", updated_at=updated_at,  updated_by=updated_by, batch_id=batch_id)
    res_info_data = info_dr.get_info_data(names=tickers,period="max", updated_at=updated_at,  updated_by=updated_by, batch_id=batch_id)
    ##resource = AccountingDataDataSource().get_balance_sheet(updated_at=updated_at,names=tickers)
    res_analyst_price_targets = analysts_dr.get_analyst_price_targets(names=tickers, updated_at=updated_at,  updated_by=updated_by, batch_id=batch_id)
    res_news = news_data_dr.get_news_data(names=tickers, updated_at=updated_at,  updated_by=updated_by, batch_id=batch_id)
    res_recommendations = recommendations_data_dr.get_recommendations_data(names=tickers, updated_at=updated_at,  updated_by=updated_by, batch_id=batch_id)
    res_recommendations_summary = (
         recommendations_data_dr.get_recommendations_summary_data(
             names=tickers, updated_at=updated_at, updated_by=updated_by, batch_id=batch_id
         )
     )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        progress=progress,
        destination=destination,
        dataset_name="landing_" + landing_prefix,
    )

    load_info = pipeline.run(
        data=[
              res_historical_data,
              res_info_data,
              res_analyst_price_targets,
              res_news,
              res_recommendations,
              res_recommendations_summary
        ],
        schema_contract=schema_contract,
        write_disposition=write_disposition,
    )

# for key in resource:
#    print(key)
#    #yield {key, value}
