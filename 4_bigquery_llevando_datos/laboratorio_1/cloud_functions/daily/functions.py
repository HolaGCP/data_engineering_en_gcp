# %%
import requests
from pathlib import Path
import pandas as pd
import decimal
import zipfile
import time
import sys
from datetime import datetime
import logging

TABLE_SCHEMA=[
        {"name":"open_time","type":"TIMESTAMP"},
        {"name":"open","type":"NUMERIC"},
        {"name":"high","type":"NUMERIC"},
        {"name":"low","type":"NUMERIC"},
        {"name":"close","type":"NUMERIC"},
        {"name":"volume","type":"NUMERIC"},
        {"name":"close_time","type":"TIMESTAMP"},
        {"name":"quote_asset_volume","type":"NUMERIC"},
        {"name":"number_trades","type":"INT64"},
        {"name":"taker_buy_base_asset_volume","type":"NUMERIC"},
        {"name":"taker_buy_quote_asset_volume","type":"NUMERIC"},
        {"name":"ignore","type":"NUMERIC"},
    ]

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def download_klines(pair_coin,date,fq="1m"):
    base_url = "https://data.binance.vision/data/spot/daily/klines"
    url = f"{base_url}/{pair_coin}/{fq}/{pair_coin}-{fq}-{date}.zip"
    r = requests.get(url)
    if sys.getsizeof(r.content)<10000:
        return False
    Path("tmp").mkdir(parents=True, exist_ok=True)
    open(f"tmp/{pair_coin}-{fq}-{date}.zip","wb").write(r.content)
    zipfile_name = f"./tmp/{pair_coin}-{fq}-{date}"
    #logging.info(zipfile_name)
    with zipfile.ZipFile(f"{zipfile_name}.zip", 'r') as zip_ref:
        zip_ref.extractall("./tmp/")
    df = pd.read_csv(f"{zipfile_name}.csv",
        header=None, names=[col["name"] for col in TABLE_SCHEMA])
    df["open_time"] = pd.to_datetime(df['open_time'], unit='ms')
    df["close_time"] = pd.to_datetime(df['close_time'], unit='ms')
    df["open"] = df["open"].astype(str).map(decimal.Decimal)
    df["high"] = df["high"].astype(str).map(decimal.Decimal)
    df["low"] = df["low"].astype(str).map(decimal.Decimal)
    df["close"] = df["close"].astype(str).map(decimal.Decimal)
    df["volume"] = df["volume"].astype(str).map(decimal.Decimal)
    df["quote_asset_volume"] = df["quote_asset_volume"].astype(str).map(decimal.Decimal)
    df["taker_buy_base_asset_volume"] = df["taker_buy_base_asset_volume"].astype(str).map(decimal.Decimal)
    df["taker_buy_quote_asset_volume"] = df["taker_buy_quote_asset_volume"].astype(str).map(decimal.Decimal)
    return df

# %%
#df = download_klines("BTCUSDT","2022-08-08")
# %%
#df.to_csv("gs://rimac-arnold-huete/binance-data/monthly/BTCUSDT-1m-2022-08-08.csv",index=False, header=False)
# %%
#pd.date_range(start='2022-07-01', end='2022-07-31')
# %%
def main():
    for dt in pd.date_range(start='2022-07-01', end=datetime.now().strftime("%Y-%m-%d")):
        dt_str = dt.strftime("%Y-%m-%d")
        logging.info(f"downloading {dt_str}")
        df = download_klines("BTCUSDT",dt_str)
        df.to_csv(f"gs://rimac-arnold-huete/binance-data/daily/BTCUSDT-1m-{dt_str}.csv",
            index=False, header=False)
        if dt_str>='2022-07-31':
            logging.info("sleeping 5 minutes")
            time.sleep(300)
# %%
#main()
# %%
