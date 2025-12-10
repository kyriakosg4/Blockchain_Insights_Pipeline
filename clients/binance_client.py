import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter   
from typing import Dict, List 
import time 
from datetime import datetime, timezone
import pandas as pd 
import os
from pathlib import Path


BINANCE_BASE = "https://api.binance.com/api/v3"


class Binance_client:
    def __init__(self, base_url:str, timeout:int=10):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        
        retry_strategy = Retry(
            total = 5,
            backoff_factor= 1,
            status_forcelist= [429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]         
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
    
    def _get(self, path:str, params:Dict):
        
        url = f"{self.base_url}{path}"
        
        r = self.session.get(url, params=params, timeout=self.timeout)
        
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After",1))
            time.sleep(wait)
            return self._get(path, params)
        
        # Custom: Binance temporary IP ban (418)
        if r.status_code == 418:
            time.sleep(2)
            raise Exception("Your IP is temporarily banned by Binance (HTTP 418).")

        r.raise_for_status()
        return r.json()
    
    
    def get_klines(self, symbol:str, interval:str, start:int, end:int) -> List[Dict]:
        
        all_candles = []
        fetch_start = start # we set it like that because the fetch stary will change every loop 
        
        while True:
            params = {
                "symbol" : symbol.upper(),
                "interval": interval,
                "startTime": fetch_start,
                "endTime": end,
                "limit": 1000
            }
            
            raw = self._get("/klines", params)
            
            if not raw:
                break
            
            for c in raw:
                all_candles.append({
                    "close_time" : datetime.fromtimestamp(c[6]/1000, tz = timezone.utc).isoformat(),
                    "close" : float(c[4]),
                    "volume" : float(c[5]),
                    "trades" : int(c[8])          
                })
                
            
            if len(raw) < 1000:
                break
            
            fetch_start = raw[-1][6]+1 # in case the candles will be more than the limit, we start the new loop considering the last close time
            
        return all_candles
    
    
    def get_hourly_prices(self, symbol:str, interval:str, start_dt:str, end_dt:str):
        
        if isinstance(start_dt, str):
            start_dt = datetime.fromisoformat(start_dt)
        if isinstance(end_dt, str):
            end_dt = datetime.fromisoformat(end_dt)
            
        start_ms = int(start_dt.timestamp()*1000)
        end_ms = int(end_dt.timestamp()*1000)
        
        return self.get_klines(symbol, interval, start_ms, end_ms)

            
    def save_csv_klines(self, symbol: str, interval: str, candles: List[Dict], base_path: str):
        if not candles:
            print("No candles returned")
            return

        df = pd.DataFrame(candles)

        df["close_time_iso"] = pd.to_datetime(df["close_time"])
        df["year_month"] = df["close_time_iso"].dt.strftime("%Y-%m")

        for ym, group in df.groupby("year_month"):

            month_folder = os.path.join(base_path, "market_metrics", ym)
            os.makedirs(month_folder, exist_ok=True)

            filename = f"{symbol.upper()}_{interval}.csv"
            csv_path = os.path.join(month_folder, filename)   # ← this was missing

            file_exists = os.path.exists(csv_path)

            group.to_csv(
                csv_path,
                index=False,
                mode="a" if file_exists else "w",
                header=not file_exists,
            )

            print(f"Saved {len(group)} rows → {csv_path} (append={file_exists})")

    
    
    def append_realtime_kline(
    self,
    symbol: str,
    interval: str,
    candle: dict,
    base_path: str
):
        """
        Appends ONE kline (candle row) to the monthly CSV.
        Creates directory & file if missing.
        """

        # Determine year-month folder
        dt = datetime.fromisoformat(candle["close_time"])
        month_key = f"{dt.year:04d}-{dt.month:02d}"

        # Directory: e.g. /data/market_metrics/2025-11/
        out_dir = Path(base_path) / "market_metrics" / month_key
        out_dir.mkdir(parents=True, exist_ok=True)

        # File: e.g. BTCUSDT_1h_2025-11.csv
        csv_path = out_dir / f"{symbol}_{interval}_{month_key}.csv"

        # Transform candle into DataFrame for append
        df = pd.DataFrame([candle])

        # Append mode — write header only if file does NOT exist
        file_exists = csv_path.exists()
        df.to_csv(
            csv_path,
            mode="a",
            index=False,
            header=not file_exists # True when the file is not existing
        )

        print(f"[Binance Realtime] Appended 1 candle → {csv_path}")
        return str(csv_path)
