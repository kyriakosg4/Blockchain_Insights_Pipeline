from clients.binance_client import Binance_client, BINANCE_BASE
from datetime import datetime, timedelta
from clients.etherscan_client import EtherscanClient, ETHEREUM_CHAIN_ID, ETHERSCAN_API_KEY, ETHERSCAN_BASE
import time
from loader.load_incremental_to_postgres import load_last_block_row, load_last_tx_rows, load_last_price_row


BINANCE_INTERVAL = 3600       # 1 hour
ETHERSCAN_INTERVAL = 29*60       # Ethereum block time
SYMBOL = "ETHUSDT"
INTERVAL = "1h"
BASE_PATH = "data"

# =======================================================================
#               REAL-TIME BINANCE (1h candle updates)
# =======================================================================

def fetch_latest_binance(symbol:str="ETHUSDT", interval:str="1h", base_url: str = BINANCE_BASE, base_path:str = BASE_PATH):
    
    client = Binance_client(base_url)
    
    now = datetime.utcnow()
    one_interval_ago = now - timedelta(hours=1)
    
    print(f"\n[Binance] Fetching latest {interval} candle for {symbol}")
    print(f"Window: {one_interval_ago} → {now}")
    
    candles = client.get_hourly_prices(
        symbol= symbol,
        interval=interval,
        start_dt=one_interval_ago,
        end_dt=now
    )
    
    if not candles:
        print("[Binance] ⚠ No new candle returned.")
        return 
    
    last_candle = candles[-1]
    
    market_path = client.append_realtime_kline(
        symbol=symbol,
        interval=interval,
        candle=last_candle,
        base_path=base_path 
    )
    
    print("[Binance] ✔ Saved latest candle.")
    
    inserted = load_last_price_row(market_path)

    print(f"[Postgres] ✔ Inserted {inserted} price row.")
    
    
    

# =======================================================================
#               REAL-TIME ETHERSCAN (latest block)
# =======================================================================


def fetch_latest_etherscan(base_path:str="data"):
    
    """
    Fetch the latest Ethereum block and save it.
    EtherscanClient already supports saving by date.
    """
    
    client = EtherscanClient(api_key=ETHERSCAN_API_KEY, chain_id=ETHEREUM_CHAIN_ID or "1", base_url=ETHERSCAN_BASE)
    
    print("\n[Etherscan] Fetching latest block...")
    
    latest_block = client.get_latest_block_number()
    today = datetime.utcnow().date().isoformat() # gets the datetime at utc, strips the date and converts the date object to a string in ISO 8601 format.
    
    blocks_csv, txs_csv = client.csv_append(
        block_number=latest_block,
        day=today,
        chain="eth",
        output_dir= base_path
    )
    
    print("[Etherscan] ✔ Latest block saved.")
    
    inserted_blocks = load_last_block_row(blocks_csv)
    inserted_txs = load_last_tx_rows(txs_csv)

    print(
        f"[Postgres] ✔ Inserted {inserted_blocks} block rows and {inserted_txs} tx rows."
    )
    
    

def real_time_pipeline():
    
    # Individual timers for each pipeline
    last_binance_run = 0
    last_etherscan_run = 0

   

    print("\n🚀 Starting Real-Time Pipeline (Binance + Etherscan)...\n")

    while True:
        now = time.time() # returns a floating point in seconds with decimals to be ms

        # -------------------- Binance Every 1 Hour --------------------
        if now - last_binance_run >= BINANCE_INTERVAL:
            try:
                fetch_latest_binance(SYMBOL, INTERVAL, BINANCE_BASE, BASE_PATH)
            except Exception as e:
                print(f"[Binance] ❌ Error: {e}")
            last_binance_run = now

        # -------------------- Etherscan Every 12 Seconds --------------------
        if now - last_etherscan_run >= ETHERSCAN_INTERVAL:
            try:
                fetch_latest_etherscan(BASE_PATH)
            except Exception as e:
                print(f"[Etherscan] ❌ Error: {e}")
            last_etherscan_run = now

        # Small sleep to reduce CPU usage
        time.sleep(1)


# =======================================================================
#                               MAIN
# =======================================================================
if __name__ == "__main__":
    real_time_pipeline()