from datetime import date, timedelta
from clients.etherscan_client import EtherscanClient, ETHERSCAN_API_KEY, ETHEREUM_CHAIN_ID
from clients.binance_client import Binance_client, BINANCE_BASE

def ingest_day(client: EtherscanClient,day_str: str, blocks_per_day: int = 50):
    client.save_to_csv(day_str, n=blocks_per_day, chain="eth",
                       output_dir="data")

def ingest_range(start_day: str, end_day: str, blocks_per_day: int = 50):
    client = EtherscanClient(ETHERSCAN_API_KEY, ETHEREUM_CHAIN_ID or "1")
    start = date.fromisoformat(start_day)
    end = date.fromisoformat(end_day)

    curr = start
    while curr <= end:
        ingest_day(client, curr.isoformat(), blocks_per_day)
        curr += timedelta(days=1)

def ingest_yesterday(blocks_per_day: int = 50):
    d = (date.today() - timedelta(days=1)).isoformat()
    ingest_day(d, blocks_per_day)


def binance_ingest_range(symbol:str, base_url:str, interval:str, start_dt:str, end_dt:str, base_path:str):

    client = Binance_client(base_url)

    print(f"\n Fetching {symbol} {interval} klines")
    print(f" start date: {start_dt}")
    print(f" end date: {end_dt}")

    candles  = client.get_hourly_prices(
        symbol = symbol,
        interval = interval,
        start_dt = start_dt,
        end_dt = end_dt
    )

    print(f"✔ Retrieved {len(candles)} candles")

    client.save_csv_klines(
        symbol=symbol,
        interval=interval,
        candles=candles,
        base_path=base_path
    )

    print("Historical Binance ingestion completed.\n")


if __name__ == "__main__":
    # # example: backfill 5 days
    # ingest_range("2025-12-02", "2025-12-09", blocks_per_day=50)

    binance_ingest_range(
        symbol="ETHUSDT",
        base_url=BINANCE_BASE,
        interval="1h",
        start_dt="2025-11-01",
        end_dt="2025-12-09",
        base_path=r"C:\Users\User\Desktop\Projects\Blockchain_pipe\data"
    )