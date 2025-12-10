# pipeline_gap_repair.py

from datetime import date, datetime, timedelta, timezone
from typing import List, Tuple
from psycopg2.extras import execute_batch

from clients.etherscan_client import (
    EtherscanClient,
    ETHERSCAN_API_KEY,
    ETHEREUM_CHAIN_ID,
)
from clients.binance_client import Binance_client, BINANCE_BASE
from loader.load_to_postgres import get_db_connection  # reuse your existing DB util :contentReference[oaicite:0]{index=0}
from logging_config import get_logger

logger = get_logger(__name__)


# ============================================================
# Helpers
# ============================================================

def _day_bounds_iso(day_str: str) -> Tuple[str, str]:
    """
    Given 'YYYY-MM-DD', return ISO datetimes for start/end of that day in UTC.
    """
    d = datetime.fromisoformat(day_str).replace(tzinfo=timezone.utc)
    start = d
    end = d + timedelta(days=1)
    return start.isoformat(), end.isoformat()


# ============================================================
# 1) Etherscan gap repair: blocks + sampled txs
# ============================================================

def repair_etherscan_for_day(day_str: str, blocks_per_day: int = 50) -> None:
    """
    Fetch the *intended* N=blocks_per_day sampled blocks for the given day
    directly from Etherscan and insert them into Postgres.

    - DOES NOT touch CSVs.
    - Uses ON CONFLICT in SQL to avoid duplicates in raw_blocks/raw_transactions.
    """

    logger.info(f"[GapRepair] Etherscan repair for {day_str}, n={blocks_per_day}")

    client = EtherscanClient(
        api_key=ETHERSCAN_API_KEY,
        chain_id=ETHEREUM_CHAIN_ID or "1",
    )  # :contentReference[oaicite:1]{index=1}

    # 1) Determine which block numbers SHOULD exist for that day
    block_numbers = client.blocks_for_day(day_str, n=blocks_per_day)
    logger.info(f"[GapRepair] Target block sample for {day_str}: {len(block_numbers)} blocks")

    # 2) Collect rows for DB
    block_rows = []
    tx_rows = []

    for bn in block_numbers:
        try:
            block = client.get_block_by_number(bn, full_txs=True)
        except Exception as e:
            logger.warning(f"[GapRepair] Skipping block {bn} due to error: {e}")
            continue

        if not isinstance(block, dict):
            logger.warning(f"[GapRepair] Block {bn} returned invalid format: {type(block)}")
            continue

        # Use the same logic as your CSV writer (_block_header and _tx_rows) :contentReference[oaicite:2]{index=2}
        hdr = client._block_header(block, bn, chain="eth")
        block_rows.append((
            hdr["Chain"],
            int(hdr["Block Number"]),
            hdr["Block Hash"],
            int(hdr["Timestamp (Unix)"]),
            hdr["Timestamp (ISO)"],
            int(hdr["Gas Used"]),
            int(hdr["Gas Limit"]),
            int(hdr["Tx Count"]),
        ))

        tx_sample_rows = client._tx_rows(block, bn, chain="eth")
        for row in tx_sample_rows:
            tx_rows.append((
                row["Tx Hash"],
                row["Chain"],
                int(row["Block Number"]),
                row["Timestamp (ISO)"],
                row.get("From"),
                row.get("To"),
                int(row["Gas Limit"]),
                int(row["Gas Price"]),
                int(row["Gas Used"]),
                int(row["Effective Gas Price"]),
                str(row["Value"]),  # let psycopg2 handle numeric-as-string
            ))

    logger.info(f"[GapRepair] Collected {len(block_rows)} block rows, {len(tx_rows)} tx rows for day={day_str}")

    if not block_rows and not tx_rows:
        logger.info("[GapRepair] Nothing to insert for this day; exiting.")
        return

    # 3) Insert into Postgres with ON CONFLICT DO NOTHING (same as your loaders) 

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if block_rows:
                sql_blocks = """
                    INSERT INTO raw_blocks (
                        chain,
                        block_number,
                        block_hash,
                        timestamp_unix,
                        timestamp_iso,
                        gas_used,
                        gas_limit,
                        tx_count
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (block_number) DO NOTHING;
                """
                execute_batch(cur, sql_blocks, block_rows, page_size=500)
                logger.info(f"[GapRepair] raw_blocks: attempted insert {len(block_rows)} rows")

            if tx_rows:
                sql_txs = """
                    INSERT INTO raw_transactions (
                        tx_hash,
                        chain,
                        block_number,
                        timestamp_iso,
                        from_address,
                        to_address,
                        gas_limit,
                        gas_price,
                        gas_used,
                        effective_gas_price,
                        value
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tx_hash) DO NOTHING;
                """
                execute_batch(cur, sql_txs, tx_rows, page_size=2000)
                logger.info(f"[GapRepair] raw_transactions: attempted insert {len(tx_rows)} rows")

        conn.commit()
        logger.info("[GapRepair] Etherscan repair committed successfully.")
    except Exception as e:
        conn.rollback()
        logger.exception(f"[GapRepair] Error inserting Etherscan repair for {day_str}: {e}")
        raise
    finally:
        conn.close()


# ============================================================
# 2) Binance gap repair: prices
# ============================================================

def repair_binance_for_day(day_str: str,
                           symbol: str = "BTCUSDT",
                           interval: str = "1h") -> None:
    """
    Fetch all candles for the given day directly from Binance
    and insert them into raw_prices.

    - DOES NOT touch CSVs.
    - Uses ON CONFLICT(close_time) DO NOTHING.
    """

    logger.info(f"[GapRepair] Binance repair for {day_str}, symbol={symbol}, interval={interval}")

    start_iso, end_iso = _day_bounds_iso(day_str)

    client = Binance_client(BINANCE_BASE)  # :contentReference[oaicite:4]{index=4}

    candles = client.get_hourly_prices(
        symbol=symbol,
        interval=interval,
        start_dt=start_iso,
        end_dt=end_iso,
    )

    logger.info(f"[GapRepair] Retrieved {len(candles)} candles from Binance for {day_str}")

    if not candles:
        logger.info("[GapRepair] No candles to insert; exiting.")
        return

    rows = []
    for c in candles:
        rows.append((
            c["close_time"],          # ISO string
            float(c["close"]),
            float(c["volume"]),
            int(c["trades"]),
        ))

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            sql_prices = """
                INSERT INTO raw_prices (
                    close_time,
                    close,
                    volume,
                    trades
                )
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (close_time) DO NOTHING;
            """
            execute_batch(cur, sql_prices, rows, page_size=500)
            logger.info(f"[GapRepair] raw_prices: attempted insert {len(rows)} rows")

        conn.commit()
        logger.info("[GapRepair] Binance repair committed successfully.")
    except Exception as e:
        conn.rollback()
        logger.exception(f"[GapRepair] Error inserting Binance repair for {day_str}: {e}")
        raise
    finally:
        conn.close()


# ============================================================
# 3) Convenience wrappers
# ============================================================

def repair_day(day_str: str,
               blocks_per_day: int = 50,
               symbol: str = "BTCUSDT",
               interval: str = "1h") -> None:
    """
    Repair BOTH:
      - Etherscan block sample (blocks + sampled txs)
      - Binance hourly prices
    for the given day, directly in Postgres.
    """

    logger.info(f"[GapRepair] Starting full repair for day={day_str}")
    repair_etherscan_for_day(day_str, blocks_per_day=blocks_per_day)
    repair_binance_for_day(day_str, symbol=symbol, interval=interval)
    logger.info(f"[GapRepair] Completed full repair for day={day_str}")


def repair_yesterday(blocks_per_day: int = 50,
                     symbol: str = "BTCUSDT",
                     interval: str = "1h") -> None:
    """
    Convenience: repair gaps for *yesterday*.
    Useful for scheduling with Airflow.
    """

    y = (date.today() - timedelta(days=1)).isoformat()
    repair_day(y, blocks_per_day=blocks_per_day, symbol=symbol, interval=interval)


if __name__ == "__main__":
    # Manual test: repair yesterday
    repair_yesterday()
