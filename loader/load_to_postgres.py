"""
load_to_postgres.py

- Connects to PostgreSQL
- Loads ALL historical CSVs under ./data into:
    - raw_blocks
    - raw_transactions
    - raw_prices
- Provides reusable functions:
    - load_blocks_csv_to_postgres(csv_path)
    - load_transactions_csv_to_postgres(csv_path)
    - load_prices_csv_to_postgres(csv_path)
- Uses UPSERT (ON CONFLICT DO NOTHING) to skip duplicates.
"""


from dotenv import load_dotenv
from clients.etherscan_client import require_env
import os
from pathlib import Path 
import psycopg2
from psycopg2.extras import execute_batch
from typing import Iterable
import csv 
from logging_config import get_logger

# -------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------

logger = get_logger(__name__)


# -------------------------------------------------------------------
# Database config
# -------------------------------------------------------------------

load_dotenv()

DB_HOST = os.getenv("PGHOST", "localhost")
DB_DATABASE = os.getenv("PGDATABASE", "blockchain_pipe")
DB_PORT = os.getenv("PGPORT", "5432")
DB_USER = os.getenv("PGUSER", "postgres")

DB_PASSWORD = require_env("PGPASSWORD")

DATA_ROOT = Path(__file__).resolve().parent.parent / "data" # takes the absolute path of the currnt directory (guarranteeing about it) and goes 2 times back so it can append data folder

def get_db_connection():
    con = psycopg2.connect(
        user = DB_USER,
        dbname = DB_DATABASE,
        host = DB_HOST,
        password = DB_PASSWORD,
        port = DB_PORT
    )
    
    con.autocommit = False # autocommit is a property, in that way we definining manually commit as we already doing in thecode below
    return con

def iter_csv_rows(csv_path: Path) -> Iterable[dict]:
    with csv_path.open("r", newline="", encoding="utf-8") as f: # open in read mode 
        reader = csv.DictReader(f)  # is a machine that produces one dictionary each time you iterate over it. Each row is a Dict
        for row in reader:
            yield row  # yield is like return it just pause, gives the value and then continues from where it stayed

def standardize_block_headers(row: dict) -> dict:
    return {
        "chain": row.get("Chain"),
        "block_number": row.get("Block Number"),
        "block_hash": row.get("Block Hash"),
        "timestamp_unix": row.get("Timestamp (Unix)"),
        "timestamp_iso": row.get("Timestamp (ISO)"),
        "gas_used": row.get("Gas Used"),
        "gas_limit": row.get("Gas Limit"),
        "tx_count": row.get("Tx Count"),
    }


def standardize_transaction_headers(row: dict) -> dict:
    return {
        "tx_hash": row.get("Tx Hash"),
        "chain": row.get("Chain"),
        "block_number": row.get("Block Number"),
        "timestamp_iso": row.get("Timestamp (ISO)"),
        "from_address": row.get("From"),
        "to_address": row.get("To"),
        "gas_limit": row.get("Gas Limit"),
        "gas_price": row.get("Gas Price"),
        "gas_used": row.get("Gas Used"),
        "effective_gas_price": row.get("Effective Gas Price"),
        "value": row.get("Value"),
    }


def load_blocks_to_csv(csv_path:str|Path) -> int:
    
    csv_path = Path(csv_path)
    logger.info(f"loading blocks from {csv_path}")
    
    rows = []
    
    for raw_row in iter_csv_rows(csv_path):
        row = standardize_block_headers(raw_row)
        rows.append((
            row["chain"],
            int(row["block_number"]),
            row["block_hash"],
            int(row["timestamp_unix"]),
            row["timestamp_iso"],     # psycopg2 can parse ISO timestamp string
            int(row["gas_used"]),
            int(row["gas_limit"]),
            int(row["tx_count"]),
        ))
        
    if not rows:
        logger.info("no rows found inside file; skipping")
        return 0 # the result should be an integer according to the func (return -> gives None)


    sql = """
    INSERT INTO raw_blocks (
        chain,
        block_number,
        block_hash,
        timestamp_unix,
        timestamp_iso,
        gas_used,
        gas_limit,
        tx_count
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (block_number) DO NOTHING;
    """
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:   # with here means that will close automatically (no need for manualy closing)
            execute_batch(cur, sql, rows, page_size=500)
            inserted = cur.rowcount   # rowcount is just a number stored inside the cursor object
        conn.commit()
        logger.info(f"new block rows {inserted} inserted, conflicts skipped")
        return inserted 
    except Exception as e:
        conn.rollback()
        logger.exception(f"Error loading blokc rows from {csv_path}: {e} ")         
        raise  # this needed after the loging of the error, since without the program continues as if nothing happened
               # stops the program noticably 
    finally:
        conn.close() # we doing finally since without it, in case we have an error jumps out immediatelly withou closing it 
    


def load_transactions_csv_to_postgres(csv_path: str | Path) -> int:
    """
    Load a transactions CSV into raw_transactions.

    Assumes CSV columns:
      tx_hash, chain, block_number, timestamp_iso, from, to,
      gas_limit, gas_price, gas_used, effective_gas_price, value
    """
    csv_path = Path(csv_path)
    logger.info(f"Loading transactions from {csv_path}")

    rows_to_insert = []

    for raw_row in iter_csv_rows(csv_path):
        row = standardize_transaction_headers(raw_row)
        rows_to_insert.append((
            row["tx_hash"],
            row["chain"],
            int(row["block_number"]) if row.get("block_number") else None,
            row["timestamp_iso"],
            row.get("from") or row.get("from_address"),  # safety
            row.get("to") or row.get("to_address"),
            int(row["gas_limit"]) if row.get("gas_limit") else None,
            int(row["gas_price"]) if row.get("gas_price") else None,
            int(row["gas_used"]) if row.get("gas_used") else None,
            int(row["effective_gas_price"]) if row.get("effective_gas_price") else None,
            row["value"],  # NUMERIC; let psycopg2 handle as string
        ))

    if not rows_to_insert:
        logger.info("No rows found in file; skipping.")
        return 0

    sql = """
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (tx_hash) DO NOTHING;
    """

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows_to_insert, page_size=2000)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"Inserted {inserted} new transaction rows (conflicts skipped).")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.exception(f"Error loading transactions from {csv_path}: {e}")
        raise
    finally:
        conn.close()
        

def load_prices_csv_to_postgres(csv_path:Path) ->int:
    
    csv_path = Path(csv_path)
    logger.info(f"Loading Prices from {csv_path}")
    
    rows = []
    
    for row in iter_csv_rows(csv_path):
        rows.append((
            row["close_time"],
            float(row["close"]),
            float(row["volume"]),
            int(row["trades"])     
        ))

    if not rows:
        logger.info("No rows find in file; skipping")
        return 0


    sql = """
    INSERT INTO raw_prices (
       close_time,
       close,
       volume,
       trades       
    )
    VALUES (
        %s, %s, %s, %s
    )
    ON CONFLICT (close_time) DO NOTHING;
    """
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows, page_size=500)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"Inserted new price rows {inserted}, conflicts skipped")
        return inserted
    except Exception as e:
        conn.rollback()
        logger.exception(f"Error Loading prices from {csv_path}:{e}")
        raise
    finally:
        conn.close()
        

def find_csv_files_under(root:Path) ->list[Path]:
    return list(root.rglob("*.csv")) # that returns a generator (a lazy list), which produce items one at a time (e.g. x for x in range(3))
                                     # the result is a list of a path objects like Path("data/blocks/2025-11/blocks_2025-11-01.csv"),

def detect_csv_type(csv_path:Path) -> str | None:
    """
    Very simple detection based on folder naming.
    You already have:
      data/blocks/YYYY-MM/...
      data/transactions/YYYY-MM/...
      data/market_metrics/YYYY-MM/...
    """
    
    parts = csv_path.parts # this is a property of the path returns the names of the folder or files inside that path
    if "blocks" in parts:
        return "blocks"
    if "transactions" in parts:
        return "transactions"
    if "market_metrics" in parts:
        return "prices"
    return None

def load_all_historical():
    logger.info(f"Scanning for historical CSVs undert: {DATA_ROOT}")
    all_csvs = find_csv_files_under(DATA_ROOT)
    logger.info(f"Found {len(all_csvs)} csv files.")
    
    total_blocks = total_txs = total_prices = 0
    
    for csv_path in sorted(all_csvs):
        csv_type = detect_csv_type(csv_path)
        if csv_type == "blocks":
            total_blocks += load_blocks_to_csv(csv_path)
        elif csv_type == "transactions":
            total_txs += load_transactions_csv_to_postgres(csv_path)
        elif csv_type == "prices":
            total_prices += load_prices_csv_to_postgres(csv_path)
        else:
            logger.warning(f"Could not detect csv type for: {csv_path}; skipping.")
            
    logger.info("=== Historical load summary ===")
    logger.info(f"Blocks inserted:       {total_blocks}")
    logger.info(f"Transactions inserted: {total_txs}")
    logger.info(f"Prices inserted:       {total_prices}")


if __name__ == "__main__":
    load_all_historical()
    
    
    
            


    