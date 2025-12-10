import os
from load_to_postgres import get_db_connection
from pathlib import Path
from logging_config import get_logger



DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "blockchain_warehouse")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASSWORD = os.getenv("PGPASSWORD", "postgres")

logger = get_logger(__name__)


# ===================================================================
# 1) LOAD LAST BLOCK ROW
# ===================================================================
def load_last_block_row(blocks_csv_path: str | Path):
    """
    Reads ONLY the last line of blocks_YYYY-MM-DD.csv
    and inserts it into raw_blocks.
    """
    
    blocks_csv_path = Path(blocks_csv_path)
    
    with blocks_csv_path.open("r", encoding="utf-8") as f:  # we dont need to define newline when reading, python automatically normalizes newline characters when reading
        lines = f.readlines()  # this convers all the lines into a list of string, each string a single row
        if len(lines) <= 1:  # just the header if it is =1
            logger.info("No new rows to load")
            return 0
        last_row = lines[-1].strip().split(",") # strip here it just removes the \n at the end when we change a row
    
    chain, block_number, block_hash, ts_unix, ts_iso, gas_used, gas_limit, tx_count = last_row
    
    # insert doesnt have to match the order of the table in sql
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
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    ) ON CONFLICT (block_number) DO NOTHING;
    """
    
    # the order needs to match insert
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (
                chain, 
                int(block_number),
                block_hash,
                int(ts_unix),
                ts_iso,
                int(gas_used),
                int(gas_limit),
                int(tx_count)   
            ))
        conn.commit()
        logger.info(f"Inserted 1 block row -> block number: {block_number}")
        return 1  # the reason we return 1, is to show that the row succesfully inserted
    except Exception as e:
        conn.rollback
        logger.error(f"Error inserting last block row: {e}") # here we need error since the row wasnt inserted
        return 0
    finally:
        conn.close()
    

# ===================================================================
# 2) LOAD LAST TX ROWS
# ===================================================================

def load_last_tx_rows(txs_csv_path: str | Path):

    txs_csv_path = Path(txs_csv_path)
    
    with txs_csv_path.open("r", encoding="utf-8") as f:
        lines = f.readlines()
        if len(lines) <= 1:
            logger.info("No new transactions rows to load")
            return 0
        
    header = lines[0]
    data_lines = lines[1:]
    
    last_block_number = data_lines[-1].split(",")[2]
    last_timestamp = data_lines[-1].split(",")[3]
    
    last_rows = []
    for line in reversed(data_lines):
        cols = line.split(",")    # this breaks the single string row into a list of strings
        if cols[2] == last_block_number and cols[3] == last_timestamp:
            last_rows.append(cols)
        else:
            break
    
    last_rows.reverse()
    
    sql = """
        INSERT INTO raw_transactions (
            tx_hash, chain, block_number,
            timestamp_iso, from_address, to_address,
            gas_limit, gas_price, gas_used,
            effective_gas_price, value
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (tx_hash) DO NOTHING;
    """
    
    conn = get_db_connection()
    inserted = 0 
    
    try: 
        with conn.cursor() as cur:
            for cols in last_rows:
                (chain, tx_hash, block_number, ts_iso, from_address, to_address,  # cols is list of strings so with that way we assign every variable 
                    gas_limit, gas_price, gas_used, eff_gas, value) = cols
                
                cur.execute(sql, (
                    tx_hash,
                    chain,
                    int(block_number),
                    ts_iso,
                    from_address,
                    to_address,
                    int(gas_limit),
                    int(gas_price),
                    int(gas_used),
                    int(eff_gas),
                    value
                ))
                
                inserted += 1 
                
            
            conn.commit()
            logger.info(f"Inserted {inserted} tx_rows -> block number: {block_number}")
            return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting last tx rows: {e}")
        return 0
    finally:
        conn.close()
        

def load_last_price_row(price_csv_path: str | Path):
    price_csv_path = Path(price_csv_path)
    
    with price_csv_path.open("r", encoding="utf-8") as f:
        lines = f.readlines()
        if len(lines) <= 1:
            logger.info("No market metrics rows to load")
            return 0 
        
        last_row = lines[-1].strip().split(",")
        
    close_time, close, volume, trades = last_row
    
    sql = """
        INSERT INTO raw_prices (
            close_time, close, volume, trades
        )
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (close_time) DO NOTHING;
    """
        
    
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql,
                        close_time,
                        close,
                        volume,
                        int(trades)
                        )
        
        conn.commit()
        logger.info(f"Inserted 1 market metrics ros -> close_time = {close_time}")
        return 1
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting last candle row: {e}")
        return 0
    finally:
        conn.close()
        
    
    
            
        
