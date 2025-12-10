import os 
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Any   
import time
import csv
from pathlib import Path
from tqdm import tqdm
import random

ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"

load_dotenv()

def require_env(name: str):
    val = os.getenv(name)
    if not val:
        raise SystemExit(f"Missing env var: {name}") # exception that exits your program immediately
    return val

ETHERSCAN_API_KEY = require_env("ETHERSCAN_API_KEY") # no variable with that name exists yet, you passing it with quotes
ETHEREUM_CHAIN_ID = os.getenv("ETHEREUM_CHAIN_ID")

class EtherscanClient:
    def __init__(self, api_key:str, chain_id: str ="1", base_url:str = ETHERSCAN_BASE):
        self.api_key = api_key
        self.chain_id = chain_id
        self.base_url = base_url
        self.session = requests.Session()
        
    
    def _get(
        self,
        params: Dict[str, Any],
        max_retries: int = 5,
        backoff: float = 0.7, 
        timeout: int = 20,
    ) -> Dict[str, Any]:
        # always include required params
        full_params = {"chainid": self.chain_id, "apikey": self.api_key, **params}

        for attempt in range(max_retries):
            r = self.session.get(self.base_url, params=full_params, timeout=timeout)
            # Retry on 429/5xx
            if r.status_code in (429, 500, 502, 503, 504):
                sleep_for = backoff * (2 ** attempt)
                print(f"⚠️ HTTP {r.status_code} — retrying in {sleep_for:.2f}s (attempt {attempt+1})")
                time.sleep(sleep_for)
                continue

            r.raise_for_status()
            data = r.json() or {}
            
            # print("\n==== DEBUG RAW RESPONSE ====")
            # print("URL:", r.url)
            # print("DATA:", data)
            # print("TYPE(result):", type(data.get("result")))
            # print("============================\n")

            
            message = str(data.get("message", "")).lower()
            error   = str(data.get("error", "")).lower()

            if "rate limit" in message or "rate limit" in error:
                sleep_for = backoff * (2 ** attempt)
                print(f"⚠️ Rate limited — retrying in {sleep_for:.2f}s (attempt {attempt+1})")
                time.sleep(sleep_for)
                continue
            
            # If Etherscan returns an explicit error object
            if "error" in data:
                raise RuntimeError(f"Etherscan API error: {data['error']}")


            # --------------------------------------------------
            # 🔥 Success case
            # --------------------------------------------------
            return data

        raise RuntimeError(f"Max retries exceeded for params={params}")



    def get_block_by_number(self, block_num, full_txs: bool = True) -> Dict[str, Any]:
        
        params = {
            "module":"proxy",
            "action":"eth_getBlockByNumber",
            "tag": hex(block_num),
            "boolean": "true" if full_txs else "false" # eth_getBlockByNumber expects "boolean": "true"|"false"
        }
        
        data = self._get(params)
        result = data.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"Etherscan error for block {block_num}: {result}")
        # print(f"DEBUG {block_num=} -> type(result)={type(data.get('result'))}, sample={str(data.get('result'))[:120]}")
        return result


    def get_block_by_time(self, date_unix: int, closest:str="before") -> int:
            
        params = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": date_unix,
            "closest": closest
        }
        
        result = self._get(params)["result"]
        # result may be str; cast to int
        try:
            return int(result)
        except (TypeError, ValueError):
            raise RuntimeError(f"Unexpected block number: {result!r}")
    
    
    def get_tx_receipt(self, tx_hash: str):
        
        params ={
            "module":"proxy",
            "action": "eth_getTransactionReceipt",
            "txhash":tx_hash
        }
        
        return self._get(params)["result"]
    

    def _get_receipt_with_retries(
        self,
        tx_hash: str,
        max_tx_retries: int = 3,
        base_sleep: float = 0.5,
    ):
        """
        Try to fetch a transaction receipt a few times.
        This is on top of the network retries inside _get().
        If all attempts fail, return None and log a warning.
        """
        last_err = None
        for attempt in range(max_tx_retries):
            try:
                return self.get_tx_receipt(tx_hash)
            except Exception as e:
                last_err = e
                # If not the last attempt, backoff a bit then try again
                if attempt < max_tx_retries - 1:
                    sleep_for = base_sleep * (2 ** attempt)
                    print(f"warn: receipt attempt {attempt+1} failed for {tx_hash}, retrying in {sleep_for:.2f}s: {e}")
                    time.sleep(sleep_for)

        # After all attempts failed:
        print(f"warn: giving up on receipt for {tx_hash} after {max_tx_retries} attempts: {last_err}")
        return None
        
    
    def blocks_for_day(self, start_d: str, n:int=50):
        
        start_unix, end_unix = utc_day_bounds(start_d)
        
        start_block = self.get_block_by_time(start_unix, closest="after")
        end_block = self.get_block_by_time(end_unix-1, closest="before")
        
        if start_block > end_block:
            start_block, end_block = end_block, start_block
        
        total_blocks = end_block - start_block 
        if total_blocks <0 or n <= 1:
            return start_block

        step = max(1, total_blocks//n)
        blocks = list(range(start_block, end_block+1, step))
        return blocks[:n]
    
    @staticmethod
    def _h2i(h) -> int:
        
        if h is None: # we need None here since we looking only for missing values 
            return 0
        
        if isinstance(h,int):
            return h
        
        s = str(h).strip()
        if s.startswith("0x") or s.startswith("0X"):
            return int(s,16) # this tells to it to parse the string as hexadecimal (python does not detect "0x")

        # fallback 
        try:
            return int(s) # base 16 before might fail (e.g. non numeric text) so we use base 10 or ("1234")
        except ValueError:
            return 0
    
    @staticmethod
    def _ts_iso(ts_unix:int):
        return datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat() 
    
    def _block_header(self,block:Dict[str, Any], block_number:int, chain:str="eth"):
        
        h2i = self._h2i
        ts = h2i(block.get("timestamp"))
        
        return {
            "Chain": chain,
            "Block Number": block_number,
            "Block Hash": block.get("hash"),
            "Timestamp (Unix)": ts,
            "Timestamp (ISO)": self._ts_iso(ts),
            "Gas Used": h2i(block.get("gasUsed")),
            "Gas Limit": h2i(block.get("gasLimit")),
            "Tx Count": len(block.get("transactions", []))      
        }
        
    
    def _tx_rows(self, block: Dict[str, Any], block_number:int, chain:str="eth" ):
        
        h2i = self._h2i
        ts = h2i(block.get("timestamp"))
        tx_rows = []
        
        txs = block.get("transactions", [])
        
        sample_size = min(20, len(txs))
        if sample_size == 0:
            return []
        
        random.seed(block_number)
        sample_txs = random.sample(txs, sample_size)
        
        for tx in sample_txs:
            rows = {
                "Chain":chain,
                "Block Number":block_number,
                "Timestamp (ISO)": self._ts_iso(ts),
                "Tx Hash": tx.get("hash"),
                "From": tx.get("from"),
                "To": tx.get("to"),
                "Gas Price": h2i(tx.get("gasPrice")),
                "Gas Limit": h2i(tx.get("gas")),
                "Value": h2i(tx.get("value"))
            }
            
            gas_used = 0
            effect_gas = 0 
            receipt = self._get_receipt_with_retries(tx["hash"])
            
            if isinstance(receipt, dict):
                gas_used = h2i(receipt.get("gasUsed"))
                effect_gas= h2i(receipt.get("effectiveGasPrice"))
            else:
                print(f"warn: invalid receipt for tx {tx.get('hash')}: {receipt}")
                gas_used = 0
                effect_gas = 0
            
            rows["Gas Used"] = gas_used
            rows["Effective Gas Price"] = effect_gas
            tx_rows.append(rows)
            
            time.sleep(0.7)
            
        return tx_rows 
                
        
    def save_to_csv(self, day:str, n:int=50, chain:str="eth", output_dir:str = "data"):
    
        d = datetime.fromisoformat(day)
        month_key = f"{d.year:04d}-{d.month:02d}"
        
        blocks_dir = Path(output_dir)/"blocks"/ month_key
        txs_dir = Path(output_dir)/"transactions"/ month_key
        blocks_dir.mkdir(parents=True, exist_ok= True)
        txs_dir.mkdir(parents=True, exist_ok=True)
        
        blocks_path = blocks_dir / f"blocks_{day}.csv"
        txs_path = txs_dir / f"transactions_{day}.csv"
        
        block_fields = [
        "Chain","Block Number","Block Hash",
        "Timestamp (Unix)","Timestamp (ISO)",
        "Gas Used","Gas Limit",
        "Tx Count"
        ]
        tx_fields = [
            "Chain","Tx Hash","Block Number","Timestamp (ISO)",
            "From","To","Gas Limit","Gas Price","Gas Used", "Effective Gas Price","Value"
        ]
        
        block_nums = self.blocks_for_day(day, n)
        
        with blocks_path.open("w", newline ="", encoding = "utf-8") as fb, \
            txs_path.open("w", newline ="", encoding = "utf-8") as ft:
                
                bw = csv.DictWriter(fb, fieldnames=block_fields)
                tw = csv.DictWriter(ft, fieldnames=tx_fields)
                bw.writeheader(); tw.writeheader()
                
        
                for i in tqdm(block_nums, desc=f"Blocks {day}"):
                    
                    print(f"Fetching block {i} ...")

                    if i is None or not isinstance(i, int) or i <= 0:
                        print(f"[SKIP] Invalid block number: {i}")
                        continue

                    try:
                        # get_block_by_number already returns the actual block dict
                        block = self.get_block_by_number(i, full_txs=True)
                    except Exception as e:
                        print(f"warn: skipping block {i}: {e}")
                        continue

                    # If block is missing or malformed
                    if block is None or not isinstance(block, dict):
                        print(f"[SKIP] Block {i} returned null or invalid format.")
                        continue



                    
                    hdr = self._block_header(block, i, chain)
                    bw.writerow({k:hdr.get(k) for k in block_fields})
                    
                    for row in self._tx_rows(block, i, chain):
                        tw.writerow({k:row.get(k) for k in tx_fields})
                    
                    time.sleep(0.15)
            
        print(f"Saved {blocks_path} and {txs_path} (requested {day}; wrote {len(block_nums)} blocks)")
        return str(blocks_path), str(txs_path)
    

    def csv_append(
    self, 
    block_number: int, 
    day: str, 
    chain: str = "eth", 
    output_dir: str = "data"
):
        """
        Appends a single block + its transactions to the existing CSV files.
        Creates files if they don't exist.
        """

        d = datetime.fromisoformat(day)
        month_key = f"{d.year:04d}-{d.month:02d}"

        blocks_dir = Path(output_dir) / "blocks" / month_key
        txs_dir = Path(output_dir) / "transactions" / month_key
        blocks_dir.mkdir(parents=True, exist_ok=True)
        txs_dir.mkdir(parents=True, exist_ok=True)

        blocks_path = blocks_dir / f"blocks_{day}.csv"
        txs_path = txs_dir / f"transactions_{day}.csv"

        block_fields = [
            "Chain","Block Number","Block Hash",
            "Timestamp (Unix)","Timestamp (ISO)",
            "Gas Used","Gas Limit","Tx Count"
        ]
        tx_fields = [
            "Chain","Tx Hash","Block Number","Timestamp (ISO)",
            "From","To","Gas Limit","Gas Price","Gas Used",
            "Effective Gas Price","Value"
        ]

        # Fetch block data
        block = self.get_block_by_number(block_number, full_txs=True)
        if not block:
            print(f"[WARN] Block {block_number} returned None, skipping.")
            return None

        header_row = self._block_header(block, block_number, chain)
        tx_rows = self._tx_rows(block, block_number, chain)

        # ---------------- APPEND BLOCK ----------------
        file_exists = os.path.exists(blocks_path)
        with blocks_path.open("a", encoding="utf-8", newline="") as fb:
            bw = csv.DictWriter(fb, fieldnames=block_fields)
            if not file_exists:
                bw.writeheader()
            bw.writerow({k: header_row.get(k) for k in block_fields})

        # ---------------- APPEND TXs ----------------
        file_exists = os.path.exists(txs_path)
        with txs_path.open("a", encoding="utf-8", newline="") as ft:
            tw = csv.DictWriter(ft, fieldnames=tx_fields)
            if not file_exists:
                tw.writeheader()
            for row in tx_rows:
                tw.writerow({k: row.get(k) for k in tx_fields})

        print(f"[Realtime] Appended block {block_number} to {blocks_path}")
        return str(blocks_path), str(txs_path)
                  

def utc_day_bounds(date_srt:str):
    start_d = datetime.fromisoformat(date_srt).replace(tzinfo = timezone.utc)
    end_d = start_d + timedelta(days = 1)
    return int(start_d.timestamp()), int(end_d.timestamp())


    
    