select current_database()

drop table if exists raw_blocks;


create table if not exists raw_blocks (
	block_number bigint primary key,
	chain text,
	block_hash text,
	timestamp_unix bigint,
	timestamp_iso timestamp,
	gas_used bigint,
	gas_limit bigint,
	tx_count bigint	
);

create table if not exists raw_transactions (
	tx_hash text primary key,
	chain text,
	block_number bigint,
	timestamp_iso timestamp,
	from_address text,
	to_address TEXT,
    gas_limit BIGINT,
    gas_price BIGINT,
    gas_used BIGINT,
    effective_gas_price BIGINT,
    value numeric
    );


CREATE TABLE IF NOT EXISTS raw_prices (
    close_time TIMESTAMP PRIMARY KEY,
    close NUMERIC,
    volume NUMERIC,
    trades BIGINT
);




create index if not exists idx_raw_blocks_ts
on raw_blocks (timestamp_iso);

create index if not exists idx_raw_transactions_ts
on raw_transactions (timestamp_iso);

create index if not exists idx_raw_transactions_block_num
on raw_transactions (block_number);

drop index if exists idx_raw_blocks_ts_chain, idx_raw_transactions_ts_chain;


select * from raw_prices limit 5;

SELECT COUNT(*) FROM raw_prices;

SELECT 
    block_number,
    COUNT(*) AS tx_count
FROM raw_transactions
GROUP BY block_number
HAVING COUNT(*) < 20 OR COUNT(*) > 20
ORDER BY block_number;


DROP TABLE IF EXISTS raw_transactions CASCADE;
DROP TABLE IF EXISTS raw_blocks CASCADE;
DROP TABLE IF EXISTS raw_prices CASCADE;


DROP VIEW IF EXISTS stg_transactions CASCADE;
DROP VIEW IF EXISTS tx_enriched CASCADE;
DROP VIEW IF EXISTS tx_price_enriched CASCADE;


SELECT * FROM stg_blocks LIMIT 20;
SELECT * FROM stg_transactions LIMIT 20;
SELECT * FROM stg_prices LIMIT 20;

SELECT * FROM block_enriched LIMIT 20;
SELECT * FROM tx_enriched LIMIT 20;
SELECT * FROM tx_price_enriched LIMIT 20;

SELECT * FROM daily_network_stats ORDER BY day LIMIT 10;

SELECT COUNT(*) FROM daily_network_stats;


select * from raw_blocks rb limit 5;

