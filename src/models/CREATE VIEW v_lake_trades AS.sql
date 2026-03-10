CREATE VIEW market_data.v_lake_trades AS 
SELECT 
    splitByChar('/',_path)[-8] AS exchange,
    splitByChar('/',_path)[-7] AS mkt_type,
    splitByChar('/',_path)[-6] AS pair,
    toDate(substring(splitByChar('/',_path)[-1],8,8)) AS trade_date,
    substring(splitByChar('/',_path)[-1],17,2) AS hour,
    *
FROM market_data.lake_trades_raw