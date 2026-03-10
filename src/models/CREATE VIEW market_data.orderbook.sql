CREATE VIEW market_data.v_lake_orderbook AS
SELECT
    splitByChar('/',_path)[-8] AS exchange,
    splitByChar('/',_path)[-7] AS mkt_type,
    splitByChar('/',_path)[-6] AS pair,
    toDate(substring(splitByChar('/',_path)[-1],-19,8)) AS orderbook_date,
    substring(splitByChar('/',_path)[-1],-10,2) AS hour,
    (bid_prices[1] * ask_volumes[1] + ask_prices[1] * bid_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS micro_price,
    (bid_volumes[1] - ask_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS imbalance,
    ask_prices[1] - bid_prices[1] AS spread,
    *
FROM market_data.lake_orderbook_raw