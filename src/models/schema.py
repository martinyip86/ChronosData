from pydantic import Field,BaseModel
from typing import List,Tuple,Optional
import time
import json
from datetime import datetime

class TickData(BaseModel):
    symbol:str = Field(...,description="BTC/USDT")
    bid_volume:float = Field(...,description="bid volume")
    bid_price:float = Field(...,description="bid price")
    ask_volume:float = Field(...,description="ask volume")
    ask_price:float = Field(...,description="ask price")
    bid_prices:List[float] = Field(...,description="bid prices")
    ask_prices:List[float] = Field(...,description="ask prices")
    bid_volumes:List[float] = Field(...,description="bid volumes")
    ask_volumes:List[float] = Field(...,description="ask volumes")
    nonce:int = Field(...,description="orderbook updated Serial Number")
    timestamp:int = Field(...,description="交易所原始毫秒时间戳")
    local_timestamp:int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))
    exchange_id:str = Field("Binance",description="数据来源，用于SOR对比")

class TradeData(BaseModel):
    symbol:str = Field(...,description="BTC/USDT")
    exchange_id:str = Field(...,description="Binance, OKX, etc.")
    trade_id:str = Field(...,description="成交 Id")
    timestamp:int = Field(...,description="交易所原始毫秒时间戳")
    side:str = Field(...,description="buy/sell")
    price:float = Field(...,description="价格")
    amount:float = Field(...,description="交易量")
    cost:float = Field(...,description="成交总额 price * amount")
    is_taker_buyer:bool = Field(...,description="True=主动买(看多), False=主动卖(看空)")
    raw_info:str = Field(...,description="ccxt底层数据,json模式")
    local_timestamp:int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))

    @classmethod
    def from_ccxt(cls,trade:dict,exchange:str):
        info = trade.get('info', {})
        
        # 1. 判定 is_taker_buyer (兼容逻辑)
        # 币安原生：m 为 True 表示 Maker 是买家 -> Taker 是卖家
        is_m = info.get('m') if 'm' in info else info.get('isBuyerMaker')
        
        if is_m is not None:
            is_taker_buyer = not (str(is_m).lower() == 'true')
        else:
            # 兼容 backfill 时手动传入的字段，如果都没有，默认 False
            is_taker_buyer = trade.get('is_taker_buyer', False)


        return cls(
            symbol = trade['symbol'],
            exchange_id = exchange,
            trade_id = str(trade['id']),
            timestamp = int(trade.get('timestamp')) or int(time.time() * 1000),
            side = trade['side'],
            price = float(trade['price']),
            amount = float(trade['amount']),
            cost = float(trade.get('cost',0.0)),
            is_taker_buyer=is_taker_buyer,
            raw_info = json.dumps(trade['info'])
        )