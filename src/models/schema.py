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
    bids:List[Tuple[float,float]] = Field(...,description="bid depth[[bid_price,bid_volume]]")
    asks:List[Tuple[float,float]] = Field(...,description="ask depth[[ask_price,ask_volume]]")
    nonce:int = Field(...,alias="lastUpdateId",description="orderbook updated Serial Number")
    timestamp:int = Field(...,description="交易所原始毫秒时间戳")
    local_timestamp:int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))
    exchange_time:Optional[int] = Field(...,description="交易所撮合引擎时间")
    source:str = Field("Binance",description="数据来源，用于SOR对比")

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
        return cls(
            symbol = trade['symbol'],
            exchange_id = exchange,
            trade_id = str(trade['id']),
            timestamp = int(trade.get('timestamp')) or int(time.time() * 1000),
            side = trade['side'],
            price = float(trade['price']),
            amount = float(trade['amount']),
            cost = float(trade.get('cost',0.0)),
            is_taker_buyer = not trade['info'].get('m',False),
            raw_info = json.dumps(trade['info'])
        )