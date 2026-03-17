from pydantic import Field,BaseModel
from typing import List,Tuple,Optional
import time
import json
from datetime import datetime

class TickData(BaseModel):
    symbol:str = Field(...,description="BTC/USDT")
    mkt_type:str = Field(...,description="spot")
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
    mkt_type:str = Field(...,description="spot")
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
    def from_ccxt(cls,trade:dict,exchange:str,mkt_type:str):
        info = trade.get('info', {})
        
        if exchange.lower() == 'binance':
            # Binance: m=True (Maker是买方) -> Taker是卖方 -> is_taker_buyer = False
            is_m = info.get('m', str(info.get('isBuyerMaker', '')).lower() == 'true')
            is_taker_buyer = not is_m

            if is_m is not None:
                is_taker_buyer = not (str(is_m).lower() == 'true')
            else:
                # 兼容 backfill 时手动传入的字段，如果都没有，默认 False
                is_taker_buyer = trade.get('is_taker_buyer', False)
        elif exchange.lower() == 'okx':
            # OKX: side="buy" (Taker是买方) -> is_taker_buyer = True
            is_taker_buyer = trade.get('side') == 'buy'

        return cls(
            symbol = trade['symbol'],
            exchange_id = exchange,
            mkt_type = mkt_type,
            trade_id = str(trade['id']),
            timestamp = int(trade.get('timestamp')) or int(time.time() * 1000),
            side = trade['side'],
            price = float(trade['price']),
            amount = float(trade['amount']),
            cost = float(trade.get('cost',0.0)),
            is_taker_buyer=is_taker_buyer,
            raw_info = json.dumps(trade['info'])
        )