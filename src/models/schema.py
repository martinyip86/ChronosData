from pydantic import Field,BaseModel
from typing import List,Tuple,Optional
import time
import json
from datetime import datetime

class TickData(BaseModel):
    """
    Level 2 Market Depth Snapshot.
    Captures top 20 bids/asks to provide a high-fidelity view of the orderbook.
    """
    symbol:str = Field(...,description="Instrument symbol (e.g., BTC/USDT)")
    mkt_type:str = Field(...,description="Market segment (spot/swap/future)")
    bid_volume:float = Field(...,description="Best bid quantity")
    bid_price:float = Field(...,description="Best bid price")
    ask_volume:float = Field(...,description="Best ask quantity")
    ask_price:float = Field(...,description="Best ask price")

    # Depth arrays for micro-structure analysis (e.g., Order Flow Imbalance)
    bid_prices:List[float] = Field(...,description="Array of top 20 bid prices")
    ask_prices:List[float] = Field(...,description="Array of top 20 ask prices")
    bid_volumes:List[float] = Field(...,description="Array of top 20 bid volumes")
    ask_volumes:List[float] = Field(...,description="aArray of top 20 ask volumes")
    nonce:int = Field(...,description="Exchange sequence number/Update ID")
    timestamp:int = Field(...,description="Original exchange matching engine timestamp (ms)")
    local_timestamp:int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))
    exchange_id:str = Field("Binance",description="Data source identifier for Smart Order Routing (SOR)")

class TradeData(BaseModel):
    """
    Individual Trade Execution Record.
    Standardized schema to unify trade events across multiple exchanges.
    """
    symbol:str = Field(...,description="Instrument symbol")
    exchange_id:str = Field(...,description="Bxchange identifier (e.g., Binance, OKX)")
    mkt_type:str = Field(...,description="Market segment (spot/swap/future)")
    trade_id:int = Field(...,description="Unique execution ID from exchange by String Int")
    trade_id_raw:str = Field(...,description="Unique execution ID from exchange by String")
    timestamp:int = Field(...,description="Matching engine execution timestamp (ms)")
    side:str = Field(...,description="Execution direction (buy/sell)")
    price:float = Field(...,description="Execution price")
    amount:float = Field(...,description="Execution quantity")
    is_taker_buyer:bool = Field(...,description="Directional intent: True=Taker Buy (Bullish), False=Taker Sell (Bearish)")
    local_timestamp:int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))

    @classmethod
    def from_ccxt(cls,trade:dict,exchange:str):
        """
        Factory method to normalize disparate exchange trade formats into a unified schema.
        Handles complex 'Taker/Maker' logic specific to each venue.
        """
        info = trade.get('info', {})
        
        # --- Normalized Directional Logic ---
        # Determining 'is_taker_buyer' is critical for Order Flow analysis.
        if exchange.lower() == 'binance':
            # Binance Logic: 
            # 'm' (isBuyerMaker) means the buyer was the Maker (Passive).
            # Therefore, if m=True, the Taker was the Seller -> is_taker_buyer = False.
            is_m = info.get('m', str(info.get('isBuyerMaker', '')).lower() == 'true')
            is_taker_buyer = not is_m

            # Robust boolean conversion
            is_m_bool = str(is_m).lower() == 'true'
            is_taker_buyer = not is_m_bool
        elif exchange.lower() == 'okx':
            # OKX Logic: 
            # Directly provides 'side'. If side is 'buy', the Taker initiated a buy.
            is_taker_buyer = trade.get('side') == 'buy'

        return cls(
            symbol = trade['symbol'],
            exchange_id = trade['exchange_id'],
            mkt_type = trade['mkt_type'],
            trade_id = int(trade['id']),
            trade_id_raw = str(trade['id']),
            timestamp = int(trade.get('timestamp')) or int(time.time() * 1000),
            side = trade['side'],
            price = float(trade['price']),
            amount = float(trade['amount']),
            is_taker_buyer=is_taker_buyer
        )