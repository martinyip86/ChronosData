from src.collectors.base_stream import BaseStream
import ccxt.pro as ccxt_pro

class OkxStream(BaseStream):
    def __init__(self,symbol,redis_client,dtype):
        trade_data = {'info': {'instId': 'BTC-USDT', 'tradeId': '975044170', 'px': '69531.1', 'sz': '0.00016395', 'side': 'buy', 'ts': '1773288201469', 'count': '1', 'source': '0', 'seqId': 73449897621}, 'timestamp': 1773288201469, 'datetime': '2026-03-12T04:03:21.469Z', 'symbol': 'BTC/USDT', 'id': '975044170', 'order': None, 'type': None, 'takerOrMaker': None, 'side': 'buy', 'price': 69531.1, 'amount': 0.00016395, 'cost': 11.399623845, 'fee': {'cost': None, 'currency': None}, 'fees': []}
        super().__init__('binance',symbol,redis_client,dtype)
        self.client = None
        self.last_trade_id_key = f"last_trade_id:{self.exchange_id}:{self.symbol}"
        self.dedup_prefix = f"seen:{self.exchange_id}:{self.symbol}"
        self.last_id_mem = None

    def _create_client(self):
        config = {
            'enableRateLimit':True,
            'options':{'defaultType':'spot'}
        }
        return ccxt_pro.okx(config)
