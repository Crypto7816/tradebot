import ssl
import asyncio
import logging
from typing import Literal, Union


import msgpack
import nats
from nats.aio.client import Client as NATS
import ccxt.pro as ccxtpro


from entity import OrderResponse, MarketDataStore, EventSystem


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class NatsManager:
    def __init__(self, nats_url = "nats://104.194.152.27:4222", cert_path = "./keys"):
        self._nc = None
        self._nats_url = nats_url
        self._cert_path = cert_path
        self._queue = asyncio.Queue()
    
    async def _connect(self):
        ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        ssl_ctx.load_cert_chain(certfile=f'{self._cert_path}/server-cert.pem',
                                keyfile=f'{self._cert_path}/server-key.pem')
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        self._nc = await nats.connect(self._nats_url, tls=ssl_ctx)
        
    async def subscribe(self):
        await self._connect()
        await self._nc.subscribe('binance.spot.bookTicker.*', cb=self._callback)
        await self._nc.subscribe('binance.linear.bookTicker.*', cb=self._callback)
        asyncio.create_task(self._process_queue())
        await asyncio.Event().wait()
        
    async def _callback(self, msg):
        res = msgpack.unpackb(msg.data)
        await self._queue.put(res)
    
    async def _process_queue(self):
        while True:
            res = await self._queue.get()
            await MarketDataStore.update(res)
            self._queue.task_done()
    
    
class ExchangeManager:
    def __init__(self, config):
        self.config = config
        self.api = self._init_exchange()
    
    def _init_exchange(self) -> ccxtpro.Exchange:
        try:
            exchange_class = getattr(ccxtpro, self.config['exchange_id'])
        except AttributeError:
            raise AttributeError(f"Exchange {self.config['exchange_id']} is not supported")
        
        api = exchange_class(self.config)
        api.set_sandbox_mode(self.config.get("sandbox", False))
        api.enableRateLimit = True
        
        return api
    
    async def load_markets(self) -> None:
        await self.api.load_markets()
    
    async def close(self) -> None:
        await self.api.close()
    
    


class OrderManager:
    def __init__(self, exchange: ExchangeManager):
        self._exchange = exchange

    async def place_limit_order(
        self,
        symbol: str,
        side: Literal['buy', 'sell'],
        amount: float,
        price: float,
        close_position: bool = False,
        client_order_id: str = None,
    ) -> Union[OrderResponse, None]:
        try:
            if close_position:
                res = await self._exchange.api.create_order(
                    symbol=symbol,
                    type='limit',
                    side = side,
                    amount = amount,
                    price = price,
                    params = {
                        'reduceOnly': True,
                        'clientOrderId': client_order_id
                    }
                )
            else:
                res = await self._exchange.api.create_order(
                    symbol=symbol,
                    type='limit',
                    side = side,
                    amount = amount,
                    price = price,
                    params = {
                        'clientOrderId': client_order_id
                    }
                )
            order_res = OrderResponse(
                id = res['id'],
                symbol = res['symbol'],
                status = res['status'],
                side = res['side'],
                amount = res['amount'],
                filled = res['filled'],
                client_order_id = res['clientOrderId'],
                average = res['average'],
                price = res['price']
            )
            logging.debug((f"Placed limit {side} order for {symbol} at {order_res['price']}: {order_res['id']} amount: {order_res['amount']}"))
            return order_res
        except Exception as e:
            logging.error(f"Error placing {side} limit order for {symbol} amount: {amount}: {e}")
            return None
    
    async def place_market_order(
        self,
        symbol: str,
        side: Literal['buy', 'sell'],
        amount: float,
        close_position: bool = False,
        client_order_id: str = None,
    ) -> Union[OrderResponse, None]:
        try:
            if close_position:
                res = await self._exchange.api.create_order(
                    symbol=symbol,
                    type='market',
                    side = side,
                    amount = amount,
                    params = {
                        'reduceOnly': True,
                        'clientOrderId': client_order_id
                    }
                )
            else:
                res = await self._exchange.api.create_order(
                    symbol=symbol,
                    type='market',
                    side = side,
                    amount = amount,
                    params = {
                        'clientOrderId': client_order_id
                    }
                )
            order_res = OrderResponse(
                id = res['id'],
                symbol = res['symbol'],
                status = res['status'],
                side = res['side'],
                amount = res['amount'],
                filled = res['filled'],
                client_order_id = res['clientOrderId'],
                average = res['average'],
                price= res['price']
            )
            logging.debug((f"Placed market {side} order for {symbol} at average {order_res['average']}: {order_res['id']} amount: {order_res['amount']}"))
            return order_res
        except Exception as e:
            logging.error(f"Error placing {side} market order for {symbol} amount: {amount}: {e}")
            return None
            
        
    async def cancel_order(self, order_id: str, symbol: str) -> Union[OrderResponse, None]:
        try:
            res = await self._exchange.api.cancel_order(id = order_id, symbol = symbol)
            order_res = OrderResponse(
                id = res['id'],
                symbol = res['symbol'],
                status = res['status'],
                side = res['side'],
                amount = res['amount'],
                filled = res['filled'],
                client_order_id = res['clientOrderId'],
                average = res['average'],
                price = res['price']
            )
            logging.debug(f"Cancelled order {order_id} for {symbol}")
            return order_res
        except Exception as e:
            logging.error(f"Error cancelling order {order_id} for {symbol}: {e}")
            return None
    



