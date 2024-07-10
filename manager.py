import asyncio
import logging
from typing import Literal, Union


import msgpack
from nats.aio.client import Client as NATS
import ccxt.pro as ccxtpro


from utils import OrderResponse, MarketDataStore


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class NatsManager:
    def __init__(self, nc: NATS):
        self._nc = nc
        self._queue = asyncio.Queue()
        
    async def subscribe(self):
        await self._nc.subscribe('binance.spot.bookTicker.*', cb=self._callback)
        await self._nc.subscribe('binance.linear.bookTicker.*', cb=self._callback)
        # await self._wait()
        
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
        self.exchange = exchange

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
                res = await self.exchange.api.create_order(
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
                res = await self.exchange.api.create_order(
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
                res = await self.exchange.api.create_order(
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
                res = await self.exchange.api.create_order(
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
            res = await self.exchange.api.cancel_order(id = order_id, symbol = symbol)
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
    



