import ssl
import asyncio
import logging
from typing import Literal, Union, Dict


import msgpack
import nats
from nats.aio.client import Client as NATS
import ccxt.pro as ccxtpro


from utils import user_data_stream, parse_symbol, parse_order_status, parse_account_update
from entity import OrderResponse, MarketDataStore, EventSystem, context


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
        self._queue = asyncio.Queue()
    
    def _init_exchange(self) -> Union[ccxtpro.Exchange, ccxtpro.binance]:
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
    
    async def watch_user_data_stream(self) -> None:
        asyncio.create_task(user_data_stream(typ='spot', api_key=self.config['apiKey'], queue=self._queue))
        asyncio.create_task(user_data_stream(typ='linear', api_key=self.config['apiKey'], queue=self._queue))
        asyncio.create_task(self._process_queue())
    
    async def _process_queue(self):
        while True:
            res = await self._queue.get()
            if res['e'] == 'executionReport':
                asyncio.create_task(EventSystem.emit('order_update', res, 'spot'))
            elif res['e'] == 'ORDER_TRADE_UPDATE':
                asyncio.create_task(EventSystem.emit('order_update', res, 'linear'))
            elif res['e'] == 'ACCOUNT_UPDATE':
                asyncio.create_task(EventSystem.emit('account_update', res, 'future'))
            elif res['e'] == 'outboundAccountPosition':
                asyncio.create_task(EventSystem.emit('account_update', res, 'spot'))
            self._queue.task_done()


class AccountManager:
    def __init__(self):
        EventSystem.on('account_update', self._on_account_update)
        EventSystem.on('position_update', self._on_position_update)
    
    def _on_account_update(self, res: Dict, typ: Literal['spot', 'future']):
        parse_account_update(res, typ, context)
        # logging.info(f"Updated account info: {context}")
    
    def _on_position_update(self, order: OrderResponse):
        if order.side == 'buy':
            amount = order.last_filled
        elif order.side == 'sell':
            amount = -order.last_filled
        
        context.position.update(symbol=order.symbol, order_amount=amount, order_price=order.price)
        logging.info(f"Position Updated:\n {context.position}")
            

class OrderManager:
    def __init__(self, exchange: ExchangeManager):
        self._exchange = exchange
        EventSystem.on('order_update', self._on_order_update)
    
    async def _on_order_update(self, res: Dict, typ: Literal['spot', 'linear']):
        if typ == 'linear':
            order = OrderResponse(
                id = res['o']['i'],
                symbol = parse_symbol(res['o']['s'], 'linear'),
                status = parse_order_status(res['o']['X']),
                side = res['o']['S'].lower(),
                amount = float(res['o']['q']),
                filled = float(res['o']['z']),
                last_filled= float(res['o']['l']),
                remaining=0,
                client_order_id= res['o']['c'],
                average = float(res['o']['ap']),
                price = float(res['o']['p'])
            )
        elif typ == 'spot':
            order = OrderResponse(
                id = res['i'],
                symbol = parse_symbol(res['s'], 'spot'),
                status = parse_order_status(res['X']),
                side = res['S'].lower(),
                amount = float(res['q']),
                filled = float(res['z']),
                last_filled= float(res['l']),
                remaining=0,
                client_order_id= res['c'],
                average = float(res['p']),
                price = float(res['p'])
            )
        if order.status == 'new':
            await EventSystem.emit('new_order', order)
        elif order.status == 'partially_filled':
            await EventSystem.emit('partially_filled_order', order)
            await EventSystem.emit('position_update', order)
        elif order.status == 'filled':
            await EventSystem.emit('filled_order', order)
            await EventSystem.emit('position_update', order)   
        elif order.status == 'canceled':
            await EventSystem.emit('canceled_order', order)

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
                last_filled = 0,
                remaining = res['remaining'],
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
                last_filled = 0,
                remaining = res['remaining'],
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
                last_filled = 0,
                remaining= res['remaining'],
                client_order_id = res['clientOrderId'],
                average = res['average'],
                price = res['price']
            )
            logging.debug(f"Cancelled order {order_id} for {symbol}")
            return order_res
        except Exception as e:
            logging.error(f"Error cancelling order {order_id} for {symbol}: {e}")
            return None

