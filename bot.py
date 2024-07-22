import time
import logging
import asyncio
from typing import List


from utils import spot_2_linear, linear_2_spot
from entity import EventSystem, MarketDataStore, OrderResponse
from manager import NatsManager, OrderManager, ExchangeManager, AccountManager

class TradingBot:
    def __init__(self, config):
        self._config = config
        self._exchange = ExchangeManager(config)
        self._order = OrderManager(self._exchange)
        self._account = AccountManager()
        self._nats = NatsManager()
        
        
        EventSystem.on('new_order', self._on_new_order)
        EventSystem.on('filled_order', self._on_filled_order)
        EventSystem.on('partially_filled_order', self._on_partially_filled_order)
        EventSystem.on('canceled_order', self._on_canceled_order)
        
    async def run(self):
        await self._exchange.load_markets()
        asyncio.create_task(self._nats.subscribe())
        asyncio.create_task(self._exchange.watch_user_data_stream())
        await self._wait()
    
    async def _wait(self):
        await asyncio.Event().wait()
        
    async def _on_new_order(self, order):
        if hasattr(self, 'on_new_order'):
            await self.on_new_order(order)
    
    async def _on_filled_order(self, order):
        if hasattr(self, 'on_filled_order'):
            await self.on_filled_order(order)
    
    async def _on_partially_filled_order(self, order):
        if hasattr(self, 'on_partially_filled_order'):
            await self.on_partially_filled_order(order)
    
    async def _on_canceled_order(self, order):
        if hasattr(self, 'on_canceled_order'):
            await self.on_canceled_order(order)

class Bot(TradingBot):
    def __init__(self, config):
        super().__init__(config)
        self.client_id = 'client_cloudzy_2'
        self.position = []
        self.order_ids = {}
        EventSystem.on('ratio_changed', self.on_ratio_changed)

    async def on_new_order(self, order: OrderResponse):
        id = order['id']

        
    
    async def on_filled_order(self, order):
        logging.info(f"Filled order: {order}")
    
    async def on_partially_filled_order(self, order):
        logging.info(f"Partially filled order: {order}")
    
    async def on_canceled_order(self, order):
        logging.info(f"Canceled order: {order}")
    
    async def on_ratio_changed(self, symbol: str, open_ratio: float, close_ratio: float):
        if open_ratio > 0.00065 and symbol not in self.position:   
            logging.info(f"Opening position for {symbol} at {open_ratio}")
            self.position.append(symbol)
        if close_ratio < 0 and symbol in self.position:
            logging.info(f"Closing position for {symbol} at {close_ratio}")
            self.position.remove(symbol)
    
    async def open_position(
        self,
        symbol: str, # spot
        amount: float = None,
        notional: float = None,
        time_interval: int = 0.1,
        close_position: bool = False,
        open_ratio: float = None,
        wait: int = 60 * 10,
    ):
        order_placed = False
        start_time = time.time()
        
        linear_symbol = spot_2_linear(symbol)
        spot_bid = MarketDataStore.quote[symbol].bid
        spot_ask = MarketDataStore.quote[symbol].ask
        linear_bid = MarketDataStore.quote[linear_symbol].bid
        linear_ask = MarketDataStore.quote[linear_symbol].ask
        
        if not notional and not amount:
            raise Exception("Either 'notional' or 'amount' must be provided.")
        elif not amount:
            amount = notional / linear_ask if not close_position else notional / linear_bid
        amount = float(self._exchange.api.amount_to_precision(symbol, amount))
        
        remain_amount = 0
        while True:
            if time.time() - start_time > wait:
                logging.info(f"Operation for {symbol} timed out after {wait} seconds. Cancelling order if exists.")
                if order_placed and res:
                    try:
                        await self._order.cancel_order(res['id'], linear_symbol)
                        logging.info(f"[TIME OUT] Cancelled order {res['id']} for {linear_symbol}")
                    except Exception as e:
                        logging.error(f"[TIME OUT] Error cancelling order for {linear_symbol}: {e}")
                return False
            
            curr_spot_bid = MarketDataStore.quote[symbol].bid
            curr_spot_ask = MarketDataStore.quote[symbol].ask
            curr_linear_bid = MarketDataStore.quote[linear_symbol].bid
            curr_linear_ask = MarketDataStore.quote[linear_symbol].ask
            
            ratio = curr_linear_ask / curr_spot_ask - 1 if not close_position else curr_linear_bid / curr_spot_bid - 1
            logging.info(f"symbol: {symbol}, ratio: {ratio}, open_ratio: {open_ratio}, spot_bid: {curr_spot_bid}, spot_ask: {curr_spot_ask}, linear_bid: {curr_linear_bid}, linear_ask: {curr_linear_ask}")
            if not order_placed:
                amount = remain_amount if remain_amount > 0 else amount
                if close_position:
                    price = (open_ratio + 1) * curr_spot_bid
                    price = float(self._exchange.api.price_to_precision(linear_symbol, price))
                    res = await self._order.place_limit_order(
                        symbol=linear_symbol,
                        side='buy',
                        amount=amount,
                        price=price,
                        close_position=True,
                        client_order_id=self.client_id,
                    )
                else:
                    price = (open_ratio + 1) * curr_spot_ask
                    price = float(self._exchange.api.price_to_precision(linear_symbol, price))
                    res = await self._order.place_limit_order(
                        symbol=linear_symbol,
                        side='sell',
                        amount=amount,
                        price=price,
                        client_order_id=self.client_id,   
                    )
                order_placed = True
            
            if order_placed and res:
                order_res = await self._exchange.api.fetch_order(id=res['id'], symbol=linear_symbol)
                #TODO check if order is filled
                # if order_res['status'] == 'closed':
                #     logging.info(f"Order {res['id']} for {linear_symbol} has been filled.")
                #     if res['id'] in self.orders:
                #         logging.info(f"[SOCKET DELAY] Symbol: {symbol}")
                #         await self.on_filled_order(order_res)
                #     return True
            
            if close_position:
                if order_placed and curr_spot_bid != spot_bid: # if curr_spot_bid changes, cancel the order
                    curr_price = (open_ratio + 1) * curr_spot_bid
                    curr_price = float(self._exchange.api.price_to_precision(linear_symbol, curr_price))
                    if curr_price != price:
                        try:
                            res = await self._order.cancel_order(res['id'], linear_symbol)
                            remain_amount = res['remaining']
                            spot_bid = curr_spot_bid
                            order_placed = False
                        except Exception as e:
                            logging.error(f"Error cancelling order for {linear_symbol}: {e}")
                    else:
                        logging.info(f"Price: {curr_price} has not changed for {linear_symbol}.")
            else:
                if order_placed and curr_spot_ask != spot_ask:
                    curr_price = (open_ratio + 1) * curr_spot_ask
                    curr_price = float(self._exchange.api.price_to_precision(linear_symbol, curr_price))
                    if curr_price != price:
                        try:
                            res = await self._order.cancel_order(res['id'], linear_symbol)
                            remain_amount = res['remaining']
                            spot_ask = curr_spot_ask
                            order_placed = False
                        except Exception as e:
                            logging.error(f"Error cancelling order for {linear_symbol}: {e}")
                    else:
                        logging.info(f"Price: {curr_price} has not changed for {linear_symbol}.")
            
            if not res:
                return True
            
            await asyncio.sleep(time_interval)
        



        