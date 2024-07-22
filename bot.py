import logging
import asyncio
from typing import List

from entity import EventSystem, MarketDataStore
from manager import NatsManager, OrderManager, ExchangeManager, AccountManager

class TradingBot:
    def __init__(self, config):
        self._config = config
        self._exchange = ExchangeManager(config)
        self._order = OrderManager(self._exchange)
        self._account = AccountManager(self._exchange)
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
        self.position = []
        # EventSystem.on('ratio_changed', self.on_ratio_changed)

    async def on_new_order(self, order):
        logging.info(f"New order: {order}")
    
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
    



        