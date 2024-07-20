import logging
from typing import List

from entity import EventSystem
from manager import NatsManager, OrderManager, ExchangeManager

class TradingBot:
    def __init__(self, config):
        self._config = config
        self._exchange = ExchangeManager(config)
        self._order = OrderManager(self._exchange)
        self._nats = NatsManager()
        
        
        EventSystem.on('new_order', self._on_new_order)
        EventSystem.on('filled_order', self._on_filled_order)
        EventSystem.on('partially_filled_order', self._on_partially_filled_order)
        EventSystem.on('canceled_order', self._on_canceled_order)
        
    async def run(self):
        await self._nats.subscribe()
        
    async def _on_new_order(self, order):
        pass
    
    async def _on_filled_order(self, order):
        pass
    
    async def _on_partially_filled_order(self, order):
        pass
    
    async def _on_canceled_order(self, order):
        pass

class Bot(TradingBot):
    def __init__(self, config):
        super().__init__(config)
        self.position = []
        
        EventSystem.on('ratio_changed', self.on_ratio_changed)

    async def on_ratio_changed(self, symbol: str, open_ratio: float, close_ratio: float):
        if open_ratio > 0.00065 and symbol not in self.position:
            logging.info(f"Opening position for {symbol} at {open_ratio}")
            self.position.append(symbol)
        if close_ratio < 0 and symbol in self.position:
            logging.info(f"Closing position for {symbol} at {close_ratio}")
            self.position.remove(symbol)
    



        