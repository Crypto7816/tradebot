import logging
from typing import List

from utils import EventSystem
from manager import NatsManager, OrderManager, ExchangeManager

class TradingBot:
    def __init__(self, config):
        self._config = config
        self._exchange = ExchangeManager(config)
        self._order = OrderManager(self._exchange)
        self._nats = NatsManager()
        
        self.position = []
        
        EventSystem.on('ratio_changed', self.on_ratio_changed)

    async def on_ratio_changed(self, symbol: str, open_ratio: float, close_ratio: float):
        if open_ratio > 0.00065 and symbol not in self.position:
            logging.info(f"Opening position for {symbol} at {open_ratio}")
            self.position.append(symbol)
        if close_ratio < 0 and symbol in self.position:
            logging.info(f"Closing position for {symbol} at {close_ratio}")
            self.position.remove(symbol)
    
    async def run(self):
        await self._nats.subscribe()


        