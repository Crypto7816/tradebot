import logging
from typing import List

from utils import EventSystem


class TradingBot:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        EventSystem.on('ratio_changed', self.on_ratio_changed)

    async def on_ratio_changed(self, symbol: str, bid_ratio: float, ask_ratio: float):
        # if symbol in self.symbols:
        if ask_ratio > 0.001 and bid_ratio < 0:
            logging.info(f"Ratio changed for {symbol}: bid ratio: {bid_ratio} ask ratio: {ask_ratio}")