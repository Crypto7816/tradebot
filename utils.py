from collections import defaultdict
import logging
from typing import Dict, Literal
from dataclasses import dataclass, fields


class EventSystem:
    listeners = defaultdict(list)

    @classmethod
    async def emit(cls, event, *args):
        logging.debug(f"Emitting event: {event}")
        for listener in cls.listeners[event]:
            await listener(*args)

    @classmethod
    def on(cls, event, callback):
        cls.listeners[event].append(callback)
        logging.debug(f"Added listener for event: {event}")

@dataclass
class OrderResponse:
    id: str
    symbol: str
    status: str
    side: Literal['buy', 'sell']
    amount: float
    filled: float
    client_order_id: str
    average: float
    price: float
    
    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            raise KeyError(key)

    def keys(self):
        return [f.name for f in fields(self)]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(fields(self))
    
    
class MarketDataStore:
    data = defaultdict(dict)
    bid_ratio = {}
    ask_ratio = {}
    
    @classmethod
    async def update(cls, data: Dict):
        symbol = data['s']
        cls.data[symbol] = {
            'bid': float(data['b']),
            'ask': float(data['a'])
        }
        
        logging.debug(f"Updated {symbol} bid: {data['b']} ask: {data['a']}")
        
        spot_symbol = symbol.replace(':USDT', '') if ':' in symbol else symbol
        await cls.calculate_ratio(spot_symbol)
            
    
    @classmethod
    async def calculate_ratio(cls, spot_symbol: str):
        linear_symbol = f'{spot_symbol}:USDT'
        if spot_symbol in cls.data and linear_symbol in cls.data:
            spot_bid = cls.data[spot_symbol]['bid']
            spot_ask = cls.data[spot_symbol]['ask']
            linear_bid = cls.data[linear_symbol]['bid']
            linear_ask = cls.data[linear_symbol]['ask']
            
            cls.bid_ratio[spot_symbol] = linear_ask / spot_ask - 1
            cls.ask_ratio[spot_symbol] = linear_bid / spot_bid - 1
            
            await EventSystem.emit('ratio_changed', spot_symbol, cls.bid_ratio[spot_symbol], cls.ask_ratio[spot_symbol])

    
class Application:
    pass