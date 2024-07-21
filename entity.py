import logging
import asyncio


from collections import defaultdict
from typing import Dict, List, Callable, Any, Literal
from dataclasses import dataclass, fields


@dataclass(slots=True)
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
    
class Quote:
    __slots__ = ['ask', 'bid']

    def __init__(self, ask=0, bid=0):
        self.ask = ask
        self.bid = bid

    def __getitem__(self, key):
        if key == 'ask':
            return self.ask
        elif key == 'bid':
            return self.bid
        else:
            raise KeyError(f"Invalid key: {key}")

    def __setitem__(self, key, value):
        if key == 'ask':
            self.ask = value
        elif key == 'bid':
            self.bid = value
        else:
            raise KeyError(f"Invalid key: {key}")

    def __repr__(self):
        return f"Quote(ask={self.ask}, bid={self.bid})"   
    
    
class MarketDataStore:
    data:Dict[str, Quote] = defaultdict(Quote)
    open_ratio = {}
    close_ratio = {}
    
    @classmethod
    async def update(cls, data: Dict):
        symbol = data['s']
        cls.data[symbol] = Quote(
            ask=float(data['a']),
            bid=float(data['b'])
        )
        
        logging.debug(f"Updated {symbol} bid: {data['b']} ask: {data['a']}")
        
        spot_symbol = symbol.replace(':USDT', '') if ':' in symbol else symbol
        await cls.calculate_ratio(spot_symbol)
            
    
    @classmethod
    async def calculate_ratio(cls, spot_symbol: str):
        linear_symbol = f'{spot_symbol}:USDT'
        if spot_symbol in cls.data and linear_symbol in cls.data:
            spot_bid = cls.data[spot_symbol].bid
            spot_ask = cls.data[spot_symbol].ask
            linear_bid = cls.data[linear_symbol].bid
            linear_ask = cls.data[linear_symbol].ask
            
            # cls.close_ratio[spot_symbol] = linear_bid / spot_bid - 1
            # cls.open_ratio[spot_symbol] = linear_ask / spot_ask - 1
            
            cls.open_ratio[spot_symbol] = linear_bid / spot_ask - 1
            cls.close_ratio[spot_symbol] = linear_ask / spot_bid - 1
            
            await EventSystem.emit('ratio_changed', spot_symbol, cls.open_ratio[spot_symbol], cls.close_ratio[spot_symbol])


class EventSystem:
    _listeners: Dict[str, List[Callable]] = {}

    @classmethod
    def on(cls, event: str, callback: Callable):
        if event not in cls._listeners:
            cls._listeners[event] = []
        cls._listeners[event].append(callback)

    @classmethod
    async def emit(cls, event: str, *args: Any, **kwargs: Any):
        if event in cls._listeners:
            for callback in cls._listeners[event]:
                if asyncio.iscoroutinefunction(callback):
                    await callback(*args, **kwargs)
                else:
                    callback(*args, **kwargs)

