import ssl
import asyncio

import uvloop
import nats

from bot import TradingBot
from manager import NatsManager


async def main():
    config = {
        'exchange_id': 'binance',
    }
    bot = TradingBot(config)
    await bot.run()


if __name__ == '__main__':
    uvloop.install()
    asyncio.run(main())