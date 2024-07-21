import asyncio

import uvloop

from bot import Bot


async def main():
    config = {
        'exchange_id': 'binance',
        'sandbox': False,
        'apiKey': 'your-api-key',
        'secret': 'your-secret', 
    }
    bot = Bot(config)
    await bot.run()


if __name__ == '__main__':
    uvloop.install()
    asyncio.run(main())