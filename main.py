import asyncio
import logging


import uvloop


from bot import Bot


from configparser import ConfigParser


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


config = ConfigParser()
config.read('keys/config.cfg')

API_KEY = config['binance_2']['API_KEY']
API_SECRET = config['binance_2']['SECRET']


async def main():
    config = {
        'exchange_id': 'binance',
        'sandbox': False,
        'apiKey': API_KEY,
        'secret': API_SECRET, 
    }
    bot = Bot(config)
    await bot.run()


if __name__ == '__main__':
    uvloop.install()
    asyncio.run(main())