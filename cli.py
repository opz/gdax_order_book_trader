import logging
logging.basicConfig(level=logging.INFO)

from gdax_trader import GDAXTrader
from strategies.order_book_imbalance import OBIStrategy


if __name__ == '__main__':
    trader = GDAXTrader()

    trader.set_product('BTC-USD')

    strategy = OBIStrategy()
    trader.add_strategy(strategy)

    trader.run()
