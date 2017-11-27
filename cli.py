import logging
logging.basicConfig(level=logging.INFO)

from gdax_trader import GDAXTrader
from strategies.arbitrage import ArbitrageStrategy


if __name__ == '__main__':
    trader = GDAXTrader()

    trader.set_product('BTC-USD')

    strategy = ArbitrageStrategy()
    trader.add_strategy(strategy)

    trader.run()
