from decimal import Decimal, ROUND_FLOOR
import logging
import os
import time

from json.decoder import JSONDecodeError
import pandas as pd
from requests.exceptions import ConnectionError

import gdax

from utils import connection_retry


logger = logging.getLogger(__name__)


class GDAXTrader:
    """
    Run trading strategy and interact with GDAX API

    Attributes:
        product: The product being tracked by the trader
        strategies: The strategies being run by the trader
        client: The GDAX API client
    """

    # Environment variables required for authenticating with GDAX
    GDAX_KEY_ENV = 'GDAX_KEY'
    GDAX_SECRET_ENV = 'GDAX_SECRET'
    GDAX_PASSPHRASE_ENV = 'GDAX_PASSPHRASE'
    GDAX_API_URL_ENV = 'GDAX_API_URL'

    # GDAX rate limit of 3 requests per second with a little extra padding
    RATE_LIMIT = 1.0 / 3.0 + 0.5

    # Frequency of order book scrapes in seconds
    FREQUENCY = 60

    # Maximum number of retry attempts after a connection error
    MAX_RETRIES = 5

    def __init__(self):
        self.product = None
        self.strategies = []
        self.client = GDAXTrader._get_client()

    def set_product(self, product):
        self.product = product

    def add_strategy(self, strategy):
        strategy.add_trader(self)
        self.strategies.append(strategy)

    def run(self):
        """
        Start the GDAX trading algorithm

        Uses set product and strategies added to the `GDAXTrader`.
        """

        running = True

        logger.info('Starting GDAX Trader...')

        start_time = time.time()

        while running:
            success = self._run_iteration()

            if not success:
                logger.warning('Data unavailable, iteration skipped...')

            # Sleep to achieve the desired frequency
            elapsed_time = (time.time() - start_time) % GDAXTrader.FREQUENCY
            sleep_time = GDAXTrader.FREQUENCY - elapsed_time

            logger.info('Sleeping for {:.2f} seconds'.format(sleep_time))

            time.sleep(sleep_time)

    def _run_iteration(self):
        """
        Perform an iteration of the GDAX trading algorithm
        """

        # Retrieve account data
        try:
            accounts = self._get_accounts()
        # Skip iteration if account data is unavailable
        except (ConnectionError, JSONDecodeError):
            return False

        # Get order book data
        try:
            order_book = self._get_order_book(self.product)

        # Skip iteration if ticker data is unavailable
        except (ConnectionError, JSONDecodeError):
            return False

        # For some reason the data is contained in a single element tuple
        try:
            order_book = order_book[0]
        except TypeError:
            return False

        bid_orders, ask_orders = self._order_book_to_df(order_book)

        # Update all strategies
        for strategy in self.strategies:
            logger.info('Next iteration...')
            strategy.next_data(accounts, bid_orders, ask_orders)
            strategy.next()

        return True

    @classmethod
    def _get_client(cls):
        """
        Get an authenticated GDAX client

        :returns: an authenticated GDAX client
        :raises KeyError: Error raised if environment variables are not set
        """

        try:
            key = os.environ[GDAXTrader.GDAX_KEY_ENV]
            secret = os.environ[GDAXTrader.GDAX_SECRET_ENV]
            passphrase = os.environ[GDAXTrader.GDAX_PASSPHRASE_ENV]
        except KeyError as error:
            raise KeyError('Missing environment variable for GDAX: '.format(error))

        try:
            api_url = os.environ[GDAXTrader.GDAX_API_URL_ENV]
        except KeyError:
            client = gdax.AuthenticatedClient(key, secret, passphrase)
        else:
            client = gdax.AuthenticatedClient(key, secret, passphrase,
                    api_url=api_url)

        return client

    def _order_book_to_df(self, order_book):
        """
        Converts raw order book data to bid/ask dataframes

        :param order_book: order book data
        :returns: tuple(bid_orders, ask_orders)
        """

        try:
            columns = order_book['bids'].pop(0)
            bid_orders = pd.DataFrame(list(order_book['bids']), columns=columns)
        except KeyError as error:
            logger.warning(error)
            bid_orders = None

        try:
            columns = order_book['ask'].pop(0)
            ask_orders = pd.DataFrame(list(order_book['ask']), columns=columns)
        except KeyError as error:
            logger.warning(error)
            ask_orders = None

        return bid_orders, ask_orders

    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def _get_order_book(self, product):
        """
        Get order book data for a product

        :param product: the GDAX product
        :returns: order book data
        """

        return self.client.get_product_order_book(product, level=2)

    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def _get_accounts(self):
        """
        Get accounts data

        :returns: accounts data
        """

        return self.client.get_accounts()

    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def get_order(self, order_id):
        """
        Get a single order

        :param order_id: the order ID
        :returns: the order
        """

        return self.client.get_order(order_id)

    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def get_fills(self, order_id):
        """
        Get fills for an order

        :param order_id: the order ID
        :returns: list of fills
        """

        return self.client.get_fills(order_id=order_id)

    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def buy(self, price, size, product):
        """
        Place buy order for a product

        :param price: the maximum price that will be accepted
        :size: the amount to buy
        :product: the product to place the buy order for
        :returns: order data
        """

        logger.info('BUY: {} of {}, PRICE: {}'.format(size, product, price))

        try:
            price_str = str(price)
            size_str = str(size.quantize(Decimal('1.00000000'),
                    rounding=ROUND_FLOOR))
        except ValueError as error:
            logger.warning(error)
            return None

        order = self.client.buy(price=price_str, size=size_str, product_id=product,
                post_only=True)

        logger.info('ORDER: {}'.format(order))

        return order

    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def sell(self, price, size, product):
        """
        Place sell order for a product

        :param price: the minimum price that will be accepted
        :size: the amount to sell
        :product: the product to place the sell order for
        :returns: order data
        """

        logger.info('SELL: {} of {}, PRICE: {}'.format(size, product, price))

        try:
            price_str = str(price)
            size_str = str(size.quantize(Decimal('1.00000000'),
                    rounding=ROUND_FLOOR))
        except ValueError as error:
            logger.warning(error)
            return None

        order = self.client.sell(price=price_str, size=size_str, product_id=product,
                post_only=True)

        logger.info('ORDER: {}'.format(order))

        return order

    @connection_retry(MAX_RETRIES, RATE_LIMIT)
    def cancel_order(self, order_id):
        """
        Cancel an order

        :param order_id: the order ID
        :returns: the API response
        """

        logger.info('CANCEL: {}'.format(order_id))
        return self.client.cancel_order(order_id)
