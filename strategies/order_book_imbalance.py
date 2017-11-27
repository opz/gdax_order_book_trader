from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from json.decoder import JSONDecodeError

import logging

from strategy import Strategy


logger = logging.getLogger(__name__)


class OBIStrategy(Strategy):
    """
    Use order book imbalance to trade on price momentum

    Attributes:
        order: The currently open order
    """

    BUY_SIGNAL = 'buy'
    SELL_SIGNAL = 'sell'

    PERIOD = 30

    MINIMUM_HOLD_TIME = 20 # seconds to hold a limit order before cancelling

    LIMIT_PADDING = 0.01 # Amount to pad limit order prices

    def set_up(self):
        self.order = None
        self.order_book_imbalance = []

    def next(self):
        # Get the trade signal for the current node
        signal = self._get_trade_signal()

        logger.info('Next trade signal: {}'.format(signal))

        # Update open orders
        if self._track_order():
            logger.info('Update pending order...')
            self._update_pending_order(signal)
            return

        logger.info('ORDER: {}'.format(self.order))

        logger.info('Place new order...')
        self._place_order(signal)

    def _track_order(self):
        """
        Track the order if it is available

        Updates the current order with new data, or clears the current order
        if it no longer exists.

        :returns: `True` if the order has updated, `False` if it does not exist
        """

        try:
            order_id = self.order['id']
        except (KeyError, TypeError):
            return False

        try:
            order = self.trader.get_order(order_id)
        except (ConnectionError, JSONDecodeError) as error:
            logger.warning(error)
            return False

        # Check if a partially filled order has been cancelled
        try:
            cancelled = order['done_reason'] == 'cancelled'
        except KeyError:
            cancelled = False

        try:
            rejected = order['status'] == 'rejected'
        except KeyError:
            rejected = False

        # Check if order has been completed
        try:
            is_done = order['status'] == 'done'
        except KeyError:
            is_done = False

        # Check if order has been settled
        try:
            is_settled = order['settled'] == True
        except KeyError:
            is_settled = False

        # An unfilled order that was cancelled will throw an error
        order_error = 'message' in order

        # Clear the order if it no longer exists or has been cancelled
        if order_error or cancelled or rejected or (is_done and is_settled):
            self.order = None
            return False
        else:
            self.order = order

        return True

    def _get_trade_signal(self):
        """
        Get the Buy/Sell trade signal based on order book imbalance

        Returns `None` if there is no trade signal.

        :returns: tuple(trade_signal, currency_pair, distance)
            - trade_signal: either a buy or a sell signal
        """

        signal = None

        delta = 2
        beta = 1

        best_bid = self._get_market_price(OBIStrategy.BUY_SIGNAL)
        best_ask = self._get_market_price(OBIStrategy.SELL_SIGNAL)

        bid_qty = 0
        for index, row in self.bid_orders.iterrows():
            price_diff = abs(row['price'] - best_bid)
            price_pct_diff = price_diff / best_bid
            bid_qty += row['size'] / (delta * price_pct_diff + beta)

        ask_qty = 0
        for index, row in self.ask_orders.iterrows():
            price_diff = abs(row['price'] - best_ask)
            price_pct_diff = price_diff / best_ask
            ask_qty += row['size'] / (delta * price_pct_diff + beta)

        order_book_imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)

        self.order_book_imbalance.append(order_book_imbalance)

        if len(self.order_book_imbalance) > OBIStrategy.PERIOD:
            last_period_obi = self.order_book_imbalance[-OBIStrategy.PERIOD:]

            threshold = pd.DataFrame(last_period_obi).std()[0]
            buy_threshold = threshold * 2
            sell_threshold = -threshold * 2

            logger.info('OBI: {}, Threshold: {}:{}'.format(
                    order_book_imbalance, buy_threshold, sell_threshold))

            if order_book_imbalance > buy_threshold:
                signal = OBIStrategy.BUY_SIGNAL
            elif order_book_imbalance < sell_threshold:
                signal = OBIStrategy.SELL_SIGNAL

        logger.info('Trade signal: {}'.format(signal))

        return signal

    def _get_market_price(self, signal):
        """
        Get the market price based on the trade signal for a product

        A buy signal uses the bid price, a sell signal uses the ask price.

        :param signal: the trade signal
        :returns: the market price
        """

        try:
            market_price = Decimal(self.order['price'])
        except (KeyError, TypeError, InvalidOperation):
            market_price = None

        if signal == OBIStrategy.BUY_SIGNAL:
            market_price = Decimal(self.bid_orders['price'].max())

        elif signal == OBIStrategy.SELL_SIGNAL:
            market_price = Decimal(self.ask_orders['price'].min())

        return market_price

    def _cancel_order(self):
        """
        Cancel the current order

        :returns: `True` if success, `False` if there is no current order
        """

        try:
            order_id = self.order['id']
        except (KeyError, TypeError):
            return False

        try:
            self.trader.cancel_order(order_id)
        except (ConnectionError, JSONDecodeError) as error:
            logger.warning(error)
            return False

        return True

    def _update_pending_order(self, signal):
        """
        Make adjustments to a pending order based on new data

        Reprice the order if market conditions change and cancel the order if
        the trade signal changes.

        :param signal: the trade signal
        :returns: `True` on success, `False` when there is no pending order
        """

        try:
            price = Decimal(self.order['price'])
            created_at = self.order['created_at']
        except (KeyError, TypeError, InvalidOperation):
            return False

        market_price = self._get_market_price(signal)

        timeframe = timedelta(seconds=OBIStrategy.MINIMUM_HOLD_TIME)
        created_at_dt = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ')
        created_at_dt = created_at_dt.replace(tzinfo=timezone.utc)
        current_time = datetime.now(timezone.utc)
        elapsed_time = current_time - created_at_dt

        logger.info('Time elapsed since order creation: {}'.format(elapsed_time))

        can_be_cancelled = elapsed_time > timeframe

        if signal == OBIStrategy.BUY_SIGNAL:
            price_changed = price != (market_price + OBIStrategy.LIMIT_PADDING)

        elif signal == OBIStrategy.BUY_SIGNAL:
            price_changed = price != (market_price - OBIStrategy.LIMIT_PADDING)

        if not signal or (can_be_cancelled and price_changed):
            logger.info('Cancel pending order')
            self._cancel_order()
        else:
            logger.info('Pending order unchanged')

        return True

    def get_product_base(self):
        """
        Get the base currency from the product

        :returns: the base currency
        """

        currency_pair = self.trader.product.split('-')

        try:
            currency = currency_pair[0]
        except IndexError:
            currency = None

        return currency

    def get_product_quote(self):
        """
        Get the quote currency from the product

        :returns: the quote currency
        """

        currency_pair = self.trader.product.split('-')

        try:
            currency = currency_pair[1]
        except IndexError:
            currency = None

        return currency

    def _place_order(self, signal):
        """
        Place a new order based on the trade signal

        :param signal: the trade signal
        :returns: `True` if order is placed, `False` if no order is placed
        """

        # Do not place a new order if an old order is still open
        if self.order != None:
            return False

        market_price = self._get_market_price(signal)

        if signal == OBIStrategy.BUY_SIGNAL:
            logger.info('Signal indicates BUY order')

            currency = self.get_product_quote()
            size = self.get_currency_balance(currency) / market_price
            logger.info('Size of position: {}'.format(size))

            try:
                order = self.trader.buy(market_price, size, self.trader.product)
            except (ConnectionError, JSONDecodeError) as error:
                logger.warning(error)
                return False

            if 'message' not in order:
                self.order = order

            return True

        elif signal == OBIStrategy.SELL_SIGNAL:
            logger.info('Signal indicates SELL order')

            currency = self.get_product_base()
            size = self.get_currency_balance(currency)
            logger.info('Size of position: {}'.format(size))

            try:
                order = self.trader.sell(market_price, size, self.trader.product)
            except (ConnectionError, JSONDecodeError) as error:
                logger.warning(error)
                return False

            if 'message' not in order:
                self.order = order

            return True

        logger.info('No signal indicated')

        return False
