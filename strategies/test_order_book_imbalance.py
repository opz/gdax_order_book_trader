from datetime import datetime, timedelta
from decimal import Decimal
import unittest
from unittest.mock import MagicMock, patch, call

import pandas as pd

from strategies.order_book_imbalance import OBIStrategy

class OBIStrategyTestCase(unittest.TestCase):
    """
    Test :class:`OBIStrategy`

    Methods:
        - :meth:`OBIStrategy.next`
        - :meth:`OBIStrategy._track_order`
        - :meth:`OBIStrategy._get_market_price`
        - :meth:`OBIStrategy._cancel_order`
        - :meth:`OBIStrategy._update_pending_order`
        - :meth:`OBIStrategy._place_order`
    """

    def test_next_place_new_order(self):
        """
        Test :meth:`OBIStrategy.next`

        Assert :meth:`OBIStrategy._place_order` is called.
        """

        obi = OBIStrategy()

        TEST_CURRENCY = 'USD'
        obi.accounts = [
            {
                'currency': TEST_CURRENCY,
                'balance': Decimal(1),
            }
        ]

        trade_signal = OBIStrategy.BUY_SIGNAL
        obi._get_trade_signal = MagicMock(return_value=trade_signal)

        obi._track_order = MagicMock(return_value=False)
        obi._update_pending_order = MagicMock()
        obi._place_order = MagicMock()

        obi.next()

        self.assertEqual(obi._update_pending_order.called, 0)
        self.assertEqual(obi._place_order.called, 1)

    def test_next_update_pending_order(self):
        """
        Test :meth:`OBIStrategy.next`

        Assert :meth:`OBIStrategy._update_pending_order` is called.
        """

        obi = OBIStrategy()

        TEST_CURRENCY = 'USD'
        obi.accounts = [
            {
                'currency': TEST_CURRENCY,
                'balance': Decimal(1),
            }
        ]

        trade_signal = OBIStrategy.BUY_SIGNAL
        obi._get_trade_signal = MagicMock(return_value=trade_signal)

        obi._track_order = MagicMock(return_value=True)
        obi._update_pending_order = MagicMock()
        obi._place_order = MagicMock()

        obi.next()

        self.assertEqual(obi._update_pending_order.called, 1)
        self.assertEqual(obi._place_order.called, 0)

    def test__track_order_success(self):
        """
        Test :meth:`OBIStrategy._track_order`

        Assert the current order data is updated and `True` is returned.
        """

        obi = OBIStrategy()

        test_order = {
            'id': 1,
        }
        obi.order = test_order

        trader = MagicMock()

        test_order_update = {
            'id': 2,
        }
        trader.get_order.return_value = test_order_update

        obi.trader = trader

        success = obi._track_order()

        self.assertTrue(success)
        self.assertEqual(obi.order, test_order_update)

    def test__track_order_with_no_current_order(self):
        """
        Test :meth:`OBIStrategy._track_order`

        Assert `False` is returned.
        """

        obi = OBIStrategy()

        obi.order = None

        trader = MagicMock()

        test_order_update = {
            'id': 2,
        }
        trader.get_order.return_value = test_order_update

        obi.trader = trader

        success = obi._track_order()

        self.assertFalse(success)
        self.assertEqual(obi.order, None)

    def test__track_order_with_connection_error(self):
        """
        Test :meth:`OBIStrategy._track_order`

        Assert the current order data is cleared and `False` is returned.
        """

        obi = OBIStrategy()

        test_order = {
            'id': 1,
        }
        obi.order = test_order

        trader = MagicMock()

        trader.get_order.side_effect = ConnectionError

        obi.trader = trader

        success = obi._track_order()

        self.assertFalse(success)
        self.assertEqual(obi.order,
                test_order)

    def test__track_order_that_has_been_cancelled(self):
        """
        Test :meth:`OBIStrategy._track_order`

        Assert the current order data is cleared and `False` is returned.
        """

        obi = OBIStrategy()

        test_order = {
            'id': 1,
        }
        obi.order = test_order

        trader = MagicMock()

        test_order_update = {
            'id': 2,
            'done_reason': 'cancelled',
        }
        trader.get_order.return_value = test_order_update

        obi.trader = trader

        success = obi._track_order()

        self.assertFalse(success)
        self.assertEqual(obi.order, None)

    def test__track_order__that_has_been_filled(self):
        """
        Test :meth:`OBIStrategy._track_order`

        Assert the current order data is cleared and `False` is returned.
        """

        obi = OBIStrategy()

        test_order = {
            'id': 1,
        }
        obi.order = test_order

        trader = MagicMock()

        test_order_update = {
            'id': 2,
            'status': 'done',
            'settled': True,
        }
        trader.get_order.return_value = test_order_update

        obi.trader = trader

        success = obi._track_order()

        self.assertFalse(success)
        self.assertEqual(obi.order, None)

    def test__get_market_price_with_no_signal(self):
        """
        Test :meth:`OBIStrategy._get_market_price`

        Assert the returned market price is the same as the current order
        price.
        """

        obi = OBIStrategy()

        TEST_PRICE = 1.0
        obi.order = {
            'price': TEST_PRICE,
        }

        TEST_BID = 2.0
        TEST_ASK = 3.0

        columns = ['price', 'size', 'num-orders']

        bid_orders = [
            ['{}'.format(TEST_BID), '1.0', 1],
        ]
        obi.bid_orders = pd.DataFrame(bid_orders, columns=columns)

        ask_orders = [
            ['{}'.format(TEST_ASK), '1.0', 1],
        ]
        obi.ask_orders = pd.DataFrame(ask_orders, columns=columns)

        TEST_SIGNAL = None
        market_price = obi._get_market_price(TEST_SIGNAL)

        self.assertEqual(market_price, TEST_PRICE)

    def test__get_market_price_with_no_order_and_no_signal(self):
        """
        Test :meth:`OBIStrategy._get_market_price`

        Assert the returned market price is `None`.
        """

        obi = OBIStrategy()

        obi.order = None

        TEST_BID = 2.0
        TEST_ASK = 3.0

        columns = ['price', 'size', 'num-orders']

        bid_orders = [
            ['{}'.format(TEST_BID), '1.0', 1],
        ]
        obi.bid_orders = pd.DataFrame(bid_orders, columns=columns)

        ask_orders = [
            ['{}'.format(TEST_ASK), '1.0', 1],
        ]
        obi.ask_orders = pd.DataFrame(ask_orders, columns=columns)

        TEST_SIGNAL = None
        market_price = obi._get_market_price(TEST_SIGNAL)

        self.assertEqual(market_price, None)

    def test__get_market_price_with_buy_signal(self):
        """
        Test :meth:`OBIStrategy._get_market_price`

        Assert the returned market price is the ticker bid price.
        """

        obi = OBIStrategy()

        TEST_PRICE = 1.0
        obi.order = {
            'price': TEST_PRICE,
        }

        TEST_BID = 2.0
        TEST_ASK = 3.0

        columns = ['price', 'size', 'num-orders']

        bid_orders = [
            ['{}'.format(TEST_BID), '1.0', 1],
        ]
        obi.bid_orders = pd.DataFrame(bid_orders, columns=columns)

        ask_orders = [
            ['{}'.format(TEST_ASK), '1.0', 1],
        ]
        obi.ask_orders = pd.DataFrame(ask_orders, columns=columns)

        market_price = obi._get_market_price(OBIStrategy.BUY_SIGNAL)

        self.assertEqual(market_price, TEST_BID)

    def test__get_market_price_with_sell_signal(self):
        """
        Test :meth:`OBIStrategy._get_market_price`

        Assert the returned market price is the ticker ask price.
        """

        obi = OBIStrategy()

        TEST_PRICE = 1.0
        obi.order = {
            'price': TEST_PRICE,
        }

        TEST_BID = 2.0
        TEST_ASK = 3.0

        columns = ['price', 'size', 'num-orders']

        bid_orders = [
            ['{}'.format(TEST_BID), '1.0', 1],
        ]
        obi.bid_orders = pd.DataFrame(bid_orders, columns=columns)

        ask_orders = [
            ['{}'.format(TEST_ASK), '1.0', 1],
        ]
        obi.ask_orders = pd.DataFrame(ask_orders, columns=columns)

        market_price = obi._get_market_price(OBIStrategy.SELL_SIGNAL)

        self.assertEqual(market_price, TEST_ASK)

    def test__cancel_order_with_valid_order(self):
        """
        Test :meth:`OBIStrategy._cancel_order`

        Assert :meth:`GDAXTrader.cancel_order` is called with current order ID
        and `True` is returned.
        """

        obi = OBIStrategy()

        TEST_ID = 1
        obi.order = {
            'id': TEST_ID,
        }

        trader = MagicMock()
        obi.trader = trader

        success = obi._cancel_order()

        trader.cancel_order.assert_called_with(TEST_ID)
        self.assertTrue(success)

    def test__cancel_order_without_order(self):
        """
        Test :meth:`OBIStrategy._cancel_order`

        Assert `False` is returned.
        """

        obi = OBIStrategy()

        obi.order = None

        trader = MagicMock()
        obi.trader = trader

        success = obi._cancel_order()

        self.assertFalse(success)

    def test__update_pending_order_with_no_change(self):
        """
        Test :meth:`OBIStrategy._update_pending_order`

        Assert :meth:`OBIStrategy._cancel_order` is not called and
        `True` is returned.
        """

        obi = OBIStrategy()

        TEST_PRICE = 1.0
        TEST_CREATED = '2017-01-01T01:00:00.000000Z'
        obi.order = {
            'price': TEST_PRICE,
            'created_at': TEST_CREATED,
        }

        TEST_MARKET_PRICE = TEST_PRICE - OBIStrategy.LIMIT_PADDING
        obi._get_market_price = MagicMock(return_value=TEST_MARKET_PRICE)

        cancel_order = MagicMock()
        obi._cancel_order = cancel_order

        success = obi._update_pending_order(OBIStrategy.BUY_SIGNAL)

        self.assertEqual(cancel_order.called, 0)
        self.assertTrue(success)

    def test__update_pending_order_without_order(self):
        """
        Test :meth:`OBIStrategy._update_pending_order`

        Assert :meth:`OBIStrategy._cancel_order` is not called and
        `False` is returned.
        """

        obi = OBIStrategy()

        obi.order = None

        TEST_PRICE = 1.0
        obi._get_market_price = MagicMock(return_value=TEST_PRICE)

        cancel_order = MagicMock()
        obi._cancel_order = cancel_order

        success = obi._update_pending_order(OBIStrategy.BUY_SIGNAL)

        self.assertEqual(cancel_order.called, 0)
        self.assertFalse(success)

    @patch('strategies.order_book_imbalance.datetime')
    def test__update_pending_order_with_change(self, datetime):
        """
        Test :meth:`OBIStrategy._update_pending_order`

        Assert :meth:`OBIStrategy._cancel_order` is called and `True`
        is returned.
        """

        TEST_NOW = '2017-01-01T01:00:00.000000Z'
        now = datetime.strptime(TEST_NOW)
        timeframe = timedelta(seconds=OBIStrategy.MINIMUM_HOLD_TIME + 1)
        now.__sub__ = MagicMock(return_value=timeframe)
        datetime.now.return_value = now

        obi = OBIStrategy()

        test_created = now + timeframe
        TEST_PRICE = 1.0
        obi.order = {
            'price': TEST_PRICE,
            'created_at': test_created,
        }

        NEW_PRICE = 2.0
        obi._get_market_price = MagicMock(return_value=NEW_PRICE)

        cancel_order = MagicMock()
        obi._cancel_order = cancel_order

        success = obi._update_pending_order(OBIStrategy.BUY_SIGNAL)

        self.assertEqual(cancel_order.called, 1)
        self.assertTrue(success)

    def test__place_order_with_current_order(self):
        """
        Test :meth:`OBIStrategy._place_order`

        Assert that no orders are placed and `False` is returned.
        """

        obi = OBIStrategy()

        obi.order = {
            'id': 1,
        }

        TEST_PRICE = 1.0
        obi._get_market_price = MagicMock(return_value=TEST_PRICE)

        obi.get_currency_balance = MagicMock()

        trader = MagicMock()
        obi.trader = trader

        TEST_SIGNAL = OBIStrategy.BUY_SIGNAL
        success = obi._place_order(TEST_SIGNAL)

        self.assertEqual(trader.buy.called, 0)
        self.assertEqual(trader.sell.called, 0)
        self.assertFalse(success)

    def test__place_order_buy_signal(self):
        """
        Test :meth:`OBIStrategy._place_order`

        Assert a buy order is placed and `True` is returned.
        """

        obi = OBIStrategy()

        TEST_PRICE = 1.0
        obi._get_market_price = MagicMock(return_value=TEST_PRICE)

        TEST_BALANCE = 1.0
        obi.get_currency_balance = MagicMock(return_value=TEST_BALANCE)

        trader = MagicMock()
        obi.trader = trader

        TEST_SIGNAL = OBIStrategy.BUY_SIGNAL
        success = obi._place_order(TEST_SIGNAL)

        self.assertEqual(trader.buy.called, 1)
        self.assertEqual(trader.sell.called, 0)
        self.assertTrue(success)

    def test__place_order_sell_signal(self):
        """
        Test :meth:`OBIStrategy._place_order`

        Assert a sell order is placed and `True` is returned.
        """

        obi = OBIStrategy()

        TEST_PRICE = 1.0
        obi._get_market_price = MagicMock(return_value=TEST_PRICE)

        TEST_BALANCE = 1.0
        obi.get_currency_balance = MagicMock(return_value=TEST_BALANCE)

        trader = MagicMock()
        obi.trader = trader

        TEST_SIGNAL = OBIStrategy.SELL_SIGNAL
        success = obi._place_order(TEST_SIGNAL)

        self.assertEqual(trader.buy.called, 0)
        self.assertEqual(trader.sell.called, 1)
        self.assertTrue(success)

    def test__place_order_with_no_signal(self):
        """
        Test :meth:`OBIStrategy._place_order`

        Assert that no orders are placed and `False` is returned.
        """

        obi = OBIStrategy()

        TEST_PRICE = 1.0
        obi._get_market_price = MagicMock(return_value=TEST_PRICE)

        obi.get_currency_balance = MagicMock()

        trader = MagicMock()
        obi.trader = trader

        TEST_SIGNAL = None
        success = obi._place_order(TEST_SIGNAL)

        self.assertEqual(trader.buy.called, 0)
        self.assertEqual(trader.sell.called, 0)
        self.assertFalse(success)
