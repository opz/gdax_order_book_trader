from requests.exceptions import ConnectionError
import unittest
from unittest.mock import patch, MagicMock

from gdax_trader import GDAXTrader


class GDAXTraderTestCase(unittest.TestCase):
    """
    Test :class:`GDAXTrader`

    Methods:
        - :meth:`GDAXTrader._run_iteration`
        - :meth:`GDAXTrader._get_client`
    """

    @patch('gdax_trader.GDAXTrader._get_client')
    def test__run_iteration_success(self, client):
        """
        Test :meth:`GDAXTrader._run_iteration`

        Assert order book data is retrieved, every strategy is iterated,
        and `True` is returned.
        """

        trader = GDAXTrader()

        strategy = MagicMock()
        trader.add_strategy(strategy)

        trader._get_accounts = MagicMock()
        trader._get_order_book = MagicMock()
        trader._get_orders = MagicMock()

        result = trader._run_iteration()

        self.assertEqual(trader._get_order_book.call_count, 1)
        self.assertEqual(strategy.next.call_count, 1)
        self.assertTrue(result)

    @patch('gdax_trader.GDAXTrader._get_client')
    def test__run_iteration_with_account_error(self, client):
        """
        Test :meth:`GDAXTrader._run_iteration`

        Assert method returns `False` and that strategies are not updated.
        """

        trader = GDAXTrader()

        strategy = MagicMock()
        trader.add_strategy(strategy)

        trader._get_accounts = MagicMock(side_effect=ConnectionError)
        trader._get_order_book = MagicMock()
        trader._get_orders = MagicMock()

        result = trader._run_iteration()

        self.assertEqual(trader._get_order_book.call_count, 0)
        self.assertEqual(strategy.next.call_count, 0)
        self.assertFalse(result)

    @patch('gdax_trader.GDAXTrader._get_client')
    def test__run_iteration_with_order_book_error(self, client):
        """
        Test :meth:`GDAXTrader._run_iteration`

        Assert method returns `False` and that strategies are not updated.
        """

        trader = GDAXTrader()

        strategy = MagicMock()
        trader.add_strategy(strategy)

        trader._get_accounts = MagicMock()
        trader._get_order_book = MagicMock(side_effect=ConnectionError)
        trader._get_orders = MagicMock()

        result = trader._run_iteration()

        self.assertEqual(strategy.next.call_count, 0)
        self.assertFalse(result)

    def test__get_client_with_env_and_api_url(self):
        """
        Test :meth:`GDAXTrader._get_client`

        Assert a client is created with a key, secret, passphrase, and API url.
        """

        environ = {
            GDAXTrader.GDAX_KEY_ENV: '1',
            GDAXTrader.GDAX_SECRET_ENV: '1',
            GDAXTrader.GDAX_PASSPHRASE_ENV: '1',
            GDAXTrader.GDAX_API_URL_ENV: '1',
        }

        with patch.dict('os.environ', environ, True):
            with patch('gdax.AuthenticatedClient') as gdax:
                client = GDAXTrader._get_client()

                gdax.assert_called_with(environ[GDAXTrader.GDAX_KEY_ENV],
                        environ[GDAXTrader.GDAX_KEY_ENV],
                        environ[GDAXTrader.GDAX_PASSPHRASE_ENV],
                        api_url=environ[GDAXTrader.GDAX_API_URL_ENV])

    def test__get_client_with_env(self):
        """
        Test :meth:`GDAXTrader._get_client`

        Assert a client is created with a key, secret, and passphrase.
        """

        environ = {
            GDAXTrader.GDAX_KEY_ENV: '1',
            GDAXTrader.GDAX_SECRET_ENV: '1',
            GDAXTrader.GDAX_PASSPHRASE_ENV: '1',
        }

        with patch.dict('os.environ', environ, True):
            with patch('gdax.AuthenticatedClient') as gdax:
                client = GDAXTrader._get_client()

                gdax.assert_called_with(environ[GDAXTrader.GDAX_KEY_ENV],
                        environ[GDAXTrader.GDAX_KEY_ENV],
                        environ[GDAXTrader.GDAX_PASSPHRASE_ENV])

    def test__get_client_without_env(self):
        """
        Test :meth:`GDAXTrader._get_client`

        Assert an exception is raised that indicates missing environment
        variables.
        """

        with patch.dict('os.environ', {}, True), self.assertRaises(KeyError):
            client = GDAXTrader._get_client()
