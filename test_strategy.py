import unittest

from strategy import Strategy

class StrategyTestCase(unittest.TestCase):
    """
    Test :class:`Strategy`

    Methods:
        - :meth:`Strategy.get_currency_balance`
    """

    def test_get_currency_balance_success(self):
        """
        Test :meth:`Strategy.get_currency_balance`

        Assert the correct currency balance is returned.
        """

        strategy = Strategy()

        TEST_CURRENCY = 'USD'
        TEST_BALANCE = 1.0

        strategy.accounts = [
            {
                'currency': TEST_CURRENCY,
                'balance': TEST_BALANCE,
            },
        ]

        balance = strategy.get_currency_balance(TEST_CURRENCY)

        self.assertEqual(TEST_BALANCE, balance)

    def test_get_currency_balance_with_invalid_currency(self):
        """
        Test :meth:`Strategy.get_currency_balance`

        Assert `None` is returned.
        """

        strategy = Strategy()

        TEST_CURRENCY = 'USD'
        TEST_BALANCE = 1.0

        strategy.accounts = [
            {
                'currency': TEST_CURRENCY,
                'balance': TEST_BALANCE,
            },
        ]

        FAKE_CURRENCY = 'FAK'

        balance = strategy.get_currency_balance(FAKE_CURRENCY)

        self.assertEqual(None, balance)

    def test_get_currency_balance_with_error(self):
        """
        Test :meth:`Strategy.get_currency_balance`

        Assert the account that throws the `KeyError` is skipped and the
        correct balance is returned.
        """

        strategy = Strategy()

        TEST_CURRENCY = 'USD'
        TEST_BALANCE = 1.0

        strategy.accounts = [
            { 'error': 'Error retrieving account.', },
            {
                'currency': TEST_CURRENCY,
                'balance': TEST_BALANCE,
            },
        ]

        balance = strategy.get_currency_balance(TEST_CURRENCY)

        self.assertEqual(TEST_BALANCE, balance)
