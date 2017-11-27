from requests.exceptions import ConnectionError
import unittest
from unittest.mock import patch, MagicMock

from utils import connection_retry


class UtilsTestCase(unittest.TestCase):
    """
    Test utils module

    Decorators:
        - :func:`connection_retry`
    """

    @patch('time.sleep')
    def test_connection_retry_success(self, sleep):
        """
        Test :func:`connection_retry`

        Assert the decorated function is successfully executed once and its
        return value is retrieved.
        """

        test_results = 'test'
        function = MagicMock(return_value=test_results)

        MAX_RETRIES = 5
        RATE_LIMIT = 1

        decorated_function = connection_retry(MAX_RETRIES, RATE_LIMIT)(function)

        results = decorated_function()

        self.assertEqual(function.call_count, 1)

        self.assertEqual(test_results, results)

    @patch('time.sleep')
    def test_connection_retry_with_retry(self, sleep):
        """
        Test :func:`connection_retry`

        Assert the `ConnectionError` is logged.
        Assert the decorated function is called twice and the return value on
        the second call is retrieved.
        """

        test_results = 'test'
        function = MagicMock(side_effect=[ConnectionError, test_results])

        MAX_RETRIES = 5
        RATE_LIMIT = 1

        decorated_function = connection_retry(MAX_RETRIES, RATE_LIMIT)(function)

        with self.assertLogs(level='WARNING'):
            results = decorated_function()

            self.assertEqual(test_results, results)

        self.assertEqual(function.call_count, 2)

    @patch('time.sleep')
    def test_connection_retry_exceed_retry(self, sleep):
        """
        Test :func:`connection_retry`

        Assert the maximum number of retries is attempted and that a
        `ConnectionError` is raised.
        """

        MAX_RETRIES = 5

        function = MagicMock(side_effect=[ConnectionError] * MAX_RETRIES)

        RATE_LIMIT = 1

        decorated_function = connection_retry(MAX_RETRIES, RATE_LIMIT)(function)

        with self.assertRaises(ConnectionError):
            decorated_function()

        self.assertEqual(function.call_count, MAX_RETRIES)
