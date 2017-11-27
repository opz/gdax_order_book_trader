import logging
import time

from json.decoder import JSONDecodeError
from requests.exceptions import ConnectionError


logger = logging.getLogger(__name__)


def connection_retry(max_retries, rate_limit):
    """
    Decorator for retrying function when `ConnectionError` is raised

    :param max_retries: maximum number of function attempts
    :param rate_limit: how quickly function calls can be made
    :returns: data returned by function
    :raises ConnectionError: Error raised if every retry attempt fails
    """

    def connection_retry_decorator(function):

        def wrapper(*args, **kwargs):
            data = None
            retry = 0
            connect_success = False
            connect_error = False

            while not connect_success and retry < max_retries:
                # Exit loop if function is successful
                try:
                    data = function(*args, **kwargs)
                    connect_success = True

                # Retry on connection error
                except (ConnectionError, JSONDecodeError) as error:
                    connect_error = error
                    logger.warning(error)

                retry += 1

                time.sleep(rate_limit)

            # If all retry attempts failed, raise the exception
            if retry == max_retries and connect_error:
                raise connect_error

            return data

        return wrapper

    return connection_retry_decorator

