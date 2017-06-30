#!/usr/bin/env python3

import csv
import datetime
import logging
import time

import gdax

logger = logging.getLogger('scrape_gdax')

# GDAX product to query
PRODUCT = 'ETH-USD'

# 10 second price intervals
GRANULARITY = 10

# Maximum results per page
MAX_RESULTS = 200

# Number of pages to retrieve
PAGES = 100

# GDAX rate limit of 3 requests per second with a little extra padding
RATE_LIMIT = 1.0 / 3.0 + 0.5


def get_history(date, product=PRODUCT, granularity=GRANULARITY, pages=PAGES):
    """
    Get GDAX price history

    :param date: datetime object to start from
    :param pages: number of pages to retrieve, results per page vary
    :returns: GDAX price history
    :rtype: List
    """

    if pages < 1:
        return

    start = date

    history = []

    client = gdax.PublicClient()

    for i in range(pages):
        # Set new start and end dates for next page
        end = start
        end_iso = end.isoformat()

        start = end - datetime.timedelta(seconds=granularity * MAX_RESULTS)
        start_iso = start.isoformat()

        logger.info('{} to {}'.format(end, start))
        logger.info('({:d}/{:d})'.format(i + 1, pages))

        new_history = client.get_product_historic_rates(
                product, start=start_iso, end=end_iso, granularity=granularity)

        # If results are not a list, most likely an API error occurred
        try:
            history += new_history
        except TypeError as error:
            logger.warning('{}: {}'.format(error, new_history))
            continue

        logger.info('Number of new results: {:d}\n'.format(len(new_history)))

        time.sleep(RATE_LIMIT)

    return history


def write_history_csv(history):
    """
    Write GDAX price history to CSV file

    :param history: GDAX price history
    """

    csv_name = 'gdax_history_{}_{}.csv'.format(PRODUCT, GRANULARITY)

    with open(csv_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        for bar in history:
            # Convert timestamp to ISO date for use with backtrader
            date = datetime.datetime.fromtimestamp(bar[0]).isoformat(' ')
            bar[0] = date

            writer.writerow(bar)


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    import sys
    import os

    if len(sys.argv) <= 1:
        filename = os.path.basename(__file__)
        print('Usage: ./{} <Product> <Granularity> <Pages>'.format(filename))
        print('Example: ./{} ETH-USD 10 1000'.format(filename))
        exit()

    try:
        product = sys.argv[1]
    except IndexError:
        product = PRODUCT

    try:
        granularity = int(sys.argv[2])
    except (IndexError, ValueError):
        granularity = GRANULARITY

    try:
        pages = int(sys.argv[3])
    except (IndexError, ValueError):
        pages = PAGES

    # Most recent price to start from
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    history = get_history(now_utc, product, granularity, pages)

    write_history_csv(history)
