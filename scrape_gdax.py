#!/usr/bin/env python3

import csv
import datetime
import logging
import time

import gdax

logger = logging.getLogger('scrape_gdax')

# 60 second price intervals
GRANULARITY = 60

# Maximum results per page
MAX_RESULTS = 200

# Number of pages to retrieve
PAGES = 100

# GDAX rate limit of 3 requests per second with a little extra padding
RATE_LIMIT = 1.0 / 3.0 + 0.5


def get_history(date, product, granularity=GRANULARITY, pages=PAGES):
    """
    Get GDAX price history

    :param date: datetime object to start from
    :param product: the GDAX product to get the history of
    :param granularity: granularity of history in seconds
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


def write_history_csv(filename, history):
    """
    Write GDAX price history to CSV file

    :param history: GDAX price history
    """

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        for bar in history:
            # Convert timestamp to ISO date for use with backtrader
            try:
                date = datetime.datetime.fromtimestamp(bar[0]).isoformat(' ')
            except TypeError as error:
                logger.warning('Skipping row: {}: {}'.format(error, bar))
                continue

            bar[0] = date

            writer.writerow(bar)


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    import sys
    import os
    import argparse

    DESCRIPTION = 'Scrape GDAX data and output to a CSV.'
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    PRODUCT_ARG = 'product'
    PRODUCT_HELP = 'GDAX product to scrape data for. E.g. BTC-USD.'
    parser.add_argument(PRODUCT_ARG, help=PRODUCT_HELP)

    PAGES_ARG = 'pages'
    PAGES_HELP = ('The number of pages of data to scrape.'
            ' Each page has a maximum of {:d} results.').format(MAX_RESULTS)
    parser.add_argument(PAGES_ARG, type=int, help=PAGES_HELP)

    GRANULARITY_ARG = 'g'
    GRANULARITY_META = 'granularity'
    GRANULARITY_DEFAULT = GRANULARITY
    GRANULARITY_HELP = ('Granularity of data in seconds.'
            ' Default is {:d}.').format(GRANULARITY_DEFAULT)
    parser.add_argument('-{}'.format(GRANULARITY_ARG),
            metavar=GRANULARITY_META, type=int, default=GRANULARITY_DEFAULT,
            help=GRANULARITY_HELP)

    STARTDATE_ARG = 's'
    STARTDATE_META = 'start-date'
    STARTDATE_HELP = ('Date and time to start scraping backwards from.'
            ' Uses the ISO 8601 format (E.g. 2017-07-14T10:19:32).'
            ' Default is the current date and time.')
    parser.add_argument('-{}'.format(STARTDATE_ARG), metavar=STARTDATE_META,
            help=STARTDATE_HELP)

    args = parser.parse_args()

    product = getattr(args, PRODUCT_ARG)
    pages = getattr(args, PAGES_ARG)
    granularity = getattr(args, GRANULARITY_ARG)
    start_date = getattr(args, STARTDATE_ARG)

    # Convert ISO 8601 datetime to datetime object
    try:
        start_utc = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        print('Invalid datetime format: {}'.format(start_date))
        exit()
    except TypeError:
        # Use current time if no datetime was supplied
        start_utc = datetime.datetime.now(datetime.timezone.utc)

    is_tzinfo = start_utc.tzinfo is None
    is_utoffset = is_tzinfo or start_utc.tzinfo.utoffset(start_utc) is None

    # Set timezone if no timezone was supplied
    if is_utoffset:
        start_utc.replace(tzinfo=datetime.timezone.utc)

    # Round start date based on chosen granularity
    start_timestamp = start_utc.timestamp()
    time_delta = start_timestamp % granularity
    start_timestamp -= time_delta
    rounded_start_utc = datetime.datetime.fromtimestamp(start_timestamp)

    # Get history and write to CSV
    filename = 'gdax_history_{}_{}.csv'.format(product, granularity)
    history = get_history(rounded_start_utc, product, granularity, pages)

    write_history_csv(filename, history)
