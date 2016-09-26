import pandas as pd
import datetime as dt


class OptionsDataFrame:
    csv_url = 'https://drive.google.com/uc?export=download&id=0B05gj9_AyZMUTF8tY2tsMlhWdVE'
    options_df = None

    def __init__(self, underlying=None, start=dt.datetime(2000, 1, 1), end=dt.datetime.today(), download_path=''):
        """
        Options Data Frame Constructor
        :param underlying: List of tickers to filter the data frame to
        :param start: Minimum date as datetime object
        :param end: Maximum date as datetime object
        :param download_path: file path string for location to save csv. Ex. '~/Downloads/options_data.csv'
        """
        options_df = pd.read_csv(self.csv_url)

        options_df['date'] = pd.to_datetime(options_df['date'])
        options_df['expdt'] = pd.to_datetime(options_df['expdt'])
        options_df = options_df.drop('expdt2', 1)

        if underlying is not None:
            options_df = options_df[options_df['underlying'].apply(lambda u: u in underlying)]

        options_df = options_df[options_df['date'] > start]
        options_df = options_df[options_df['date'] < end]

        self.options_df = options_df

        if download_path != '':
            options_df.to_csv(download_path, index=False)

    def fetch_data(self):
        """
        Return the options data as a pandas data frame
        """
        return self.options_df

    def download_options_data(self, path):
        """
        Save the options data frame as a CSV
        :param path: file path string to store csv. Ex. '~/Downloads/options_data.csv'
        """
        self.options_df.to_csv(path, index=False)

    def filter_underlying(self, tickers):
        """
        Filter the data frame down to only include a specified list of tickers as underlyings
        :param tickers: List of tickers to filter the data frame to
        """
        self.options_df = self.options_df[self.options_df['underlying'].apply(lambda u: u in tickers)]

    def filter_date(self, start=dt.datetime(2000, 1, 1), end=dt.datetime.today()):
        """
        Filter the data frame down to only include data within a certain date range
        :param start: start date for the minimum of the date range
        :param end: end date for the maximum of the date range
        """
        self.options_df = self.options_df[self.options_df['date'] < end & self.options_df['date'] > start]

    def filter_days_to_expiry(self, days):
        """
        Filter the data frame down to only include rows where the option is some number of days away from expiry
        :param days: number of days out from expiry
        """
        self.options_df = self.options_df[(self.options_df['expdt'] - self.options_df['date']) == dt.timedelta(days)]
