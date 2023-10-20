import pandas as pd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from datetime import datetime

from transactions import Transactions
from config import Config


class StatementException(Exception):
    pass


class StatementsParser:

    def __init__(self, statement_path: str, timezone: str) -> None:
        self._df = pd.DataFrame(columns=Transactions.headers())
        self._stmt = pd.read_csv(statement_path)
        self._timezone = timezone

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, value):
        raise StatementException(
            'Transactions DF can\'t be directly modified.')

    def get_stmt_columns(self) -> list:
        return self._stmt.columns.values.tolist()

    def get_df_columns(self) -> list:
        return Transactions.headers()

    def import_column(self, src_stmt_col: list, dst_df_col: str) -> None:

        if len(src_stmt_col) == 1:
            self._df[dst_df_col] = self._stmt[src_stmt_col]

        elif len(src_stmt_col) > 1:
            # To avoid problems, the program has to identify whether the values
            # are strings or numbers. The empty cells have to be filled-up.
            for col in src_stmt_col:

                # 1. check whether any of the columns is a string dtype; if any,
                # all will be handled as strings. (Pandas may consider empty
                # columns as 'float64' dtype)
                if is_string_dtype(self._stmt[col].dtype):

                    # 2. set all source columns dtype as 'object' -- it prevents
                    # a warning.
                    for c in src_stmt_col:
                        self._stmt[c] = self._stmt[c].astype(object)

                    # 3. fill up the empty rows
                    [self._stmt[c].fillna('No ' + c, inplace=True)
                     for c in src_stmt_col]

                    # 4. combine the columns
                    self._df[dst_df_col] = self._stmt[src_stmt_col].agg(
                        ' - '.join, axis=1)
                    return

            [self._stmt[c].fillna(0, inplace=True)
             for c in src_stmt_col]
            self._df[dst_df_col] = self._stmt[src_stmt_col].sum(axis=1)

    def fill_up_column(self, dst_df_col: str, value: str) -> None:
        '''
        Fillup a column with the value provided.
        '''
        self._df[dst_df_col] = value

    def conclude(self) -> None:
        '''
        Process the values in the columns after the importing.

        The processing involves amounts calculation, currency convertion, 
        datetime manipulation, etc. It
        '''
        # Convert all the fee entries to negative, if they are positive; then,
        # fill up the 'total' column
        self._df['amount'].fillna(0, inplace=True)
        self._df['fee'].fillna(0, inplace=True)
        self._df.loc[self._df['fee'] > 0, 'fee'] = self._df['fee'] * -1
        self._df['total'] = self._df[['amount', 'fee']].sum(axis=1)

        # # Convert the time from string to datatime
        self._df['time'] = pd.to_datetime(self._df['time'])

        # The time is processed as follows:
        #  - Localize (set a timezone) to the datetime object which is importated
        #    as timezone naive (required to convert)
        #  - Convert the time to the users timezone
        #  - Change the datetime object back as timezone naive for a better
        #    readability (2022-12-12 13:09:48+01:00 -> 2022-12-12 13:09:48)
        cfg = Config()
        self._df['time'] = self._df['time'].dt.tz_localize(
            self._timezone).dt.tz_convert(cfg.local_timezone).dt.tz_localize(None)
