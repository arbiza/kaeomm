import pandas as pd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from datetime import datetime

from transactions import Transactions


class StatementException(Exception):
    pass


class StatementsParser:

    def __init__(self, statement_path: str) -> None:
        self._df = pd.DataFrame(columns=Transactions.headers())
        self._stmt = pd.read_csv(statement_path)

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

    def get_time_format(dt: str) -> str:
        '''Returns the date/time format to parse str into datatime objects'''
        pass

    def import_column(self, src_stmt_col: list, dst_df_col: str) -> None:

        if len(src_stmt_col) == 1:
            self._df[dst_df_col] = self._stmt[src_stmt_col]

        elif len(src_stmt_col) > 1:
            # To avoid problems, the program has to identify whether the values
            # are strings or numbers. The empty cells have to be filled-up.
            if is_numeric_dtype(self._stmt[src_stmt_col[0]].dtype):
                [self._stmt[c].fillna(0, inplace=True) for c in src_stmt_col]
                self._df[dst_df_col] = self._stmt[src_stmt_col].sum(axis=1)

            elif is_string_dtype(self._stmt[src_stmt_col[0]].dtype):
                [self._stmt[c].fillna('No ' + c, inplace=True)
                 for c in src_stmt_col]
                self._df[dst_df_col] = self._stmt[src_stmt_col].agg(
                    ' - '.join, axis=1)

    def fill_up_column(self, dst_df_col: str, value: str) -> None:
        self._df[dst_df_col] = value

    def conclude(self) -> None:
        # Convert all the fee entries to negative, if they are positive; then,
        # fill up the 'total' column
        self._df['amount'].fillna(0, inplace=True)
        self._df['fee'].fillna(0, inplace=True)
        self._df.loc[self._df['fee'] > 0, 'fee'] = self._df['fee'] * -1
        self._df['total'] = self._df[['amount', 'fee']].sum(axis=1)

        # dt_format = str()

        # # Convert the time from string to datatime
        self._df['time'] = pd.to_datetime(self._df['time'])
