import pandas as pd

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

    def import_column(self, src_stmt_col, dst_df_col) -> None:

        if src_stmt_col not in self.get_stmt_columns():
            raise StatementException(
                'The statement has no column named "{}"'.format(src_stmt_col))
        elif dst_df_col not in self.get_df_columns():
            raise StatementException(
                'The transactions table (DF) has no column named "{}"'.format(dst_df_col))

        self._df[dst_df_col] = self._stmt[src_stmt_col]

    def fill_up_column(self, dst_df_col: str, value: str) -> None:
        self._df[dst_df_col] = value

    def fill_up_total_column(self) -> None:
        # Convert all the fee entries to negative, if they are positive
        self._df.loc[self._df['fee'] > 0, 'fee'] = self._df['fee'] * -1
        self._df['total'] = self._df['amount'] + self._df['fee']
