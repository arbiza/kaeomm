import pandas as pd


class StatementsParser:

    def __init__(self, statement_path: str) -> None:
        self._stmt_df = None
        self._stmt_path = statement_path

    def read_csv(self) -> None:
        self._stmt_df = pd.read_csv(self._stmt_path)


class Revolut(StatementsParser):
    '''
    Parses Revolut statements
    '''

    def __init__(self, df: pd.DataFrame, statement_path: str) -> None:
        super().__init__(statement_path)
        super().read_csv()
        self._df = df

    def parse(self) -> pd.DataFrame:
        '''
        The CSV statement contains the following columns:

          - Type
          - Product
          - Started Date (Transaction time in UTC)
          - Completed Date
          - Description
          - Amount
          - Fee
          - Currency
          - State
          - Balance
        '''
        return self._stmt_df
