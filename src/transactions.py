import pandas as pd

from config import transactions_db
from config import transactions_header


class CorruptedTransctionsDB(Exception):
    pass


class Transactions:
    '''
    Provides an interface to manage the transactions.

    When instantiated, it loads the transactions history database into a Pandas
    DataFrame. The dataframe is ordered by date/time.

    Headers of Transaction DataFrame:

    - time: date and time in UTC
    - type:
        - in
        - out
        - fee
    - src: Source of the transaction (bank account, card, manually added, etc.)
    - desc: Description
    - sum: Amount of the transaction
    - cur: Currency abbreviation
    - id: Unique identifier for the transaction
    - ref: When a transaction relates to another transaction, this cell will have
           the other transaction's ID
    - category: Category name
    - tags: List of tags
    '''

    def __init__(self) -> None:
        '''
        Loads the transactions database into a Pandas DataFrame, it the database
        exists. If not, it creates an empty DF with the set of columns defined in
        config.py.
        '''
        self._df = None

        try:
            self._df = pd.read_csv(transactions_db, sep='|')

            if self._df.columns.values.tolist() != transactions_header:
                raise CorruptedTransctionsDB(
                    "Exception: Transactions DB is corrupted. \n"
                    "\n"
                    "  Expected headers:\n"
                    "  {}\n"
                    "\n"
                    "  Existing headers:\n"
                    "  {}\n"
                    "\n".format(transactions_header,
                                self._df.columns.values.tolist()))

        except FileNotFoundError:
            self._df = pd.DataFrame(columns=transactions_header)

        except CorruptedTransctionsDB as e:
            print(str(e))

    def add(self, transaction: list) -> None:
        pass

    def add_bulk(self, df: pd.DataFrame) -> None:
        pass

    def save(self) -> None:
        self._df.to_csv(transactions_db, sep='|', index=False)
