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
        self._transactions = None

        try:
            self._transactions = pd.read_csv(transactions_db)

            if self._transactions.columns.values.tolist() != transactions_header:
                raise CorruptedTransctionsDB("Transactions DB is corrupted")

        except FileNotFoundError:
            self._transactions = pd.DataFrame(columns=transactions_header)

        except CorruptedTransctionsDB as e:
            print(str(e))
