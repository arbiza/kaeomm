import pandas as pd

from config import Config


class CorruptedTransctionsDB(Exception):
    pass


class Transactions:
    '''
    Provides an interface to manage the transactions.

    When instantiated, it loads the transactions history database into a Pandas
    DataFrame. The dataframe is ordered by date/time.
    '''

    def __init__(self, cfg: Config) -> None:
        '''
        Loads the transactions database into a Pandas DataFrame, if the database
        exists. If not, it creates an empty DF with the set of columns defined in
        this class "header()" static method.
        '''
        self._df = None

        # 'Config' is a Singleton class. self._cfg attributes' values will update
        # if the Config object is modified anywhere else. This is specially
        # important to keep the path to the files always actual.
        self._cfg = cfg

        try:
            self._df = pd.read_csv(self._cfg.transactions_db_path, sep='|')

            if self._df.columns.values.tolist() != self.headers():
                raise CorruptedTransctionsDB(
                    "Exception: Transactions DB is corrupted. \n"
                    "\n"
                    "  Expected headers:\n"
                    "  {}\n"
                    "\n"
                    "  Existing headers:\n"
                    "  {}\n"
                    "\n".format(self.headers(),
                                self._df.columns.values.tolist()))

        except FileNotFoundError:
            self._df = pd.DataFrame(columns=self.headers())

        except CorruptedTransctionsDB as e:
            print(str(e))

    def add(self, transaction: list) -> None:
        pass

    def add_bulk(self, df: pd.DataFrame) -> None:
        pass

    @staticmethod
    def headers() -> list:
        '''Returns the Transactions DataFrame headers'''
        return ['time',
                'type',
                'source',
                'desc',
                'amount',
                'fee',
                'total',
                'curr',
                'id',
                'ref',
                'notes',
                'category',
                'tags'
                ]

    def save(self) -> None:
        self._df.to_csv(self._cfg.transactions_db_path, sep='|', index=False)
