import pandas as pd

from config import Config
import utils


class TransactionsException(Exception):
    pass


class Transactions:
    '''
    Provides an interface to manage the transactions.

    When instantiated, it loads the transactions history database into a Pandas
    DataFrame. The dataframe is ordered by date/time.
    '''

    def __new__(cls, cfg: Config):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Transactions, cls).__new__(cls)
        return cls.instance

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
            self._df = pd.read_csv(
                self._cfg.db_dir + 'transactions.csv', sep='|')

            if self._df.columns.values.tolist() != self.headers():
                raise TransactionsException(
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

        except TransactionsException as e:
            print(str(e))

    @property
    def df(self):
        raise TransactionsException(
            'Transactions DF can\'t be accessed out of the class.')

    @df.setter
    def df(self, value):
        raise TransactionsException(
            'Transactions DF can\'t be directly modified.')

    def add(self, transaction: list) -> None:
        pass

    def add_bulk(self, new_dfs: list) -> None:

        for new_df in new_dfs:
            self._df = new_df if self._df.empty else pd.concat(
                [self._df, new_df], ignore_index=True)
        self._sort()

    def backup(self) -> None:
        '''
        Saves the current transactions database to a file named with a timestamp
        '''
        self._df.to_csv(self._cfg.db_dir + 'transactions_' +
                        utils.datetime_for_filename() + '.csv',
                        sep='|',
                        index=False)

    def df_details(self) -> None:
        print(self._df.info())

    @staticmethod
    def headers() -> list:
        '''Returns the Transactions DataFrame headers'''
        return ['time',
                'type',
                'source',
                'source_id',
                'desc',
                'amount',
                'fee',
                'total',
                'curr',
                'note',
                'category',
                'tags'
                ]

    def print_to_cli(self, columns: list = [], n_rows: int = 10) -> None:
        '''
        Print the number of rows in Transactions DataFrame defined in n_rows
        Print all columns
        '''
        with pd.option_context('display.min_rows', n_rows, 'display.max_rows', n_rows):

            if len(columns) == 0:
                print(self._df)
            else:
                print(self._df[columns])

    def reset(self) -> None:
        '''
        Backup the current database, then clean it up.
        '''
        self.backup()
        self._df = pd.DataFrame(columns=self.headers())

    def save(self) -> None:
        self._df.to_csv(self._cfg.db_dir + 'transactions.csv',
                        sep='|', index=False)

    def search_n_add_category_tag(self, col: str, key: str,
                                  category: str = None,
                                  tags: list = []) -> None:
        self._cfg.add_new_category(category)
        [self._cfg.add_new_tag(tag) for tag in tags]

        if category is not None and len(tags) == 0:
            self._df.loc[self._df[col].str.contains(
                key), 'category'] = category

        elif category is None and len(tags) > 0:
            self._df.loc[self._df[col].str.contains(
                key), 'tags'] = tags

        elif category is not None and len(tags) > 0:
            self._df.loc[self._df[col].str.contains(
                key), ['category', 'tags']] = [category, tags]

    def _sort(self) -> None:
        self._df.sort_values(by=['time'], inplace=True)
        self._df.reset_index(inplace=True, drop=True)
