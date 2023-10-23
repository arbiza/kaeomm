import pandas as pd
from pandas.api.types import is_numeric_dtype

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

    def get_transactions_with_amount(self, amount) -> pd.DataFrame:
        return self._df.loc[self._df['amount'] == amount]

    def get_transactions_with_category(self, category) -> pd.DataFrame:
        tmp = self._df.loc[~self._df['category'].isna()]
        return tmp.loc[tmp['category'] == category]

    def get_transactions_without_category(self) -> pd.DataFrame:
        return self._df.loc[self._df['category'].isna()]

    def get_transactions_with_description(self, description) -> pd.DataFrame:
        return self._df.loc[self._df['desc'].str.contains(description)]

    def get_transactions_with_tag(self, tag) -> pd.DataFrame:
        tmp = self._df.loc[~self._df['tags'].isna()]
        return tmp.loc[tmp['tags'].str.contains(tag)]

    def get_transactions_without_tags(self) -> pd.DataFrame:
        return self._df.loc[self._df['tags'].isna()]

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

    @staticmethod
    def _update_tags(row, column, numeric_col, key, tags=[], overwrite=False) -> str:
        '''
        Checkes for matches in the column informed and updates the tags.

        When the column with the values to be evaluated is numeric, the key has
        to be the same; when it's a string, it will look for a substring.

        It may overwrite the tags, or append a new one.
        This is a static method firstly designed to be used as an auxiliary 
        function for Pandas apply.

        Parameters
        ----------
            column : str
                name of the column where the program will search for the 'key'
            numeric_col : bool
                indicates whether the column where the search will be performed
                is numeric or string.
            key : str or number
                text or number the program will search for
            tags : list
                list of tags to apply to the transaction (default: [])
            overwrite : bool
                when False, the tags will be added to the existing transaction's
                tag list; when True, it overwrites with the new values. (defaul
                False)
        '''

        if (numeric_col and key == row[column]) or (not numeric_col and key in row[column]):
            if overwrite is True or pd.isna(row['tags']):
                return ','.join(tags)
            else:
                return ','.join(row['tags'].split(',') + [t for t in tags if t not in row['tags']]
                                )
        else:
            return row['tags']

    def update_transactions_category(self, col, key, category=str()) -> None:

        category = self._cfg.add_new_category(category)

        if is_numeric_dtype(self._df[col].dtype):
            self._df['category'] = self._df.apply(
                lambda s: category if key == s[col] else s['category'], axis=1)
        else:
            self._df['category'] = self._df.apply(
                lambda s: category if key in s[col] else s['category'], axis=1)

    def update_transactions_tags(self, col, key, tags=[], overwrite=True) -> None:

        if not isinstance(tags, list):
            raise TransactionsException(
                'The method \'update_transactions_tags\' expects \'tags\' as a list, it received a {}'.format(type(tags)))

        tags = [self._cfg.add_new_tag(tag) for tag in tags]

        self._df['tags'] = self._df.apply(self._update_tags, args=(
            col, is_numeric_dtype(self._df[col].dtype), key, tags, overwrite), axis=1)

    # def bulk_update_category_tags(self, col, key, category=None, tags=[], overwrite=False) -> None:
    #     '''
    #     This method searchs for rows with a given value in a given column and
    #     updates the category and/or the tag.

    #     Parameters
    #     ----------
    #         col : str
    #             name of the column where the program will search for the 'key'
    #         key : str or number
    #             text or number the program will search for
    #         category : str
    #             category to apply to the transaction (default: None)
    #         tags : list
    #             list of tags to apply to the transaction (default: [])
    #         overwrite : bool
    #             when False, the tags will be added to the existing transaction's
    #             tag list; when True, it overwrites with the new values. (defaul
    #             False)
    #     '''

    #     is_numeric_column = is_numeric_dtype(self._df[col].dtype)

    #     if category is not None:
    #         category = self._cfg.add_new_category(category)

    #         if is_numeric_column:
    #             self._df['category'] = self._df.apply(
    #                 lambda s: category if key == s[col] else s['category'], axis=1)
    #         else:
    #             self._df['category'] = self._df.apply(
    #                 lambda s: category if key in s[col] else s['category'], axis=1)

    #     if len(tags) > 0:
    #         tags = [self._cfg.add_new_tag(tag) for tag in tags]

    #         self._df['tags'] = self._df.apply(self._update_tags, args=(
    #             col, is_numeric_column, key, tags, overwrite), axis=1)

    def _sort(self) -> None:
        self._df.sort_values(by=['time'], inplace=True)
        self._df.reset_index(inplace=True, drop=True)
