import pandas as pd
from pandas.api.types import is_numeric_dtype
import re

from config import Config
from sources import Sources, Source
import utils


class TransactionsException(Exception):
    pass


class Transactions:
    '''
    Provides an interface to manage the transactions.

    When instantiated, it loads the transactions history database into a Pandas
    DataFrame. The dataframe is ordered by date/time.
    '''

    def __new__(cls, cfg: Config, sources: Sources):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Transactions, cls).__new__(cls)
        return cls.instance

    def __init__(self, cfg: Config, sources: Sources) -> None:
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

        # 'Sources' is a Singleton class too.
        self._sources = sources

        try:
            self._df = pd.read_csv(
                self._cfg.db_dir + 'transactions.csv', sep='|')

            if self._df.columns.values.tolist() != Config.headers():
                raise TransactionsException(
                    "Exception: Transactions DB is corrupted. \n"
                    "\n"
                    "  Expected headers:\n"
                    "  {}\n"
                    "\n"
                    "  Existing headers:\n"
                    "  {}\n"
                    "\n".format(Config.headers(),
                                self._df.columns.values.tolist()))

        except FileNotFoundError:
            self._df = pd.DataFrame(columns=Config.headers())

        except TransactionsException as e:
            print(str(e))

        self._df['time'] = pd.to_datetime(self._df['time'])

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

    @staticmethod
    def _column_update(row: pd.Series, search_col: str, is_number: bool,
                       key: any, target_col: str, value: any = None,
                       overwrite: bool = True) -> any:

        if pd.isna(row[search_col]) or (not is_number and row[search_col] == ''):
            return row[target_col]

        if ((is_number and key == row[search_col]) or
                (not is_number and key.lower() in row[search_col].lower())):
            if overwrite or pd.isna(row[target_col]):
                return ','.join(value) if isinstance(value, list) else value
            else:
                # Currently, the only column with list-like values is 'tags'.
                # If it's a list, it will combine the lists into a comma-separated
                # string and return, otherwise returns 'value' as it is.
                return ','.join(value + row[target_col].split(',')) if isinstance(value, list) else value

        return row[target_col]

    def decompose(self, i: int, amount: float, fee: float, note: str,
                  category: str = None, tags: list = None) -> None:
        '''
        Adds an additional transaction with some of the values from the original
        one (at index 'i'). More entries will represent one transaction, but 
        total amount will be split among them.

        It's useful, for example, to decompose a supermarket transaction into 
        different categories (food, alcohol, cleaning), or to separate the 
        amount and the fee with different categories or tags.


        Parameters
        ----------
        i : int
            index of the transaction to be extended
        amount : float
            expense amount - it has to be smaller or equal to original amount
        fee : float
            fee amount - it has to be smaller or equal to original amount
        note : str
            a note, if wanted
        category : str, optional, default=None
            category for the extended transaction. If omitted, it will have the 
            same as the original transaction; if '', it will be empty
        tags : list, optional, default=None
            list with tags for the extended to the transaction. If omitted, it 
            will have the same as the original transaction; if [], it will be 
            empty

        Returns
        -------
        None
        '''

        # There are several restrictions for this operation. Since it decompose
        # a transaction, the amounts have to mean fit into the combined amount
        # and can't be both zero.
        if amount == 0 and fee == 0:
            raise TransactionsException(
                '"amount" and "fee" can\'t both be zero')

        # If it's an expense
        if amount != 0 and self._df.loc[i]['total'] < 0:

            if amount > 0:
                amount *= -1
            if fee > 0:
                fee *= -1

            # Amount + fee can't exceed the total
            if (amount + fee) < self._df.loc[i]['total']:
                raise TransactionsException(
                    '"amount" and "fee" combined cannot exceed the total amount of the original transaction ({})'.format(
                        self._df.loc[i]['total'])
                )

            # Amount and fee have to be smaller than the original values
            if amount < self._df.loc[i]['amount'] or fee < self._df.loc[i]['fee']:
                raise TransactionsException(
                    '"amount" and "fee" have to be smaller than the original value ({} and {})'.format(
                        self._df.loc[i]['amount'], self._df.loc[i]['fee'])
                )

        # If it's an income
        elif fee != 0 and self._df.loc[i]['total'] > 0:
            if amount < 0:
                amount *= -1
            if fee < 0:
                fee *= -1

            # Amount + fee can't exceed the total
            if (amount + fee) > self._df.loc[i]['total']:
                raise TransactionsException(
                    '"amount" and "fee" combined cannot exceed the total amount of the original transaction ({})'.format(
                        self._df.loc[i]['total'])
                )

            # Amount and fee have to be smaller than the original values
            if amount > self._df.loc[i]['amount'] or fee > self._df.loc[i]['fee']:
                raise TransactionsException(
                    '"amount" and "fee" have to be smaller than the original value ({} and {})'.format(
                        self._df.loc[i]['amount'], self._df.loc[i]['fee']))

        category = self._df.loc[i]['category'] if category is None else self._cfg.add_new_category(
            category)

        if tags is None:
            tags = self._df.loc[i]['tags']
        elif isinstance(tags, list):
            tags = ','.join([self._cfg.add_new_tag(tag) for tag in tags])
        else:
            raise TransactionsException(
                'Tags has to be a list or omitted (None)\n',
                'The object receives is of type "{}"'.format(tags))

        # Update the original transaction
        self._df.loc[i, 'amount'] = self._df.loc[i]['amount'] - float(amount)
        self._df.loc[i, 'fee'] = self._df.loc[i]['fee'] - fee
        self._df.loc[i, 'total'] = self._df.loc[i]['amount'] + \
            self._df.loc[i]['fee']

        # Add the new transaction
        self.add_bulk(
            [pd.DataFrame(
                [[
                    self._df.loc[i]['time'],
                    'decompose',
                    self._df.loc[i]['source'],
                    self._df.loc[i]['source_id'],
                    self._df.loc[i]['desc'],
                    float(amount),
                    float(fee),
                    float(amount + fee),
                    self._df.loc[i]['curr'],
                    note,
                    '',
                    category,
                    tags
                ]],
                columns=Config.headers()
            )
            ])

    def df_info(self) -> str:

        return (
            "TRANSACTIONS DF DETAILS\n\n"
            "DTYPES\n\n\n"
            f"{self._df.dtypes}"
            "\n\n\nDESCRIBE\n"
            f"{self._df.describe()}"
        )

    def extend(self, i, source, amount, fee, note, category=None, tags=None):
        '''
        Adds an additional transaction with some of the values from the original
        one (at index 'i'). More entries will represent one transaction and the
        sum of them will represent the real amount expend or received.

        It's useful, for example, to register a tip at a restaurant; this 
        transaction will have the same date/time and description, but may have 
        all other values different.

        Other examples:
          - salary discounts
          - partial refunds with different sources
          - 


        Parameters
        ----------
        i : int
            index of the transaction to be extended
        source : str
            source name (from the Source class)
        amount : float
            expense amount (< 0) or income amount (> 0)
        fee : float
            fee amount
        note : str
            a note, if wanted
        category : str, optional, default=None
            category for the extended transaction. If omitted, it will have the 
            same as the original transaction; if '', it will be empty
        tags : list, optional, default=None
            list with tags for the extended to the transaction. If omitted, it 
            will have the same as the original transaction; if [], it will be 
            empty

        Returns
        -------
        None
        '''

        ext_source = None

        for s in self._sources.sources:
            if source.lower() == s.name.lower():
                ext_source = s

        if ext_source is None:
            raise TransactionsException(
                'There is no source named "{}".\n'.format(source),
                'Please, register this source first.')

        category = self._df.loc[i]['category'] if category is None else self._cfg.add_new_category(
            category)

        if tags is None:
            tags = self._df.loc[i]['tags']
        elif isinstance(tags, list):
            tags = ','.join([self._cfg.add_new_tag(tag) for tag in tags])
        else:
            raise TransactionsException(
                'Tags has to be a list or omitted (None)\n',
                'The object receives is of type "{}"'.format(tags))

        self.add_bulk(
            [pd.DataFrame(
                [[
                    self._df.loc[i]['time'],
                    'extend',
                    ext_source.name,
                    ext_source.id,
                    self._df.loc[i]['desc'],
                    float(amount),
                    float(fee),
                    float(amount + fee),
                    ext_source.currency,
                    note,
                    '',
                    category,
                    tags
                ]],
                columns=Config.headers()
            )
            ])

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
        self._df = pd.DataFrame(columns=Config.headers())

    def save(self) -> None:
        self._df.to_csv(self._cfg.db_dir + 'transactions.csv',
                        sep='|', index=False)

    def search(self,
               index: int or list = None,
               start_date: str = None,
               end_date: str = None,
               type: str = None,
               source: str = None,
               description: str = None,
               total: float = None,
               currency: str = None,
               note: str = None,
               system_category: str = None,
               categories: str or list(str) = None,
               tags: str or list(str) or int = None) -> pd.DataFrame:
        '''
        Searchs for transaction which combine all the arguments passed.

        Set the arguments you want to look for. 

        Parameters
        ----------
        index : int or list
            when "int": returns the row at the specified index
            when "list": returns the rows at the indexes in the list
            for a specific range, use index=list(range(<start>, <end+1>))
        start_date: str
            date in the format "yyyy-mm-dd"
        end_date: str
            date in the format "yyyy-mm-dd"; when omitted, the program returns
            the rows with the date in "start_date"
        type: str
            returns rows with the exact values passed in "type"
        source: str
            returns the transactions of the specified source
        description: str
            returns the transactions where the "desc" column contains the value
            passed in "description"
        total: float
            returns the transactions where the "total" is equal to the value 
            passed
        currency: str
            returns the transactions in the currency specified
        note: str
            returns the transactions where the "note" column contains the value
            passed in "note"
        system_category: str
            returns the transactions with the "system_category" specified
        categories: str or list(str)
            when "str" with some value, return the transactions with the category
            when "str" but '': returns, return transactions without category
            when "list", returns the transactions with the listed categories
        tags: str or list(str) or int
            when "str" with some value, return the transactions with the tag
            when "str" but '': returns, return transactions without tags
            when "list", returns the transactions with the listed tags
            when "int", returns the transactions with the number of tags specified
                (has to be > 0). For no tags, use an empty string ('')

        Returns
        -------
        Pandas Dataframe (None when the method was called with all arguments as None)
        '''

        s_df = self._df

        # INDEX
        if index is not None:
            if isinstance(index, int):
                s_df = s_df.loc[[index]]
            elif isinstance(index, list):
                s_df = s_df.loc[index]
            else:
                raise TransactionsException(
                    '"index" has to be an integer or a list of integers; received "{}"'.format(index))

        # DATE
        pattern = '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'

        if start_date is not None and not re.search(pattern, start_date):
            raise TransactionsException(
                'The dates must be passed as yyyy-mm-dd; {} doesn\'t match or is not a valid date.'.format(start_date))

        if end_date is not None and not re.search(pattern, end_date):
            raise TransactionsException(
                'The dates must be passed as yyyy-mm-dd; {} doesn\'t match or is not a valid date.'.format(end_date))

        if start_date is not None and end_date is None:
            s_df = s_df[(s_df['time'].dt.strftime('%Y-%m-%d') == start_date)]
        elif start_date is not None and end_date is not None:
            s_df = s_df[(s_df['time'].dt.strftime(
                '%Y-%m-%d') >= start_date) & (s_df['time'].dt.strftime('%Y-%m-%d') <= end_date)]

        # TYPE
        if type is not None:
            s_df = s_df[(s_df['type'] == type)]

        # SOURCE
        if source is not None:
            src = None
            for s in self._sources.sources:
                if s.name.lower() == source.lower():
                    src = s.id

            if src is None:
                raise TransactionsException(
                    'There is no "source" named {}.'.format(source))

            s_df = s_df[(s_df['source_id'] == src)]

        # DESCRIPTION
        if description is not None:
            description = str(description)
            s_df.dropna(subset=['desc'], inplace=True)
            s_df = s_df.loc[s_df['desc'].str.contains(description, case=False)]

        # TOTAL
        if total is not None:
            s_df = s_df.loc[s_df['total'] == total]

        # CURRENCY
        if currency is not None:
            s_df = s_df[(s_df['curr'] == currency)]

        # NOTE
        if note is not None:
            note = str(note)
            s_df = s_df.loc[s_df['note'].str.contains(note, case=False)]

        # SYSTEM CATEGORY
        if system_category is not None:
            s_df = s_df[(s_df['system_cat'] == system_category)]

        # CATEGORY
        if categories is not None:
            if isinstance(categories, str):
                if categories != '':
                    if categories.lower().capitalize() not in self._cfg.categories:
                        raise TransactionsException(
                            'There is no category named "{}"'.format(categories))
                    else:
                        s_df.dropna(subset=['category'], inplace=True)
                        s_df = s_df[s_df['category'] ==
                                    categories.lower().capitalize()]
                else:
                    s_df = s_df[s_df['category'].isna()]

            elif isinstance(categories, list):
                s_df.dropna(subset=['category'], inplace=True)

                for cat in categories:
                    if cat.lower().capitalize() not in self._cfg.categories:
                        raise TransactionsException(
                            'There is no category named "{}"'.format(cat))

                findings = [s_df.loc[s_df['category'].str.contains(
                    cat, case=False)] for cat in categories]

                s_df = pd.concat(findings)
            else:
                raise TransactionsException(
                    '"categoris" has to be a list of strings or a single string (may be empty); received "{}"'.format(categories))

        # TAGS
        if tags is not None:

            # Searching for tags in a list
            if isinstance(tags, list):
                s_df.dropna(subset=['tags'], inplace=True)

                for tag in tags:
                    if tag.lower().capitalize() not in self._cfg.tags:
                        raise TransactionsException(
                            'There is no tag named "{}"'.format(tag))

                findings = [s_df.loc[s_df['tags'].str.contains(
                    t, case=False)] for t in tags]

                s_df = pd.concat(findings)

            # Searching for a single tag
            elif isinstance(tags, str) and tags != '':
                s_df.dropna(subset=['tags'], inplace=True)
                s_df = s_df.loc[s_df['tags'].str.contains(tags, case=False)]

            # Searching for transactions not tagged
            elif isinstance(tags, str) and tags == '':
                s_df = s_df.loc[s_df['tags'].isna()]

            # Searching for transactions with a specific number of tags
            elif isinstance(tags, int) and tags > 0:
                s_df.dropna(subset=['tags'], inplace=True)
                s_df = s_df.loc[s_df['tags'].str.count(',') == tags - 1]

            else:
                raise TransactionsException(
                    '"tags" has to be a list of strings, a single string, or a positive integer; received "{}"'.format(tags))

        return s_df if s_df is not self._df else None

    def _sort(self) -> None:
        self._df.sort_values(by=['time'], inplace=True)
        self._df.reset_index(inplace=True, drop=True)

    def _system_category(self, i: list = [], col: str = None,
                         key: any = None, system_category: str = '') -> None:
        '''
        Adds or deletes system_cat to transactions based on a search or at a 
        specific index. It's meant to be used by the class only.

        When 'index' is set, it will update the specified rows; when not, it will
        search for 'key' in 'col'.

        Parameters
        ----------
        i : list
            list of indexes of the rows to be modified
        col : str
            name of the column to perform the search
        key : any
            value the code will search for in the column
        category : str, optional, default=''
            category to be added to the transaction. When empty, the existing 
            one will be removed.

        Returns
        -------
        None
        '''

        if not isinstance(i, list):
            raise TransactionsException(
                'The method \'_system_category\' expects \'i\' as a list, it received a {}'.format(type(i)))

        system_category = system_category.lower()

        if system_category != '' and system_category not in self._cfg.system_categories:
            raise TransactionsException(
                'There is no system category named as \'{}\' and it can\'t be defined'.format(system_category))

        # Set the system category at specified index
        if len(i) > 0:
            self._df['system_cat'][i] = system_category

        # Set the system category when the search for 'key' matches in 'col'
        elif col is not None and key is not None:
            self._df['system_cat'] = self._df.apply(
                self._column_update, args=[col, is_numeric_dtype(self._df[col]), key, 'system_cat', system_category], axis=1)

        else:
            raise TransactionsException(
                'Either \'i\' must be set or \'col\' and \'key\'.\n'
                'Parameters received are: i: {}, col: {}, key: {}'.format(i, col, key))

    def update_transaction_category_at_index(self, i, category=str()):
        '''
        Adds or deletes transactions category based on a search.

        If 'category' is empty, it will remove the existing category in the 
        transaction.

        Parameters
        ----------
        i : list
            list of indexes of the rows to be modified
        category : str, optional, default=''
            category to be added to the transaction. When empty, the existing 
            one will be removed.

        Returns
        -------
        None
        '''

        if not isinstance(i, list):
            raise TransactionsException(
                'The method \'update_transaction_category_at_index\' expects \'i\' as a list, it received a {}'.format(type(i)))

        category = self._cfg.add_new_category(category)

        self._df['category'][i] = category

    def update_transaction_tags_at_index(self, i, tags=[], overwrite=True):
        '''
        Adds or deletes transactions category based on a search.

        If 'category' is empty, it will remove the existing category in the 
        transaction.

        Parameters
        ----------
        i : list
            list of indexes of the rows to be modified
        tags : list, optional, default=[]
            list with categories to be added to the transaction. If empty and
            overwrite is True, the existing tags will be removed.
        overwrite : book, optional, default=True
            when False, the tags will be added to the existing transaction's
            tag list; when True, it overwrites with the new values.

        Returns
        -------
        None
        '''

        if not isinstance(i, list) or not isinstance(tags, list):
            raise TransactionsException(
                'The method \'update_transaction_tags_at_index\' expects two lists, it received {} and {}'.format(type(i), type(tags)))

        tags = [self._cfg.add_new_tag(tag) for tag in tags]

        for p in i:
            if overwrite or pd.isna(self._df['tags'][p]):
                self._df['tags'][p] = ','.join(tags)
            else:
                self._df['tags'][p] = ','.join(self._df['tags'][p].split(
                    ',') + [t for t in tags if t not in self._df['tags'][p]])

    def update_transactions_category(self, col, key, category=str()):
        '''
        Adds or deletes transactions category based on a search.

        If 'category' is empty, it will remove the existing category in the 
        transaction.

        Parameters
        ----------
        col : str
            name of the column to perform the search
        key : any
            value the code will search for in the column
        category : str, optional, default=''
            category to be added to the transaction. When empty, the existing 
            one will be removed.

        Returns
        -------
        None
        '''

        category = self._cfg.add_new_category(category)

        self._df['category'] = self._df.apply(
            self._column_update, args=[col, is_numeric_dtype(self._df[col].dtype), key, 'category', category], axis=1)

    def update_transactions_tags(self, col, key, tags=[], overwrite=True):
        '''
        Adds, appends, or deletes tags in transactions that match the search.

        If 'overwrite' is True, the new values will overwrite the existing ones,
        it includes replacing the tags with no tags.

        Parameters
        ----------
        col : str
            name of the column to perform the search
        key : any
            value the code will search for in the column
        tags : list, optional, default=[]
            list with new tags. If empty and overwrite is True, the existing tags
            will be removed
        overwrite : bool, optional, default=True
            overwrites the existing tags, when True; appends when False

        Returns
        -------
        None
        '''

        if not isinstance(tags, list):
            raise TransactionsException(
                'The method \'update_transactions_tags\' expects \'tags\' as a list, it received a {}'.format(type(tags)))

        if len(tags) == 0 and overwrite is False:
            raise TransactionsException(
                '"tags" is empty and overwrite is set "False". With this combination, the method won\'t do anything.'
            )

        tags = [self._cfg.add_new_tag(tag) for tag in tags]

        self._df['tags'] = self._df.apply(
            self._column_update, args=[col, is_numeric_dtype(self._df[col].dtype), key, 'tags', tags, overwrite], axis=1)
