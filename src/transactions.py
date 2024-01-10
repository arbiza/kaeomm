import pandas as pd
import re
import json

from config import Config
from sources import Sources, Source
from utils import datetime_for_filename, StdReturn


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
        self._df['allot'] = self._df['allot'].astype('Int64')
        self._df['link'] = self._df['link'].astype('Int64')

        # Check all the categories in the dataframe and update the categories
        # list.
        [cfg.add_new_category(str(c))
         for c in self._df['category'].drop_duplicates()]

        # Check all the tags in the dataframe and update the tags list
        for line in self._df['tags'].drop_duplicates():
            [cfg.add_new_tag(str(t)) for t in str(line).split(',')]

    @property
    def df(self):
        raise TransactionsException(
            'Transactions DF can\'t be accessed out of the class.')

    @df.setter
    def df(self, value):
        raise TransactionsException(
            'Transactions DF can\'t be directly modified.')

    def add(self, time: str, timezone: str, type: str, source: Source, desc: str,
            amount: float, fee: float = 0.0, note: str = None,
            category: str = None, tags: list = []) -> StdReturn:
        '''
        Adds a new transaction.


        Parameters
        ----------
        time : str
            date or datetime - formats: yyyy-mm-dd, yyyymmdd, yyyy-mm-dd HH:mm:ss
        timezone : str
            timezone name as in pytz.all_timezones
        type : str
            type description, like "CARD", "CASH", "manual", etc.
        source : str
            source name (from the Source class)
        desc : str
            transaction description
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
        StdReturn object with the return values.
        '''

        r = StdReturn(message="Transaction successfully updated")

        df = pd.DataFrame(columns=Config.headers())

        src = None
        for s in self._sources.sources:
            if s.name.lower() == source.lower():
                src = s

        if src is None:
            r.success = False
            r.message = 'There is no "source" named {}.'.format(source)
            return r

        df.loc[0, 'id'] = int(self._df['id'].max() + 1)
        df['time'] = pd.to_datetime([time]).tz_localize(timezone).tz_convert(
            self._cfg.local_timezone).tz_localize(None)
        df['input'] = 'manual'
        df['type'] = type
        df['source'] = src.name
        df['source_id'] = src.id
        df['desc'] = desc
        df['amount'] = amount
        df['fee'] = fee
        df['total'] = amount + fee
        df['curr'] = src.currency

        if note is not None:
            df['note'] = note

        if category is not None:
            df['category'] = category

        if tags is not None:
            df['tags'] = ','.join([self._cfg.add_new_tag(tag) for tag in tags])

        self._df = pd.concat([self._df, df], ignore_index=True)
        self._sort()

        return r

    def add_bulk(self, new_dfs: list) -> StdReturn:
        '''
        Merges the existing transactions with the new one(s).

        This method adds the unique transaction identifiers and sorts the
        resulting list by date.


        Parameters
        ----------
        new_dfs : list
            list of Pandas Dataframes to be merged with the existing transactions
            list.

        Returns
        -------
        StdReturn object with the return values.
        '''

        r = StdReturn(message='Transaction DataFrames successfully combined.')

        try:
            for new_df in new_dfs:
                self._df = new_df if self._df.empty else pd.concat(
                    [self._df, new_df], ignore_index=True)
            self._sort()

            for i in range(0, len(self._df)):
                if pd.isna(self._df.loc[i, 'id']) or self._df.loc[i, 'id'] == 0:
                    self._df.loc[i, 'id'] = self._df['id'].max() + 1
        except Exception as e:
            r.success = False
            r.message = 'Issue when combining the existing transactions with the new one'
            r.details = 'Method: Transactions.add_bulk; exception: {}'.format(
                e)

        return r

    def backup(self) -> StdReturn:
        '''
        Saves the current transactions database to a file named with a timestamp
        '''

        filename = self._cfg.db_dir + 'transactions_' + datetime_for_filename() + \
            '.csv'

        r = StdReturn(message='Backup successful')
        r.details = filename

        try:
            self._df.to_csv(filename, sep='|', index=False)
        except Exception as e:
            r.success = False
            r.message = 'Transactions backup failed.'
            r.details = 'Method: Transactions.backup; exception: {}'.format(e)

        return r

    def df_info(self) -> str:

        return (
            "TRANSACTIONS DF DETAILS\n\n"
            "DTYPES\n\n\n"
            f"{self._df.dtypes}"
            "\n\n\nDESCRIBE\n"
            f"{self._df.describe()}"
        )

    def link(self, list_i: list) -> StdReturn:
        '''
        It associates different transactions which provides a thorough view of 
        the amount spent and all the transactions involved in a process. It's 
        useful, for example, to associate transactions such as the tip with the 
        payment for the meal, long multi-step bureaucratic processes, or 
        different transactions that comprise one payment or income.

        Other examples:
          - salary discounts
          - refunds with different sources or transfers
          - rent + utilities monthly payment


        Parameters
        ----------
        list_i : list
            index list of two or more transactions to be linked.

        Returns
        -------
        StdReturn object with the return values.
        '''

        r = StdReturn()

        if len(list_i) < 2:
            r.success = False
            r.message = 'Linking requires two or more transactions.'
            r.details = 'Received {} transaction'.format(len(list_i))
            return r

        df = self.search(list_i).dropna(
            subset=['link']).drop_duplicates(subset=['link'])

        if len(df) > 1:
            r.success = False
            r.message = 'Transactions not linked - they already have different links.'
            r.details = '\n' + df[['id', 'link']].to_string()
            return r

        if len(df) == 1:
            id = int(df.iloc[0]['link'])
        else:
            id = int(self._df.loc[list_i[0], 'id'])

        # Link transactions
        for i in list_i:
            self._df.loc[i, 'link'] = int(id)

        r.message = 'Transactions successfully linked'
        r.details = '\n' + self.search(list_i)[['id', 'link']].to_string()
        return r

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

        filename = self._cfg.db_dir + 'transactions_.csv'

        r = StdReturn(message='Transactions database successfully saved')
        r.details = filename

        try:
            self._df.to_csv(filename, sep='|', index=False)
        except Exception as e:
            r.success = False
            r.message = 'Transactions backup failed.'
            r.details = 'Method: Transactions.backup; exception: {}'.format(e)

        return r

    def search(self,
               index: int or list(int) = None,
               start_date: str = None,
               end_date: str = None,
               type: str = None,
               source: str = None,
               description: str = None,
               total: float = None,
               currency: str = None,
               note: str = None,
               system: str = None,
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
            returns rows with the exact values passed in "type". '*' returns any
            type
        source: str
            returns the transactions of the specified source
        description: str
            returns the transactions where the "desc" column contains the value
            passed in "description". '*' returns any description
        total: float
            returns the transactions where the "total" is equal to the value 
            passed
        currency: str
            returns the transactions in the currency specified
        note: str
            returns the transactions where the "note" column contains the value
            passed in "note". '*' returns any note
        system: str
            returns the transactions with the "system" specified.
            '*' returns any category
        categories: str or list(str)
            when "str" with some value, return the transactions with the category
            when '': returns transactions without category
            when '*': returns transaction with any category set
            when "list", returns the transactions with the listed categories
        tags: str or list(str) or int
            when "str" with some value, return the transactions with the tag
            when '': returns transactions without tags
            when '*': returns transaction with any tag set
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
            if type == '*':
                s_df = s_df.dropna(subset=['type'])
            else:
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

            # Regardless it's "*" or any other string to look for, if "description"
            # has been set, the process requires removing the empty ones.
            #
            # If it's "*", it's enought to drop the NaN;
            # if not, it will search for the string.
            s_df = s_df.dropna(subset=['desc'])

            if description != '*':
                description = str(description)
                s_df = s_df.loc[s_df['desc'].str.contains(
                    description, case=False)]

        # TOTAL
        if total is not None:
            s_df = s_df.loc[s_df['total'] == total]

        # CURRENCY
        if currency is not None:
            s_df = s_df[(s_df['curr'] == currency)]

        # NOTE
        if note is not None:

            # Regardless it's "*" or any other string to look for, if "note"
            # has been set, the process requires removing the empty ones.
            #
            # If it's "*", it's enought to drop the NaN;
            # if not, it will search for the string.
            s_df = s_df.dropna(subset=['note'])

            if note != '*':
                note = str(note)
                s_df = s_df.loc[s_df['note'].str.contains(note, case=False)]

        # SYSTEM CATEGORY
        if system is not None:
            if system == '':
                s_df = s_df[s_df['system'].isna()]
            elif system == '*':
                s_df = s_df.dropna(subset=['system'])
            else:
                s_df = s_df[(s_df['system'] == system)]

        # CATEGORY
        if categories is not None:

            if isinstance(categories, str) and categories == '':
                s_df = s_df[s_df['category'].isna()]
            else:

                s_df = s_df.dropna(subset=['category'])

                if isinstance(categories, str):

                    if categories == '*':
                        # If categories is '*', there is nothing else to do since
                        # the empty ones have already been removed.
                        pass

                    else:
                        if categories.lower().capitalize() not in self._cfg.categories:
                            raise TransactionsException(
                                'There is no category named "{}"'.format(categories))
                        else:
                            s_df = s_df[s_df['category'] ==
                                        categories.lower().capitalize()]

                elif isinstance(categories, list):

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

            if isinstance(tags, str) and tags == '':
                s_df = s_df[s_df['tags'].isna()]

            else:
                s_df = s_df.dropna(subset=['tags'])

                # Searching for tags in a list
                if isinstance(tags, list):

                    for tag in tags:
                        if tag.lower().capitalize() not in self._cfg.tags:
                            raise TransactionsException(
                                'There is no tag named "{}"'.format(tag))

                    print(tags)

                    findings = [s_df.loc[s_df['tags'].str.contains(
                        t, case=False)] for t in tags]

                    s_df = pd.concat(findings)

                # Searching for a single tag
                elif isinstance(tags, str):
                    if tags == '*':
                        # If tags is '*', there is nothing else to do since
                        # the empty ones have already been removed.
                        pass
                    else:
                        s_df = s_df.loc[s_df['tags'].str.contains(
                            tags, case=False)]

                # Searching for transactions with a specific number of tags
                elif isinstance(tags, int) and tags > 0:
                    s_df = s_df.loc[s_df['tags'].str.count(',') == tags - 1]

                else:
                    raise TransactionsException(
                        '"tags" has to be a list of strings, a single string, or an integer > 0; received "{}"'.format(tags))

        return s_df if s_df is not self._df else None

    def _sort(self) -> None:
        self._df.sort_values(by=['time'], inplace=True)
        self._df.reset_index(inplace=True, drop=True)

    def spread(self, i: int, amount: float, fee: float, note: str = None,
               category: str = None, tags: list = None) -> None:
        '''
        Adds an additional transaction with some of the values from the original
        one (at index 'i'). More entries will represent one transaction, but 
        total amount will be split among them.

        It's useful, for example, to decompose a supermarket transaction into 
        different categories (food, alcohol, cleaning), or to separate the 
        amount and the fee with different categories or tags.

        The new transaction's 'system' column will reference the ID of the
        original transaction.


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
        self._df.loc[i, 'system'] = 'spread'

        # Add the new transaction
        self.add_bulk(
            [pd.DataFrame(
                [[
                    self._df['id'].max() + 1,
                    self._df.loc[i]['time'],
                    'manual',
                    self._df.loc[i]['type'],
                    self._df.loc[i]['source'],
                    self._df.loc[i]['source_id'],
                    self._df.loc[i]['desc'],
                    float(amount),
                    float(fee),
                    float(amount + fee),
                    self._df.loc[i]['curr'],
                    note,
                    self._df.loc[i, 'id'],
                    category,
                    tags
                ]],
                columns=Config.headers()
            )
            ])

    def update(self, index: list = None,
               search_result: pd.DataFrame = None,
               time: str = None,
               type: str = None,
               source: str = None,
               description: str = None,
               amount: float = None,
               fee: float = None,
               note: str = None,
               system: str = None,
               category: str = None,
               tags: list = None,
               overwrite_tags: bool = True) -> dict:

        r = StdReturn(message="Transaction successfully updated")

        if index is None and search_result is None:
            r.success = False
            r.message = 'Backend error with the transaction identifier'
            r.details = '"index" or "search_result" must be set. Both cannot be empty.'
            return r

        elif index is not None and search_result is not None:
            r.success = False
            r.message = 'Backend error with the transaction identifier'
            r.details = '"index" and "search_result" were both set. Only one must be set.'
            return r

        # Get the indexes of the rows to update.
        # When the indexes were provided, the program will make it as a list.
        # When a search DF was provided, i will receive the indexes
        if index is not None:
            if isinstance(index, int) and index > 0:
                i = [index]
            elif isinstance(index, list):
                i = index
            else:
                r.success = False
                r.message = 'Backend error with the transaction index.'
                r.details = 'The method expects index as an int > 0 or list(int), it received a {}'.format(
                    type(i))
                return r

        elif search_result is not None:
            i = list(search_result.index.values)
            if len(i) == 0:
                r.success = False
                r.message = 'The search passed as parameter didn\'t return any transaction, thus the program had nothing to update.'
                return r

        if time is not None:
            # The time has to be validated and converted to the timezone before
            self._df.loc[i, 'time'] = time

        self._df.loc[i, 'input'] = 'updated'

        if type is not None:
            self._df.loc[i, 'desc'] = type

        if source is not None:
            source_obj = None

            for s in self._sources.sources:
                if source.lower() == s.name.lower():
                    source_obj = s

            if source_obj is None:
                r.success = False
                r.message = 'There is no source named "{}". Please, register this source first.'.format(
                    source)

            self._df.loc[i, 'source'] = source_obj.name
            self._df.loc[i, 'source_id'] = source_obj.id

        if description is not None:
            self._df.loc[i, 'desc'] = description

        if amount is not None:
            self._df.loc[i, 'amount'] = amount

        if fee is not None:
            self._df.loc[i, 'fee'] = fee

        if amount is not None or fee is not None:
            self._df.loc[i, 'total'] = self._df.loc[i]['amount'] + \
                self._df.loc[i]['fee']

        if note is not None:
            self._df.loc[i, 'note'] = note

        if system is not None:
            self._df.loc[i, 'system'] = system

        if category is not None:
            category = self._cfg.add_new_category(category)
            self._df.loc[i, 'category'] = category

        if tags is not None:
            tags = [self._cfg.add_new_tag(tag) for tag in tags]

            self._df.loc[i, 'tags'] = self._df.loc[i, 'tags'].apply(
                lambda r:
                ','.join(tags) if overwrite_tags or pd.isna(r['tags'])
                else
                ','.join(r['tags'].split(','))
                + [t for t in tags if t not in r['tags']]
            )

        return r
