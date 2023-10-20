import json
import pandas
import time
import pytz
import tzlocal

from config import Config
from statements import StatementsParser
from transactions import Transactions
import utils


class SourcesException(Exception):
    pass


class Source:

    def __init__(self, name: str,
                 currency: str,
                 timezone: str = None,
                 id: float = None) -> None:
        self._name = name
        self._currency = currency
        self._id = time.time() if id is None else float(id)
        self._stmt_columns_mapping = []
        self.description = str()

        if timezone is None:
            self._stmt_timezone = tzlocal.get_localzone_name()
        if timezone in pytz.all_timezones_set:
            self._stmt_timezone = timezone
        else:
            raise SourcesException(
                "There is no timezone named '{}'\n".format(timezone))

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        # TODO
        # changing name will be allowed, but it requires to change it in the
        # transactions dataframe
        raise SourcesException('"name" can\'t be directly modified YET.')

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        raise SourcesException('"id" can\'t be directly modified.')

    def add_stmt_column_mapping(self, src_col: list, dst_col: str) -> None:
        if dst_col not in Transactions.headers():
            raise SourcesException(
                "The Transactions DataFrame has no column named '{}'\n".format(
                    dst_col
                )
            )
        elif dst_col in ['curr', 'source', 'total']:
            # Some fields will be automatically set during the statement parsing;
            # the user is not able to set them.
            pass
        elif len(src_col) > 0:
            self._stmt_columns_mapping.append({
                "src": src_col,
                "dst": dst_col
            })

    def statement_parse(self, stmt_path: str) -> pandas.DataFrame:
        parser = StatementsParser(stmt_path, self._stmt_timezone)

        # Proceeds only if at least one column mapping has been set
        if len(self._stmt_columns_mapping) == 0:
            raise SourcesException(
                "None 'column mapping'has been set. The statement can't be parsed\n"
                "Set the columns mapping and try again\n"
            )

        # Proceeds only if the columns names exist on the statement (for the
        # transactions DF, it's already been checked)
        for l_src in [l['src'] for l in self._stmt_columns_mapping]:
            for col_name in l_src:
                if col_name not in parser.get_stmt_columns():
                    raise SourcesException(
                        "The statement provided has no column named '{}'".format(col_name))

        [parser.import_column(l['src'], l['dst'])
         for l in self._stmt_columns_mapping]

        parser.fill_up_column('curr', self._currency)
        parser.fill_up_column('source', self._name)
        parser.fill_up_column('source_id', self._id)
        parser.conclude()
        return parser.df

    def to_dict(self) -> json:
        return {
            "name": self._name,
            "currency": self._currency,
            "id": self._id,
            "description": self.description,
            "stmt_timezone": self._stmt_timezone,
            "stmt_columns_mapping": self._stmt_columns_mapping
        }


# Types
# bank account
# savings
# Investiments (like a bank account)
# cash
# credit card


class Sources:
    '''
    Singleton class to manage all the sources the user has configured. It's like
    a wrapper for sources with some additional functions, such as handling the 
    sources database.

    When the object is instanciated, it will load all the sources in the
    database file.
    '''
    def __new__(cls, cfg: Config):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Sources, cls).__new__(cls)
        return cls.instance

    def __init__(self, cfg: Config) -> None:
        '''
        Loads the sources database into a list, if the database
        exists. If not, it creates an empty DF with the set of columns defined in
        this class "header()" static method.
        '''

        self._sources = []

        # 'Config' is a Singleton class. self._cfg attributes' values will update
        # if the Config object is modified anywhere else. This is specially
        # important to keep the path to the files always actual.
        self._cfg = cfg

        # Loads the existing sources into the sources list
        try:
            with open(self._cfg.db_dir + 'sources.json', 'r') as f:
                sources_db_json = json.load(f)

        except FileNotFoundError:
            pass

        except SourcesException as e:
            print(str(e))

        else:
            for s in sources_db_json['Sources']:
                src = Source(s['name'],
                             s['currency'],
                             s['stmt_timezone'],
                             s['id'])
                src.description = s['description']
                [src.add_stmt_column_mapping(m['src'], m['dst'])
                    for m in s['stmt_columns_mapping']]
                self._sources.append(src)

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, value):
        raise SourcesException(
            'Sources can\' be directly modified.')

    @staticmethod
    def headers() -> list:
        '''Returns the Sources DataFrame headers'''
        return ['id',
                'name',
                'type',
                'note'
                ]

    def add_source(self, src: Source) -> None:

        for s in self._sources:
            if src.name == s.name:
                raise SourcesException(
                    "There is already a source named '{}'".format(src.name))

        self._sources.append(src)
        self.save()

    def backup(self) -> None:
        '''
        Saves the current sources database to a file named with a timestamp
        '''
        srcs_json = {
            "Sources": [s.to_dict() for s in self._sources]
        }

        with open(self._cfg.db_dir + 'sources_' + utils.datetime_for_filename() + '.csv', "w") as f:
            f.write(json.dumps(srcs_json, indent=4))
        return True

    def get_source(self, name: str) -> Source:
        '''
        Returns the Source object with the name passed as argument.
        '''
        src = [s for s in self._sources if s.name == name]
        return src[0] if len(src) > 0 else []

    def reset(self) -> None:
        '''
        Backup the current database, then clean it up.
        '''
        self.backup()
        self._sources.clear()

    def save(self) -> None:
        srcs_json = {
            "Sources": [s.to_dict() for s in self._sources]
        }

        with open(self._cfg.db_dir + 'sources.json', "w") as f:
            f.write(json.dumps(srcs_json, indent=4))
        return True
