import json
import pandas

from config import Config
from statements import StatementsParser
from transactions import Transactions


class SourcesException(Exception):
    pass


class Sources:
    '''
    Singleton class to manage all the sources the user has configured.

    When the object is instanciated, it will load all the sources in the
    database file.
    '''
    def __new__(cls):
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
            with open(self._cfg.sources_db_path, 'r') as f:
                self._sources_db = json.load(f)

            # I have to load the Json sources into Objects

        except FileNotFoundError:
            pass

        except SourcesException as e:
            print(str(e))

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


class Source:

    def __init__(self, name: str, currency: str) -> None:
        self._name = name
        self._currency = currency
        self._id = "{}-{}".format(name, currency)
        self._type = None
        self._stmt_columns_mapping = []
        self._stmt_time_format = str()

    def statement_column_mapping(self, src_col: list, dst_col: str) -> None:
        if dst_col not in Transactions.headers():
            raise SourcesException(
                "The Transactions DataFrame has no column named '{}'\n".format(
                    dst_col
                )
            )
        elif dst_col in ['curr', 'source', 'total', 'id', 'ref']:
            # Some fields will be automatically set during the statement parsing;
            # the user is not able to set them.
            pass
        elif len(src_col) > 0:
            self._stmt_columns_mapping.append({
                "src": src_col,
                "dst": dst_col
            })

    def statement_parse(self, stmt_path: str) -> pandas.DataFrame:
        parser = StatementsParser(stmt_path)

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
        parser.fill_up_column('source', self._id)
        parser.conclude()
        return parser.df

    def to_json(self) -> json:
        return json.dumps({
            "name": self._name,
            "currency": self._currency,
            "stmt_columns_mapping": self._stmt_columns_mapping
        })
