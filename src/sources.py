import json

from config import Config
from statements import StatementsParser


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
            self._df = pd.read_csv(self._cfg.sources_db_path, sep='|')
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
        self._name = "{}-{}".format(name, currency)
        self._currency = currency
        self._type = None
        self._stmt_columns_mapping = []

    def statement_column_mapping(self, src_col: str, dst_col: str) -> None:
        self._stmt_columns_mapping.append({
            "src": src_col,
            "dst": dst_col
        })

    def statement_parse(self, stmt_path: str) -> bool:
        parser = StatementsParser(stmt_path)

    def to_json(self) -> json:
        return json.dumps({
            "name": self._name,
            "currency": self._currency,
            "stmt_columns_mapping": self._stmt_columns_mapping
        })
