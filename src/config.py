import json


class ConfigException(Exception):
    pass


class Config:
    '''
    '''
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Config, cls).__new__(cls)
        return cls.instance

    def __init__(self) -> None:
        '''
        Headers of Transaction DataFrame:

            - time: date and time in UTC
            - acct: Stands for "Account". It's the source of the transaction
                    (bank account, card, manually added, etc.)
            - desc: Description
            - sum: Amount of the transaction
            - cur: Currency abbreviation
            - id: Unique identifier for the transaction
            - ref: When a transaction relates to another transaction, this cell will have
                the other transaction's ID
            - category: Category name
            - tags: List of tags
        '''

        self._config_db = None

        with open('../data/db/config.json', 'r') as f:
            self._config_db = json.load(f)

    @property
    def sources_db_path(self):
        return self._config_db['sources_db']

    @sources_db_path.setter
    def sources_db_path(self, value):
        raise ConfigException(
            'Sources DB path method not yet implemented')

    @property
    def transactions_db_path(self):
        return self._config_db['transactions_db']

    @transactions_db_path.setter
    def transactions_db_path(self, value):
        raise ConfigException(
            'Transactions DB method not yet implemented')
