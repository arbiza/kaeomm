import json


class ConfigException(Exception):
    pass


class Config:
    '''
    '''
    def __new__(cls, config_dir: str = None):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Config, cls).__new__(cls)
        return cls.instance

    def __init__(self, config_dir: str = None) -> None:
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
        if config_dir is not None:
            self._config_dir = config_dir if config_dir[-1] == '/' else config_dir + '/'

        with open(self._config_dir + 'config.json', 'r') as f:
            self._config_db = json.load(f)

        self.default_currency = self._config_db['default_currency']
        self.local_timezone = self._config_db['local_timezone']
        self.db_dir = self._config_db['db_dir']
        self._categories = [i.lower().capitalize()
                            for i in self._config_db['categories']]
        self._tags = [i.lower().capitalize() for i in self._config_db['tags']]

    @property
    def categories(self):
        return self._categories

    @categories.setter
    def categories(self, value):
        raise ConfigException(
            '"categories" can\'t be set directly')

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, value):
        raise ConfigException(
            '"tags" can\'t be set directly')

    def add_new_category(self, category: str) -> None:
        if category not in self._categories:
            self._categories.append(category.lower().capitalize())

    def add_new_tag(self, tag: str) -> None:
        if tag not in self._tags:
            self._tags.append(tag.lower().capitalize())

    def del_category(self, category: str) -> None:
        self._categories.remove(category)

    def del_tag(self, tag: str) -> None:
        self._tags.remove(tag)

    def save(self) -> bool:
        config = {
            "default_currency": self.default_currency,
            "local_timezone": self.local_timezone,
            "transactions_db": self.transactions_db_path,
            "sources_db": self.sources_db_path,
            "categories": self._categories,
            "tags": self._tags
        }
        with open(self._config_dir + 'config.json', "w") as f:
            f.write(json.dumps(config, indent=4))
        return True
