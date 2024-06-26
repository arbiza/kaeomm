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
        '''
        self._app_name = 'kaeomm'

        if config_dir is not None:
            self._config_dir = config_dir if config_dir[-1] == '/' else config_dir + '/'

        with open(self._config_dir + 'config.json', 'r') as f:
            self._config_db = json.load(f)

        self.default_currency = self._config_db['default_currency']
        self.local_timezone = self._config_db['local_timezone']
        self.db_dir = self._config_db['db_dir']
        self._categories = [i.lower().capitalize()
                            for i in self._config_db['categories']]
        self._categories.sort()

        self._tags = [i.lower().capitalize() for i in self._config_db['tags']]
        self._tags.sort()

        self._system_categories = [
            'cash withdraw',  # cash withdraws are not expenses
            'currency exchange',
            'self transfer',  # moving from between sources are not accounted
        ]

    @property
    def app_name(self):
        return self._app_name

    @app_name.setter
    def app_name(self, value):
        raise ConfigException(
            '"app_name" can\'t be modified')

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

    @property
    def system_categories(self):
        return self._system_categories

    @system_categories.setter
    def system_categories(self, value):
        raise ConfigException('System Categories are hard-coded.')

    def add_new_category(self, category: str) -> str:
        c = category.lower().capitalize()
        if c not in self._categories and c != 'Nan' and c != '':
            self._categories.append(c)
            self._categories.sort()
        return c

    def add_new_tag(self, tag: str) -> str:
        t = tag.lower().capitalize()
        if t not in self._tags and t != 'Nan':
            self._tags.append(t)
            self._tags.sort()
        return t

    def del_category(self, category: str) -> None:
        self._categories.remove(category)

    def del_tag(self, tag: str) -> None:
        self._tags.remove(tag)

    @staticmethod
    def headers() -> list:
        '''Returns the transactions DataFrame headers list'''
        return ['id',
                'time',
                'input',
                'type',
                'source',
                'source_id',
                'desc',
                'amount',
                'fee',
                'total',
                'curr',
                'note',
                'system',
                'allot',
                'link',
                'category',
                'tags'
                ]

    def save(self) -> bool:
        config = {
            "default_currency": self.default_currency,
            "local_timezone": self.local_timezone,
            "db_dir": self.db_dir,
            "categories": self._categories,
            "tags": self._tags
        }
        with open(self._config_dir + 'config.json', "w") as f:
            f.write(json.dumps(config, indent=4))
        return True
