from datetime import datetime


class UtilsException(Exception):
    pass


class StdReturn:
    '''
    Structure to keep the functions and methods return consistent
    '''

    def __init__(self, success: bool = True, message: str = None, details: str = None) -> None:
        '''
        '''
        self.success = success
        self.message = message
        self.details = details

    @property
    def success(self):
        return self._success

    @success.setter
    def success(self, value):
        if value is True or value is False:
            self._success = value
        else:
            raise UtilsException('"success" accepts only "True" or "False"')


def datetime_for_filename() -> str:
    return datetime.now().strftime(('%Y-%m-%d_%H-%M-%S'))
