from datetime import datetime


def datetime_for_filename() -> str:
    return datetime.now().strftime(('%Y-%m-%d_%H-%M-%S'))
