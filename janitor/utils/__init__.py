import file
import config
import logger
import exception

file_utils = file
logger = logger
config = config


def merge_dict(source, destination):
    """
    Dict merger, thanks to vincent
    http://stackoverflow.com/questions/20656135/python-deep-merge-dictionary-data

    >>> a = { 'first' : { 'all_rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'all_rows' : { 'fail' : 'cat', 'number' : '5' } } }
    >>> merge(b, a) == {
        'first' : {
            'all_rows' : {
                'pass' : 'dog',
                'fail' : 'cat',
                'number' : '5'
            }}}
    True

    :param source: source dict
    :param destination: second dict and also resulting one
    """
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            merge_dict(value, node)
        else:
            destination[key] = value

    return destination
