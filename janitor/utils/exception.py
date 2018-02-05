from exceptions import Exception


class SweeperException(Exception):
    def __init__(self, message):
        _message = "SweeperException: {}".format(message)
        super(SweeperException, self).__init__(_message)


class HeaderNotFound(SweeperException):
    def __init__(self):
        super(HeaderNotFound, self).__init__(
            "Sweeper: Header not found in listing output")


class JSONParsingFailed(SweeperException):
    def __init__(self):
        super(JSONParsingFailed, self).__init__(
            "Sweeper: Failed to parse JSON from listing output")


class SweeperNotImplemented(SweeperException):
    def __init__(self, functionality):
        super(NotImplemented, self).__init__(
            "Sweeper: Functionality '{}' is not implemented yet".format(
                functionality
            )
        )
