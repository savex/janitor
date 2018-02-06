from exceptions import Exception


class SweeperException(Exception):
    def __init__(self, message):
        _message = "SweeperException: {}".format(message)
        super(SweeperException, self).__init__(_message)


class FailedToOpenProcess(SweeperException):
    def __init__(self, cmd, error):
        super(FailedToOpenProcess, self).__init__(
            "Execution of '{}' returned: '{}' ".format(cmd, error))


class SectionNotPresent(SweeperException):
    def __init__(self, section, profile):
        super(SectionNotPresent, self).__init__(
            "Section '{}' not present in profile of '{}' ".format(
                section,
                profile
            )
        )


class JSONParsingFailed(SweeperException):
    def __init__(self):
        super(JSONParsingFailed, self).__init__(
            "Failed to parse JSON from listing output")


class SweeperNotImplemented(SweeperException):
    def __init__(self, functionality):
        super(SweeperNotImplemented, self).__init__(
            "Functionality '{}' is not implemented yet".format(
                functionality
            )
        )
