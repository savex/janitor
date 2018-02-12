from exceptions import Exception


class SweeperBaseException(Exception):
    pass


class SweeperException(SweeperBaseException):
    def __init__(self, message, *args, **kwargs):
        super(SweeperException, self).__init__(message, *args, **kwargs)
        self.message = "SweeperException: {}".format(message)


class FailedToOpenProcess(SweeperException):
    def __init__(self, cmd, error):
        super(FailedToOpenProcess, self).__init__(
            "FailedToOpenProcess: Execution of '{}' returned: '{}' ".format(
                cmd,
                error
            ))


class SectionNotPresent(SweeperException):
    def __init__(self, section, profile):
        super(SectionNotPresent, self).__init__(
            "SectionNotFound: Section '{}' not present in profile of '{}' ".format(
                section,
                profile
            )
        )


class JSONParsingFailed(SweeperException):
    def __init__(self):
        super(JSONParsingFailed, self).__init__(
            "InvalidJSON: Failed to parse JSON from listing output")


class SweeperNotImplemented(SweeperException):
    def __init__(self, functionality):
        super(SweeperNotImplemented, self).__init__(
            "NotImplemented: Functionality '{}' is not implemented yet".format(
                functionality
            )
        )
