import utils

utils = utils
logger, logger_cli = utils.logger.setup_loggers(
    "janitor",
    log_fname=utils.config.sweeper.get_logfile_path()
)
