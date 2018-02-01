import re

from time import sleep
from subprocess import Popen, PIPE

from common import logger, logger_cli
from utils.config import ConfigFileBase

_list_action_label = "list_action"
_sweep_action_label = "sweep_action"


class Sweeper(ConfigFileBase):
    def __init__(
            self,
            filter_regex,
            filepath,
            section_name="sweeper",
    ):
        super(Sweeper, self).__init__(section_name, filepath=filepath)

        # load default values
        self.presort_sections = self.get_value(
            "presort_sections",
            value_type=bool
        )

        if filter_regex is not None:
            self.common_filter = re.compile(filter_regex)
        else:
            self.common_filter = re.compile(self.get_value("common_filter"))

        self.retry_count = self.get_value("retry", value_type=int)
        self.retry_timeout = self.get_value("timeout", value_type=float)
        self.action_concurrency = self.get_value("concurrency", value_type=int)

        # initialize all sections
        self.sweep_items = {}

        sweep_items_list = self._config.sections()
        sweep_items_list.remove(section_name)
        for sweep_item in sweep_items_list:
            _item = {}
            _list_action_cmd = self._config.get(
                sweep_item,
                _list_action_label
            )
            _sweep_action_cmd = self._config.get(
                sweep_item,
                _sweep_action_label
            )

            _item[_list_action_label] = {}
            _item[_sweep_action_label] = {}

            _item[_list_action_label]["cmd"] = _list_action_cmd
            _item[_list_action_label]['output'] = None
            _item[_list_action_label]['error'] = None
            _item[_list_action_label]['return_code'] = None

            _item['data'] = None

            _item[_sweep_action_label]["cmd"] = _sweep_action_cmd
            _item[_sweep_action_label]["pool"] = {}

            self.sweep_items[sweep_item] = _item

        self.sweep_items_list = sweep_items_list
        if self.presort_sections:
            self.sweep_items_list.sort()

    @property
    def sections_list(self):
        _keys = self.sweep_items.keys()
        _keys.sort()
        return _keys

    def _get(self, section, action, item):
        return self.sweep_items[section][action][item]

    def _get_section_pool(self, section):
        return self._get(section, _sweep_action_label, "pool")

    def get_section_data(self, section):
        return self.sweep_items[section]["data"]

    def get_section_list_error(self, section):
        return self._get(section, _list_action_label, "error")

    def get_section_list_cmd(self, section):
        return self._get(section, _list_action_label, "cmd")

    def get_section_sweep_output(self, section, data_item):
        return self._get_section_pool(section)[data_item]["item_output"]

    def get_section_sweep_error(self, section, data_item):
        return self._get_section_pool(section)[data_item]["item_error"]

    def get_section_sweep_cmd(self, section, data_item):
        return self._get_section_pool(section)[data_item]["item_cmd"]

    @staticmethod
    def _action_process(cmd):
        logger.debug("...cmd: '{}'".format(cmd))
        _cmd = cmd.split()
        sweep_process = Popen(_cmd, stdout=PIPE, stderr=PIPE)
        _output, _err = sweep_process.communicate()
        _rc = sweep_process.returncode

        # log it
        logger.debug(
            "process [{}] '{}' returned\n"
            "--- start output ---\n{}\n--- end output ---\n"
            "Error: '{}'\n"
            "Return code: {}".format(
                sweep_process.pid,
                cmd,
                _output,
                _err,
                _rc
            )
        )
        return _output, _err, _rc

    def _do_list_action(self, section):
        # execute the list action

        _out, _err, _rc = self._action_process(
            self.sweep_items[section][_list_action_label]["cmd"]
        )

        # save it
        self.sweep_items[section][_list_action_label]["output"] = _out
        self.sweep_items[section][_list_action_label]["error"] = _err
        self.sweep_items[section][_list_action_label]["return_code"] = _rc

        # Handle result
        if _rc != 0:
            logger.debug("Non-zero exit code returned. No data will be saved")
        else:
            # save data
            _data = _out.splitlines()
            self.sweep_items[section]["data"] = _data

        return _rc

    def _do_sweep_action(self, section, item=None):
        # execute the sweep action with 'item' as a format param
        if item is None:
            logger.warn("Empty item supplied. "
                        "Sweep action ignored for '{}'.".format(section))
            return

        logger.debug("Sweep action for item '{}'".format(item))
        _cmd = self.sweep_items[section][_sweep_action_label]["cmd"]
        _cmd = _cmd.format(item)
        logger_cli.debug("+ '{}'".format(_cmd))

        _out, _err, _rc = self._action_process(_cmd)

        # handle specific RC
        #_rc = 1

        # if error received, log it and retry
        if _rc != 0:
            # retry action
            _retry_left = self.retry_count
            while _retry_left > 0:
                logger.warn(
                    "About to retry sweep action in {}ms, "
                    "{} retries left".format(
                        self.retry_timeout,
                        _retry_left
                    )
                )

                sleep(self.retry_timeout / 1000)
                _out, _err, _rc = self._action_process(_cmd)

                _retry_left -= 1

        # store
        _pool = self._get_section_pool(section)
        _pool[item] = {}
        _pool[item]["item_cmd"] = _cmd
        _pool[item]["item_output"] = _out
        _pool[item]["item_error"] = _err
        _pool[item]["item_return_code"] = _rc

        return _rc

    def _do_filter_action(self, section):
        # filter data set
        _data = []
        _raw_data = self.sweep_items[section]["data"]
        for data_item in _raw_data:
            logger.debug("About to apply filter for '{}'".format(
                data_item
            ))
            _filtered_data_item = self.common_filter.match(data_item)
            if _filtered_data_item is not None:
                logger.debug("..matched value '{}'".format(
                    _filtered_data_item.string
                ))
                _data.append(_filtered_data_item.string)

        # save filtered list
        self.sweep_items[section]["data"] = _data

        return

    def do_action(self, action, section=None, **kwargs):
        rc = 0
        if section is None:
            # Do all actions in order
            _sections = self.sweep_items.keys()
            _sections.sort()
            logger.info("...{} sections total".format(_sections.__len__()))

            # TODO: use gevent to generate subprocess
            for index in range(len(_sections)):
                _section = _sections[index]
                logger.debug("...running action '{}' for section '{}'".format(
                    str(action.__name__),
                    _section
                ))
                rc = action(_section, **kwargs)

            logger.info("...done")
        else:
            logger.info("--> {}".format(section))
            rc = action(section, **kwargs)

        return rc

    def list_action(self, section=None):
        # get data lists for section
        logger.info("List action started")
        return self.do_action(
            self._do_list_action,
            section=section
        )

    def sweep_action(self, section=None, item=None):
        # Do sweep action for data item given, None is handled deeper
        logger.info("Sweep action started")
        return self.do_action(
            self._do_sweep_action,
            section=section,
            item=item
        )

    def filter_action(self, section=None):
        # Do filter action according to selected filter
        logger.info("Filter action started")
        return self.do_action(
            self._do_filter_action,
            section=section
        )
