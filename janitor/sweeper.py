import re
import json

from time import sleep
from copy import deepcopy
from subprocess import Popen, PIPE

from common import logger, logger_cli
from utils.config import ConfigFileBase
from utils.exception import *

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

        self.banner = self.get_value("banner")
        self.default_format_parser = self.get_value("default_format_parser")
        self.default_filter_field = self.get_value("default_filter_field")
        self.retry_count = self.get_value("retry", value_type=int)
        self.retry_timeout = self.get_value("timeout", value_type=float)
        self.action_concurrency = self.get_value("concurrency", value_type=int)

        self.protected_run_default = self.get_value(
            "default_protected_run",
            value_type=bool
        )

        self.last_return_code = 0

        # initialize all sections
        self.sweep_items = {}

        sweep_items_list = self._config.sections()
        sweep_items_list.remove(section_name)
        for sweep_item in sweep_items_list:
            self.sweep_items[sweep_item] = self._get_properties(
                sweep_item
            )

        self.sweep_items_list = sweep_items_list
        if self.presort_sections:
            self.sweep_items_list.sort()

    def _get_properties(self, section):
        # prepare for nested section parsing
        _section_dict = {}
        # load opton, if nothing is there - None will be returned
        action_map = self.get_safe(section, "map")
        if action_map is None:
            # No tree parsing, simple dict
            _section_dict = {}

            _list_cmd = self._config.get(section, _list_action_label)
            _sweep_cmd = self._config.get(section, _sweep_action_label)

            _protected = self.get_with_default(
                section,
                "protected_run",
                self.protected_run_default
            )

            _output_format = self.get_with_default(
                section,
                "output_format",
                self.default_format_parser
            )

            _filter_field = self.get_with_default(
                section,
                "filter_field",
                self.default_filter_field
            )

            _section_dict[_list_action_label] = {}
            _section_dict[_sweep_action_label] = {}

            _section_dict[_list_action_label]["cmd"] = _list_cmd
            _section_dict[_list_action_label]["output"] = None
            _section_dict[_list_action_label]["error"] = None
            _section_dict[_list_action_label]["return_code"] = None

            _section_dict["output"] = None
            _section_dict["output_format"] = _output_format
            _section_dict["filter_field"] = _filter_field
            _section_dict["filtered_output"] = None
            _section_dict["data"] = None
            _section_dict["protected_run"] = _protected
            _section_dict["last_rc"] = 0
            _section_dict["key"] = self._config.get(section, "key")

            _section_dict[_sweep_action_label]["cmd"] = _sweep_cmd
            _section_dict[_sweep_action_label]["pool"] = {}
        else:
            # Use map to build action tree

            raise SweeperNotImplemented("map")

            pass

        return _section_dict

    @property
    def sections_list(self):
        _keys = self.sweep_items.keys()
        _keys.sort()
        return _keys

    def _get(self, section, action, item):
        return self.sweep_items[section][action][item]

    def get_section_list_error(self, section):
        return self._get(section, _list_action_label, "error")

    def get_section_list_cmd(self, section):
        return self._get(section, _list_action_label, "cmd")

    def _get_section_sweep_pool(self, section):
        return self._get(section, _sweep_action_label, "pool")

    def get_section_sweep_output(self, section, data_item):
        return self._get_section_sweep_pool(section)[data_item]["item_output"]

    def get_section_sweep_error(self, section, data_item):
        return self._get_section_sweep_pool(section)[data_item]["item_error"]

    def get_section_sweep_cmd(self, section, data_item):
        return self._get_section_sweep_pool(section)[data_item]["item_cmd"]

    def get_section_output(self, section):
        return self.sweep_items[section]["output"]

    def get_section_filtered_output(self, section):
        return self.sweep_items[section]["filtered_output"]

    def get_section_data(self, section):
        return self.sweep_items[section]["data"]

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
            # parse data and
            # filter it according to selected type
            _data, _filtered = None, None
            _format = self.sweep_items[section]["output_format"]
            if _format == "json":
                _data = json.loads(_out)
                _filtered = self._filter_json(
                    _data,
                    self.sweep_items[section]["filter_field"]
                )
            elif _format == "raw":
                _data = _out.splitlines()
                _filtered = self._filter_raw(_data)
            self.sweep_items[section]["output"] = _data
            self.sweep_items[section]["filtered_output"] = _filtered

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
        # _rc = 1

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
        _pool = self._get_section_sweep_pool(section)
        _pool[item] = {}
        _pool[item]["item_cmd"] = _cmd
        _pool[item]["item_output"] = _out
        _pool[item]["item_error"] = _err
        _pool[item]["item_return_code"] = _rc

        return _rc

    def _do_matching(self, unfiltered):
        logger.debug("About to apply filter for '{}'".format(unfiltered))
        _filtered_data_item = self.common_filter.match(unfiltered)
        if _filtered_data_item is not None:
            logger.debug("..matched value '{}'".format(
                _filtered_data_item.string
            ))
            return _filtered_data_item.string
        else:
            return None

    def _filter_raw(self, data):
        _filtered = []
        for data_item in data:
            _filtered_data_item = self._do_matching(data_item)
            if _filtered_data_item is not None:
                _filtered.append(deepcopy(_filtered_data_item))
        return _filtered

    def _filter_json(self, json_data, field):
        _filtered = []
        for json_item in json_data:
            _filtered_data_item = self._do_matching(json_item[field])
            if _filtered_data_item is not None:
                _filtered.append(deepcopy(json_item))
        return _filtered

    def _do_serialize_data_action(self, section):
        _dict = {}
        # this function should produce dict of data to be handled by sweep
        # { "key_field": {
        #       <filter_field>: "name1",
        #       <section_child>: {}
        #    }
        # }


        # Load header if it is set
        # _lines = self.get_section_output(section)
        #
        #
        # _section_key = self.sweep_items[section]["key"]
        #
        # if _section_key == "*":
        #     _dict[_section_key] = deepcopy(
        #         self.get_section_filtered_output(section)
        #     )
        # elif _section_key
        #

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
                # check if it is eligible to execute action
                if self.sweep_items[_section]["protected"] and \
                        self.last_return_code != 0:
                    logger_cli.warn(
                        "...dropping protected section due to previous error"
                    )
                    continue
                # run action
                logger.debug("...running action '{}' for section '{}'".format(
                    str(action.__name__),
                    _section
                ))
                rc = action(_section, **kwargs)
                self.sweep_items[_section]["last_rc"] = rc

            logger.info("...done")
        else:
            # check if it is eligible to execute action
            if self.sweep_items[section]["protected_run"] and \
                    self.last_return_code != 0:
                logger_cli.warn(
                    "...dropping protected section due to previous error"
                )
                return

            logger.info("--> {}".format(section))
            rc = action(section, **kwargs)
            self.sweep_items[section]["last_rc"] = rc

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

    def serialize_data(self, section=None):
        # Serialize data from filtered output to tree
        logger.info("Extract 'key' data action started")
        return self.do_action(
            self._do_serialize_data_action,
            section=section
        )
