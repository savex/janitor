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


class DataCache(dict):
    params = {}

    def __getattr__(self, item):
        try:
            _value = self.params[item]
        except KeyError:
            self.params[item] = DataCache()
        finally:
            return self.params[item]

    def __setattr__(self, key, value):
        self.params[key] = value


class Sweeper(ConfigFileBase):
    def __init__(
            self,
            filter_regex,
            filepath,
            section_name="sweeper",
    ):
        super(Sweeper, self).__init__(section_name, filepath=filepath)

        self.profilepath = filepath

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

    def _get_subsection_properties(self, section, prefix=""):
        __section_dict = {}

        _list_cmd = self._config.get(section, prefix + _list_action_label)
        _sweep_cmd = self._config.get(section, prefix + _sweep_action_label)

        _output_format = self.get_with_default(
            section,
            prefix + "output_format",
            self.default_format_parser
        )

        _filter_field = self.get_with_default(
            section,
            prefix + "filter_field",
            self.default_filter_field
        )

        if len(prefix) > 0:
            __section_dict["section_name"] = section + " ({})".format(
                prefix[:-1]
            )
        else:
            __section_dict["section_name"] = section
        __section_dict[_list_action_label] = {}
        __section_dict[_sweep_action_label] = {}

        __section_dict[_list_action_label]["cmd"] = _list_cmd
        __section_dict[_list_action_label]["output"] = None
        __section_dict[_list_action_label]["error"] = None
        __section_dict[_list_action_label]["return_code"] = None

        __section_dict["action_map"] = self.get_with_default(
            section,
            prefix + "action_map",
            None
        )

        __section_dict["output"] = None
        __section_dict["output_format"] = _output_format
        __section_dict["filter_field"] = _filter_field
        __section_dict["filtered_output"] = None
        __section_dict["data"] = None
        __section_dict["last_rc"] = 0
        __section_dict["key"] = self._config.get(section, prefix + "key")
        __section_dict["child_options"] = self.get_with_default(
            section,
            prefix + "child_options",
            None
        )

        __section_dict[_sweep_action_label]["cmd"] = _sweep_cmd
        __section_dict[_sweep_action_label]["pool"] = {}

        return __section_dict

    def _get_child_subsection(self, section, _map, _level):
        _levels = _map.split('.')
        _prefix = _levels[_level]

        _this_level_dict = self._get_subsection_properties(
            section,
            _prefix + "_"
        )

        if len(_levels)-1 > _level:
            _this_level_dict[_levels[_level+1]] = {}
            _this_level_dict[_levels[_level+1]] = self._get_child_subsection(
                section,
                _map,
                _level+1
            )

        return _this_level_dict

    def _get_properties(self, section):
        # load opton, if nothing is there - None will be returned
        action_map = self.get_safe(section, "action_map")

        if action_map is None:
            # No tree parsing, simple dict
            _section_dict = self._get_subsection_properties(section)
        else:
            # Use map to build action tree
            _section_dict = self._get_child_subsection(section, action_map, 0)

        _protected = self.get_with_default(
            section,
            "protected_run",
            self.protected_run_default
        )
        _section_dict["protected_run"] = _protected

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
        try:
            sweep_process = Popen(_cmd, stdout=PIPE, stderr=PIPE)
        except OSError as e:
            raise FailedToOpenProcess(" ".join(_cmd), e.strerror)

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

    def _do_list_action(self, _section_data):
        # execute the list action

        _out, _err, _rc = self._action_process(
            _section_data[_list_action_label]["cmd"]
        )

        # save it
        _section_data[_list_action_label]["output"] = _out
        _section_data[_list_action_label]["error"] = _err
        _section_data[_list_action_label]["return_code"] = _rc

        # Handle result
        if _rc != 0:
            logger.debug("Non-zero exit code returned. No data will be saved")
        else:
            # parse data and
            # filter it according to selected type
            _data, _filtered = None, None
            _format = _section_data["output_format"]
            if _format == "json":
                _data = json.loads(_out)
                _filtered = self._filter_json(
                    _data,
                    _section_data["filter_field"]
                )
            elif _format == "raw":
                _data = _out.splitlines()
                _filtered = self._filter_raw(_data)
            _section_data["output"] = _data
            _section_data["filtered_output"] = _filtered

        return _rc

    def _do_sweep_action(self, _section_data, item=None):
        # execute the sweep action with 'item' as a format param
        if item is None:
            logger.warn(
                "Empty item supplied. Sweep action ignored '{}'.".format(
                    _section_data[_sweep_action_label]["cmd"]
                )
            )
            return

        logger.debug("Sweep action for item '{}'".format(item))
        _cmd = _section_data[_sweep_action_label]["cmd"]
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
        _pool = _section_data[_sweep_action_label]["pool"]
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
            _filtered_value = None
            if field is None:
                # filter all fields
                for key, value in json_item.iteritems():
                    _filtered_value = self._do_matching(value)
                    if _filtered_value is not None:
                        # break if found
                        break
            else:
                _filtered_value = self._do_matching(json_item[field])

            if _filtered_value is not None:
                # found matching, add it
                _filtered.append(deepcopy(json_item))

        return _filtered

    def do_action(self, action, _section_data=None, **kwargs):
        rc = 0
        if _section_data is None:
            # Do all actions in order
            _sections = self.sweep_items.keys()
            _sections.sort()
            logger.info("...{} sections total".format(_sections.__len__()))

            # TODO: use gevent to generate subprocess
            for index in range(len(_sections)):
                _section = _sections[index]
                _section_data = self.sweep_items[_section]
                # check if it is eligible to execute action
                if "protected_run" in _section_data and \
                        _section_data["protected_run"] and \
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
                _section_data["last_rc"] = rc

            logger.info("...done")
        else:
            # check if it is eligible to execute action
            if _section_data["protected_run"] and \
                    self.last_return_code != 0:
                logger_cli.warn(
                    "...dropping protected section due to previous error"
                )
                return

            logger.info("-> {}".format(_section_data["section_name"]))
            rc = action(_section_data, **kwargs)
            _section_data["last_rc"] = rc

        return rc

    def _list_action_runner(self, _section_data, _map, _level):
        rc = 0
        # Process next levels
        _levels = _map.split('.')
        if len(_levels)-1 > _level:
            # there is a next level present
            _next_level = _levels[_level]
            rc = self._list_action_runner(
                _section_data[_next_level],
                _map,
                _level+1
            )

        # ...process this level
        _level_path = " -> ".join(_levels[:_level])
        logger_cli.info("# {}".format(_level_path))

        # do listing for this section, will produce filtered out
        rc = self.do_action(
            self._do_list_action,
            _section_data
        )
        if rc > 0:
            logger_cli.warn("##### Failed to list objects")
            return rc

        # list all filtered 'key' childs and force them to be added as filtered
        _format = _section_data["output_format"]
        _key = _section_data["key"]
        for item in _section_data["filtered_output"]:
            # iterate key values
            _value = None
            if _format == "json":
                _value = item[_key]
            elif _format == "raw":
                _value = item



            rc = self.do_action(
                self._do_list_action,
                _section_data
            )

        return rc

    def list_action(self, section=None):
        logger.info("List action started")
        # if map is present, do child listings as well
        _map = self.sweep_items[section]["action_map"]
        if _map is None:
            _map = section

        logger_cli.info("# section map is '{}'".format(_map))
        # do listing using map, child first
        # process child level

        # get data lists for section
        rc = self._list_action_runner(self.sweep_items[section], _map, 0)
        return rc

    def _sweep_action_runner(self, _section_data, _map, _level):
        rc = 0
        # Process next levels
        _levels = _map.split('.')

        if len(_levels)-1 > _level:
            # there is a next level present
            _next_level = _levels[_level]
            rc = self._sweep_action_runner(
                _section_data[_next_level],
                _map,
                _level+1
            )

        # ...process this level
        _level_path = " -> ".join(_levels[:_level])
        logger_cli.info("# {}".format(_level_path))
        # this section options
        _format = _section_data["output_format"]
        _filtered_output = _section_data["filtered_output"]
        _key = _section_data["key"]
        # iterate it
        _count = len(_filtered_output)
        for item in _filtered_output:
            _data_item = None
            if _format == "json":
                _data_item = item[_key]
            elif _format == "raw":
                _data_item = item

            logger_cli.info("\t> {}: {}".format(
                _count,
                _data_item
            ))

            rc = self.do_action(
                self._do_sweep_action,
                _section_data=_section_data,
                item=_data_item
            )

            if rc != 0:
                logger_cli.error("\t({}) '{}'\n\tERROR: {}".format(
                    rc,
                    self.get_section_sweep_cmd(
                        _section_data["section_name"],
                        _data_item
                    ),
                    self.get_section_sweep_error(
                        _section_data["section_name"],
                        _data_item
                    )
                ))
            else:
                logger_cli.info("{}".format(
                    self.get_section_sweep_output(
                        _section_data["section_name"],
                        _data_item
                    )
                ))
            _count -= 1

        return rc

    def sweep_action(self, section=None):
        # Do sweep action for data item given, None is handled deeper
        logger.info("Sweep action started")
        _map = self.sweep_items[section]["action_map"]
        if _map is None:
            _map = section

        logger.info("Section map is '{}'".format(_map))
        # do sweep using map, child first
        # process child level

        # sweep it
        rc = self._sweep_action_runner(self.sweep_items[section], _map, 0)

        return rc
