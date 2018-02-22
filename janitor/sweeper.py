import re
import json

from time import sleep
from copy import deepcopy
from subprocess import Popen, PIPE

from common import logger, logger_cli
from utils import merge_dict
from utils.config import ConfigFileBase
from utils.exception import FailedToOpenProcess

_list_action_label = "list_action"
_sweep_action_label = "sweep_action"


class DataCache(dict):
    def __getattr__(self, item):
        d = vars(self)
        try:
            _value = d[item]
        except KeyError:
            _value = d[item] = type(self)()

        return _value

    def __setattr__(self, key, value):
        vars(self)[key] = value


class Sweeper(ConfigFileBase):
    def __init__(
            self,
            filter_regex,
            filepath,
            bash_action,
            section_name="sweeper",
    ):
        super(Sweeper, self).__init__(section_name, filepath=filepath)

        self.profilepath = filepath
        self.cache = DataCache()
        self.bash_action = bash_action

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

        __section_dict["output"] = None
        __section_dict["output_format"] = _output_format
        __section_dict["filter_field"] = _filter_field
        __section_dict["filtered_output"] = None
        __section_dict["data"] = None
        __section_dict["last_rc"] = 0
        __section_dict["key"] = self._config.get(section, prefix + "key")
        __section_dict["as_child_options"] = self.get_with_default(
            section,
            prefix + "as_child_options",
            None
        )

        __section_dict[_sweep_action_label]["cmd"] = _sweep_cmd
        __section_dict[_sweep_action_label]["pool"] = {}

        return __section_dict

    def _get_child(self, section, _map, _level, _dict):
        _levels = _map.split('.')
        _prefix = _levels[_level]

        if len(_levels) - 1 > _level:
            _dict[_levels[_level + 1]] = {}
            _dict[_levels[_level + 1]] = self._get_child(
                section,
                _map,
                _level + 1,
                _dict[_levels[_level + 1]]
            )

        merge_dict(
            self._get_subsection_properties(
                section,
                _prefix + "_"
            ),
            _dict
        )

        return _dict

    def _get_properties(self, section):
        # load opton, if nothing is there - None will be returned
        action_map = self.get_safe(section, "action_map")

        _root_section = {}

        if action_map is None:
            # No tree parsing, simple dict
            _root_section = self._get_subsection_properties(section)
        else:
            # Use map to build action tree
            _root_section[action_map.split('.')[0]] = self._get_child(
                section,
                action_map,
                0,
                {}
            )

        _root_section["protected_run"] = self.get_with_default(
            section,
            "protected_run",
            self.protected_run_default
        )
        _root_section["action_map"] = action_map

        return _root_section

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

    def is_section_present(self, section):
        return True if section in self.sections_list else False

    @staticmethod
    def get_data_item(_frmt, item, key=None):
        if _frmt == "json":
            return item[key]
        elif _frmt == "raw":
            return item
        else:
            return None

    @staticmethod
    def get_cache_key_name(data):
        _frmt = data["output_format"]
        if _frmt == "json":
            return "item." + data["section_name"] + "." + data["key"]
        elif _frmt == "raw":
            return "item." + data["section_name"] + ".raw"

    @staticmethod
    def _action_process(cmd, test=False):
        logger.debug("...cmd: '{}'".format(cmd))
        if test:
            logger_cli.info("{}\n".format(cmd))
            return None, None, 0

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

    def _do_list_action(
            self,
            cmd,
            expected_format=None,
            use_filter=False,
            filter_key=None
    ):
        # execute the list action
        if self.bash_action == 'list':
            _out, _err, _rc = self._action_process(cmd, test=True)
            return _rc, _out, None
        else:
            _out, _err, _rc = self._action_process(cmd)

        # Handle result
        _data, _filtered = None, None
        if _rc != 0:
            logger.debug("Non-zero exit code returned. No data will be saved")
        else:
            # parse data and
            # filter it according to selected type
            if expected_format == "json":
                _data = json.loads(_out)
                if use_filter:
                    _filtered = self._filter_json(
                        _data,
                        filter_key
                    )
            else:
                _data = _out.splitlines()
                if use_filter:
                    _filtered = self._filter_raw(_data)

        return _rc, _data, _filtered

    def _do_sweep_action(self, cmd):
        logger_cli.debug("+ '{}'".format(cmd))

        _out, _err, _rc = self._action_process(cmd)

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
                _out, _err, _rc = self._action_process(cmd)

                _retry_left -= 1

        return _rc, cmd, _out, _err

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

    @staticmethod
    def do_action(action, cmd, **kwargs):
        # TODO: use gevent to generate subprocess
        logger.info("Running '{}'. CMD:'{}', ARGS:'{}'".format(
            action,
            cmd,
            kwargs
        ))

        return action(cmd, **kwargs)

    def _get_map_for_section(self, section):
        _map = self.sweep_items[section]["action_map"]
        if _map is None:
            logger_cli.debug("## no action map")
            _map = section
            _data = self.sweep_items[section]
        else:
            logger_cli.debug("## action map is '{}'".format(_map))
            _map = _map.split('.')
            # extract data for root level right away
            _data = self.sweep_items[section][_map[0]]

        return _map, _data

    @staticmethod
    def _format_variables(format_string, cache):
        # fill in var values from cache
        _format_list = format_string.split(':')
        _vars = ()
        for var in _format_list[1].split(','):
            _vars += cache.__getattr__(var)
        _formatted = _format_list[0].format(_vars)
        return _formatted

    def _do_list_as_child(self, data, _map, level, cache):
        # get all items on this level for supplied options

        _cmd = data[_list_action_label]["cmd"]
        _options = self._format_variables(data["as_child_options"], cache)
        cmd = _cmd + " " + _options

        _items = self.do_action(
            self._do_list_action,
            cmd,
            expected_format=data["output_format"]
        )

        data["sweep_items"].extend(_items)

        # Process next levels
        if len(_map) - 1 > level:
            # there is a next level present
            _next_level = _map[level + 1]
            for item in _items:
                _item = self.get_data_item(
                    data["output_format"],
                    item,
                    key=data["key"]
                )
                cache.__setattr__(
                    self.get_cache_key_name(data),
                    _item
                )
                self._do_list_as_child(
                    data[_next_level],
                    _map,
                    level + 1,
                    cache
                )

    def _list_action_runner(self, _data, _map, _level):
        # Process next levels
        if len(_map) - 1 > _level:
            # there is a next level present
            _next_level = _map[_level + 1]
            self._list_action_runner(
                _data[_next_level],
                _map,
                _level + 1
            )

        # do listing for this section, will produce filtered out
        # prepare cmd and options
        _cmd = _data[_list_action_label]["cmd"]
        _format = _data["output_format"]
        _filter_key = _data["filter_field"]

        # run initial action with filter
        rc, output, filtered = self.do_action(
            self._do_list_action,
            _cmd,
            expected_format=_format,
            use_filter=True,
            filter_key=_filter_key
        )

        if rc > 0:
            logger_cli.warn("##### Failed to list objects")
            return rc
        elif self.bash_action == "list":
            return rc

        if len(_map) > 1:
            # ...process this level
            _level_path = " -> ".join(_map[:_level + 1])
            logger_cli.debug("## {}".format(_level_path))


            # list all filtered 'key' childs
            # and force them to be added as filtered
            _key = _data["key"]
            for item in filtered:
                # iterate key values
                _value = self.get_data_item(_format, item, _key)

                cache = DataCache()
                cache.__setattr__(
                    self.get_cache_key_name(_data),
                    _value
                )

                self._do_list_as_child(_data, _map,_level, cache)

                _data["sweep_items"].append(item)

        _data["output"] = output
        _data["filtered_output"] = filtered

        return rc

    def list_action(self, section=None):
        logger_cli.debug("## list action started")

        # if map is present, do child listings as well
        _map, _data = self._get_map_for_section(section)

        # do listing using map, child first
        # process child level

        # get data lists for section
        # check if it is eligible to execute action
        if self.sweep_items[section]["protected_run"] \
                and self.last_return_code != 0:
            logger_cli.warn(
                "# WARN: ...dropping protected section due to previous error"
            )
            return 0
        _data["sweep_items"] = []
        return self._list_action_runner(_data, _map, 0)

    def _sweep_action_runner(self, data, _map, _level, cache=None):
        # At this point, we should have
        # all of the items in filtered ready for sweep.
        _name = data["section_name"]
        _format = data["output_format"]
        _tab_space = "\t" * (_level + 1)

        # announce section
        logger_cli.info("{}==> '{}'".format(_tab_space, _name))

        # Run next levels first
        if len(_map) - 1 > _level:
            # there is a next level present
            _next_level = _map[_level + 1]
            self._list_action_runner(
                data[_next_level],
                _map,
                _level + 1
            )

        # At this point, we should have
        # all of the items in filtered ready for sweep.
        _name = data["section_name"]
        _format = data["output_format"]

        # get cmd for this section
        _cmd = data[_sweep_action_label]["cmd"]

        # key for this section to load from 'item'
        _key = data["key"]

        # Take item from filtered
        _count = len(data["sweep_items"])
        for _data_item in data["sweep_items"]:
            if _format == "json":
                logger_cli.info("\t> {}: {}".format(_count, _data_item[_key]))
                # handle parameter in cmd using format
                _cmd.format(_data_item[_key])
            elif _format == "raw":
                # show item in processing
                logger_cli.info("\t> {}: {}".format(_count, _data_item))
                # handle parameter in cmd using format
                _cmd.format(_data_item)

            _formatted_cmd = self._format_variables(_cmd, _cache)

            # then execute sweep on this level
            rc, cmd, output, error = self.do_action(
                self._do_sweep_action,
                _cmd,
            )

            # store
            _pool = data["pool"]
            _pool[_data_item] = {}
            _pool[_data_item]["item_cmd"] = cmd
            _pool[_data_item]["item_output"] = output
            _pool[_data_item]["item_error"] = error
            _pool[_data_item]["item_return_code"] = rc

            rc = self.do_action(
                self._do_sweep_action,
                _cmd
            )

            if rc != 0:
                logger_cli.error("\t({}) '{}'\n\tERROR: {}".format(
                    rc,
                    self.get_section_sweep_cmd(
                        _name,
                        _data_item
                    ),
                    self.get_section_sweep_error(
                        _name,
                        _data_item
                    )
                ))
            else:
                logger_cli.info("{}".format(
                    self.get_section_sweep_output(
                        data["section_name"],
                        _data_item
                    )
                ))
            _count -= 1

        return rc

    def sweep_action(self, section=None):
        # Do sweep action for data item given, None is handled deeper
        logger_cli.debug("## sweep action started")

        # if map is present, do child listings as well
        _map, _data = self._get_map_for_section(section)

        # sweep it
        return self._sweep_action_runner(self.sweep_items[section], _map, 0)
