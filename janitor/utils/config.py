import ConfigParser
import os

pkg_dir = os.path.dirname(__file__)
pkg_dir = os.path.join(pkg_dir, os.path.pardir)
pkg_dir = os.path.normpath(pkg_dir)
pkg_dir = os.path.abspath(pkg_dir)

_default_config_path = os.path.join(pkg_dir, 'etc')
_inspector_default_config_name = 'janitor.conf'

_default_config = os.path.join(
    _default_config_path,
    _inspector_default_config_name
)


class ConfigFileBase(object):
    _truth = ['true', '1', 't', 'y', 'yes', 'yeah', 'yup',
              'certainly', 'uh-huh']
    _config = None
    _section_name = None

    def __init__(self, section_name, filepath=None):
        self._section_name = section_name
        self._config = ConfigParser.ConfigParser()
        if filepath is not None:
            self._config.read(self._ensure_abs_path(filepath))
        else:
            self._config.read(_default_config)

    def force_reload_config(self, path):
        _path = self._ensure_abs_path(path)
        self._config.read(_path)

    @staticmethod
    def _ensure_abs_path(path):
        if path.startswith('~'):
            path = os.path.expanduser(path)
        else:
            path = path

        # make sure it is absolute
        if not os.path.isabs(path):
            return os.path.join(pkg_dir, path)
        else:
            return path

    def _ensure_boolean(self, _value):
        if _value.lower() in self._truth:
            return True
        else:
            return False

    def get_value(self, key, value_type=None):
        if value_type is not None:
            return value_type(self._config.get(self._section_name, key))
        else:
            return self._config.get(self._section_name, key)


class SweeperConfig(ConfigFileBase):
    def __init__(self, filename):
        super(SweeperConfig, self).__init__(
            "SweeperConfig",
            filepath=filename
        )

    def get_logfile_path(self):
        _path = self.get_value('logfile')
        return self._ensure_abs_path(_path)

sweeper = SweeperConfig(_default_config)
