import os
import sys
import argparse

from common import logger, logger_cli
from sweeper import Sweeper

pkg_dir = os.path.dirname(__file__)
pkg_dir = os.path.normpath(pkg_dir)


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('Error: {0}\n\n'.format(message))
        self.print_help()
        sys.exit(2)


def help_message():
    print"Please, execute this tool with a correct option:\n" \
         " 'stat' show count of objects about to clean \n" \
         " 'clean' do the cleaning \n"

    return


# Main
def sweeper_cli():
    _title = "Janitor:Sweeper CLI util"
    _cmd = "sweeper"
    parser = MyParser(prog=_cmd)

    logger_cli.info(_title)
    logger.info("=========> Sweep execution started")

    # arguments
    parser.add_argument(
        "-l",
        "--list-sections",
        action="store_true", default=False,
        help="List sections from the profile, preserve order of execution"
    )

    parser.add_argument(
        "-s",
        "--stat-only",
        action="store_true", default=False,
        help="List objects only, show counts next to each script entry"
    )

    parser.add_argument(
        "--sweep",
        action="store_true", default=False,
        help="Do sweep action of all objects listed"
    )

    parser.add_argument(
        "-f",
        "--filter-regex",
        default=None, help="Overide profile default filter"
    )

    parser.add_argument(
        "--section",
        default=None, help="Execute actions from specific section of a profile"
    )

    parser.add_argument(
        'profile',
        help="Cleaning profile to execute"
    )

    args = parser.parse_args()

    # Load profile
    sweep = Sweeper(args.filter_regex, args.profile)

    _sections = []

    if args.list_sections:
        # only list sections
        logger_cli.info("Sections available in profile '{}'".format(
            args.profile
        ))
        for section in sweep.sections_list:
            logger_cli.info("# {}".format(section))
    elif args.section is not None:
        _sections = [args.section]
    else:
        _sections = sweep.sweep_items_list

    # do main flow
    while len(_sections) > 0:
        _section = _sections.pop(0)
        logger_cli.info("\n# {}".format(_section))
        # Collect all data
        rc = sweep.list_action(_section)
        if rc != 0:
            logger_cli.error("\t({}) '{}'\n\tERROR: {}".format(
                rc,
                sweep.get_section_list_cmd(_section),
                sweep.get_section_list_error(_section)
            ))
        else:
            _all_data = sweep.get_section_data(_section)
            # Filter data, override if corresponding parameter is set
            sweep.filter_action(_section)
            _filtered_data = sweep.get_section_data(_section)
            _count = len(_filtered_data)
            logger_cli.info("# listed {}, matched {}.".format(
                len(_all_data),
                _count
            ))

            # Log collected data stats
            if args.stat_only:
                break
            else:
                for _data_item in _filtered_data:
                    logger_cli.info("# {}: {}".format(
                        _count,
                        _data_item
                    ))

                    if args.sweep:
                        # Do sweep actions
                        rc = sweep.sweep_action(_section, item=_data_item)
                        if rc != 0:
                            logger_cli.error("\t({}) '{}'\n\tERROR: {}".format(
                                rc,
                                sweep.get_section_sweep_cmd(
                                    _section,
                                    _data_item
                                ),
                                sweep.get_section_sweep_error(
                                    _section,
                                    _data_item
                                )
                            ))
                        else:
                            logger_cli.info("{}".format(
                                sweep.get_section_sweep_output(
                                    _section,
                                    _data_item
                                )
                            ))
                    _count -= 1

    logger_cli.info("\nDone")
    return

# Entry
if __name__ == '__main__':
    sweeper_cli()
    sys.exit(0)
