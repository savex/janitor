import argparse
import os
import sys
import traceback

import janitor
from common import logger, logger_cli
from janitor.sweeper import Sweeper

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
        "--bash-action",
        default=None,
        help="Generate bash commands for 'action'. Options: 'list', 'sweep'"
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
        help="Action profile to execute"
    )

    args = parser.parse_args()

    # Some info on current config values
    logger_cli.debug(
        "Current working folder is: {}".format(
            os.path.abspath(
                os.curdir
            )
        )
    )
    logger_cli.debug("Using profile: '{}'\n".format(args.profile))

    # Check if profile exists
    try:
        os.stat(args.profile)
    except os.error:
        logger_cli.error("Profile '{}' not found".format(args.profile))
        sys.exit(1)

    # Load profile
    sweep = Sweeper(
        args.filter_regex,
        args.profile,
        args.bash_action
    )
    logger_cli.info("### {}".format(sweep.banner))
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
        # check if section is present in profile
        if not sweep.is_section_present(_section):
            logger_cli.error("# !!! Section'{}' not present in '{}'".format(
                _section,
                args.profile
            ))
            continue

        # Execute as usual
        logger_cli.info("\n### {}".format(_section))
        # Collect all data
        rc = sweep.list_action(_section)
        if rc != 0:
            logger_cli.error("\t({}) '{}'\n\tERROR: {}".format(
                rc,
                sweep.get_section_list_cmd(_section),
                sweep.get_section_list_error(_section)
            ))
        elif args.bash_action not in ['list']:
            _all_output = sweep.get_section_output(_section)
            _filtered_output = sweep.get_section_filtered_output(_section)
            _count = len(_filtered_output)
            logger_cli.info("# listed {}, matched {}.".format(
                len(_all_output),
                _count
            ))

            # Log collected data stats
            if args.stat_only:
                continue
            else:
                if args.sweep:
                    # Do sweep actions
                    rc = sweep.sweep_action(
                        _section
                    )

                _count -= 1

    logger_cli.info("\nDone")
    return

# Entry
if __name__ == '__main__':
    try:
        sweeper_cli()
    except janitor.utils.exception.SweeperException as e:
        for line in traceback.format_exc().splitlines():
            logger.error("{}".format(line))
        logger_cli.error("ERROR: {}".format(e))
        sys.exit(1)
    sys.exit(0)
