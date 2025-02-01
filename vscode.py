#!/usr/bin/env python3.9

''' vscode: launch and manage VS Code HPC jobs '''

###############################################################################
#    _  _  ____     ___  __  ____  ____
#   / )( \/ ___)   / __)/  \(    \(  __)
#   \ \/ /\___ \  ( (__(  O )) D ( ) _)
#    \__/ (____/   \___)\__/(____/(____)
#
###############################################################################

import argparse
from random import choices
from string import hexdigits
import sys

from rich import print
from rich_argparse import RawDescriptionRichHelpFormatter

from vscode_manager import \
    __prog__, __version__, __status__, SESSION_STORE, R_VERSIONS
from vscode_manager.commands import vscode_start, vscode_stop, vscode_list


# argparse ####################################################################


commands = {}

parser = argparse.ArgumentParser(
    prog='vscode',
    description=r'''
|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
                    _  _  ____     ___  __  ____  ____
                   / )( \/ ___)   / __)/  \(    \(  __)
                   \ \/ /\___ \  ( (__(  O )) D ( ) _)
                    \__/ (____/   \___)\__/(____/(____)

                    launch and manage VS Code HPC jobs
''',
    formatter_class=RawDescriptionRichHelpFormatter,
    exit_on_error=False,
    allow_abbrev=False)
subparsers = parser.add_subparsers(
    title='commands',
    metavar='{command}',
    dest='command',
    required=True)

parser.add_argument(
    '-v', '--version',
    action='version', version=f'{__prog__} v{__version__} ({__status__})',
    help='show the program version and exit')
parser.add_argument(
    '-q', '--quiet', action='store_true',
    help='silence logging information')

#
# vscode start
#

commands['start'] = subparsers.add_parser(
    'start',
    aliases=['create', 'new'],
    help='start a new VS Code server',
    description='start a new VS Code server',
    formatter_class=RawDescriptionRichHelpFormatter)
# register the alias names as placeholders
commands['create'] = commands['new'] = commands['start']
# arguments
commands['start'].add_argument(
    'r_version', choices=R_VERSIONS,
    help='version of Python to launch as the default VS Code kernel')
commands['start'].add_argument(
    '-n', '--name', default='vscode_server', type=str,
    help='job name for the scheduler (default "%(default)s")')
commands['start'].add_argument(
    '-@', '--cpu', default=1, type=int,
    help='requested number of CPUs (default %(default)s)')
commands['start'].add_argument(
    '-m', '--mem', default=8, type=int,
    help='requested amount of RAM (GB, default %(default)s)')
commands['start'].add_argument(
    '-w', '--wallclock', dest='time', default=16, type=int,
    help='requested runtime (hrs, default %(default)s)')
commands['start'].add_argument(
    '-g', '--gpu', default=0, type=int,
    help='requested number of GPUs (default %(default)s)')
commands['start'].add_argument(  # hidden
    '-p', '--partition', default='int', type=str,
    choices=('int', 'cpu', 'hmem', 'gpu'),
    help=argparse.SUPPRESS)
commands['start'].add_argument(
    '-b', '--bind', type=str, default='',
    help='additional bind path/s using the singularity format \
    specification (src[:dest[:opts]])')
commands['start'].add_argument(  # hidden
    '-l', '--log', action='store_true',
    help=argparse.SUPPRESS)

#
# vscode stop
#

commands['stop'] = subparsers.add_parser(
    'stop',
    aliases=['delete', 'cancel', 'kill'],
    help='stop an existing VS Code server instance',
    description='stop an existing VS Code server instance',
    formatter_class=RawDescriptionRichHelpFormatter)
# register the alias names as placeholders
commands['delete'] = commands['cancel'] = commands['kill'] = commands['stop']
# arguments
commands['stop'].add_argument(
    'job', type=str, nargs='*',
    help='list of job number/s and/or name/s to kill')
commands['stop'].add_argument(
    '-a', '--all', action='store_true',
    help='stop all running VS Code instances')

#
# vscode list
#

commands['list'] = subparsers.add_parser(
    'list',
    aliases=['ls', 'show'],
    help='list running VS Code servers',
    description='list running VS Code servers',
    formatter_class=RawDescriptionRichHelpFormatter)
# register the alias names as placeholders
commands['ls'] = commands['show'] = commands['list']


# main ########################################################################


def main():

    # catch program name by itself
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    # catch command names by themselves
    if len(sys.argv) == 2 and \
            (sys.argv[1] in commands and
            sys.argv[1] not in ('list', 'ls', 'show')):
        commands[sys.argv[1]].print_help()
        sys.exit(0)

    # catch unknown commands and errors
    try:
        args = parser.parse_args()
    except argparse.ArgumentError:
        parser.print_help()
        sys.exit(1)

    # Run the relevant command from vscode_manager.commands
    if args.command in ('start', 'create'):
        vscode_start(args)
    elif args.command in ('stop', 'delete', 'cancel', 'kill'):
        vscode_stop(args)
    elif args.command in ('list', 'ls', 'show'):
        vscode_list(args)


if __name__ == '__main__':
    main()
