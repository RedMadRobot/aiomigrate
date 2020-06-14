"""Console migrate tool."""

import argparse
import logging.config
import sys
import typing

from aiomigrate import config
from aiomigrate import entities
from aiomigrate import operations


def render_2col_table(
        header: typing.Tuple[str, str],
        rows: typing.List[typing.Tuple[str, str]],
) -> None:
    """Render 2 column table."""
    col0_width = max(len(header[0]) + 2, max(len(row[0]) for row in rows) + 2)
    col1_width = max(len(header[1]) + 2, max(len(row[1]) for row in rows) + 2)
    h0_after_spaces = ' ' * ((col0_width - len(header[0])) // 2)
    h0_before_spaces = ' ' * (col0_width - len(header[0]) - len(h0_after_spaces))
    h1_after_spaces = ' ' * ((col1_width - len(header[1])) // 2)
    h1_before_spaces = ' ' * (col1_width - len(header[1]) - len(h1_after_spaces))
    sys.stdout.write(
        '+{col0_minuses}+{col1_minuses}+\n'.format(
            col0_minuses='-' * col0_width,
            col1_minuses='-' * col1_width,
        ),
    )
    sys.stdout.write(
        (
            '|{h0_before_spaces}{h0}{h0_after_spaces}|'
            '{h1_before_spaces}{h1}{h1_after_spaces}|\n'
        ).format(
            h0_before_spaces=h0_before_spaces,
            h0=header[0],
            h0_after_spaces=h0_after_spaces,
            h1_before_spaces=h1_before_spaces,
            h1=header[1],
            h1_after_spaces=h1_after_spaces,
        ),
    )
    sys.stdout.write(
        '+{col0_minuses}+{col1_minuses}+\n'.format(
            col0_minuses='-' * col0_width,
            col1_minuses='-' * col1_width,
        ),
    )
    for row in rows:
        sys.stdout.write(
            '| {cell0}{cell0_after_spaces}| {cell1}{cell1_after_spaces}|\n'.format(
                cell0=row[0],
                cell0_after_spaces=' ' * (col0_width - len(row[0]) - 1),
                cell1=row[1],
                cell1_after_spaces=' ' * (col1_width - len(row[1]) - 1),
            ),
        )
    sys.stdout.write(
        '+{col0_minuses}+{col1_minuses}+\n'.format(
            col0_minuses='-' * col0_width,
            col1_minuses='-' * col1_width,
        ),
    )


async def migrate_up(
        args: argparse.Namespace,
) -> None:
    """Handle db up command."""
    cfg = config.Config(
        dsn='postgres://localhost/mydb',
        table='migrations',
        files_loader='package',
        files_source='mypkg:db/migrations',
    )
    options = entities.MigrationOptions(
        direction=entities.Direction.UP,
        limit=args.limit if args.limit != 0 else None,
    )
    number = await operations.migrate(cfg, options)
    sys.stdout.write('Applied {} migrations\n'.format(number))


async def migrate_down(
        args: argparse.Namespace,
) -> None:
    """Handle db down command."""
    cfg = config.Config(
        dsn='postgres://localhost/mydb',
        table='migrations',
        files_loader='package',
        files_source='mypkg:db/migrations',
    )
    options = entities.MigrationOptions(
        direction=entities.Direction.DOWN,
        limit=args.limit if args.limit != 0 else None,
    )
    number = await operations.migrate(cfg, options)
    sys.stdout.write('Applied {} migrations\n'.format(number))


async def migration_status(
        _args: argparse.Namespace,
) -> None:
    """Handle db status command."""
    cfg = config.Config(
        dsn='postgres://localhost/mydb',
        table='migrations',
        files_loader='package',
        files_source='mypkg:db/migrations',
    )
    migrations = await operations.migration_status(cfg)
    rows = []
    for migration in migrations:
        if migration.status == entities.MigrationStatus.APPLIED:
            rows.append((migration.name, str(migration.apply_time)))
        else:
            rows.append((migration.name, 'no'))
    render_2col_table(('MIGRATION', 'APPLIED'), rows)


def get_parser() -> argparse.ArgumentParser:
    """Get argument parser."""
    parser = argparse.ArgumentParser(
        description='Manage PostgreSQL database migrations',
    )
    subparsers = parser.add_subparsers(
        dest='func',
        required=True,
        help='sub-command help',
    )
    parser_down = subparsers.add_parser(
        'down',
        help='Undo a database migration',
    )
    parser_down.add_argument(
        '--limit',
        type=int,
        default=1,
        metavar='LIMIT',
        help='Limit the number of migrations (0 = unlimited, default is 1).',
    )
    parser_down.set_defaults(func=migrate_down)
    parser_up = subparsers.add_parser(
        'up',
        help='Migrates the database to the most recent version available',
    )
    parser_up.add_argument(
        '--limit',
        type=int,
        default=0,
        metavar='LIMIT',
        help='Limit the number of migrations (0 = unlimited, default is 0).',
    )
    parser_up.set_defaults(func=migrate_up)
    parser_status = subparsers.add_parser(
        'status',
        help='Show migration status',
    )
    parser_status.set_defaults(func=migration_status)
    return parser


def parse_args(argv: typing.List[str]) -> argparse.Namespace:
    """Parse arguments."""
    parser = get_parser()
    return parser.parse_args(argv[1:])


def main() -> None:
    """Run console tool."""
    log_level = 'INFO'
    log_config: typing.Dict[str, typing.Any] = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s [%(levelname)s] (%(name)s) %(message)s',
                'datefmt': '%Y-%m-%dT%H:%M:%S%Z',
            },
        },
        'handlers': {
            'stdout': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',
                'level': log_level,
                'formatter': 'simple',
            },
        },
        'loggers': {
            '': {
                'handlers': ['stdout'],
                'level': log_level,
            },
        },
    }
    logging.config.dictConfig(log_config)


if __name__ == '__main__':
    main()
