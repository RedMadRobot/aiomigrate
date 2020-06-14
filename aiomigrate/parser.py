"""Migration files parser."""

import dataclasses
import typing

from aiomigrate import entities

SQL_CMD_PREFIX = '-- +migrate '
OPTION_NO_TRANSACTION = 'notransaction'


class MigrationParseError(Exception):
    """Error parsing SQL migration file."""


@dataclasses.dataclass(frozen=True)
class _MigrateCommand:
    """Migration command."""

    command: str
    options: typing.List[str]


def _parse_command(line: str) -> _MigrateCommand:
    parts = line.replace(SQL_CMD_PREFIX, '', 1).split()
    if not parts:
        raise MigrationParseError('incomplete migration command')
    return _MigrateCommand(command=parts[0], options=parts[1:])


def _ends_with_semicolon(line: str) -> bool:
    before_comment = line.split('--', maxsplit=1)[0]
    return before_comment.rstrip().endswith(';')


def _raise_err_no_terminator() -> None:
    raise MigrationParseError(
        "The last statement must be ended by a semicolon or "
        "'-- +migrate StatementEnd' marker. "
        "See https://github.com/rubenv/sql-migrate for details.",
    )


@dataclasses.dataclass
class _ParseState:
    """Migration parse state (mutable)."""

    buf: str
    statement_ended: bool
    ignore_semicolons: bool
    current_direction: typing.Optional[entities.Direction]
    parsed_migration: entities.Migration


def _interpret_command(parse_state: _ParseState, cmd: _MigrateCommand) -> None:
    """Apply command to parse state."""
    if cmd.command == 'Up':
        if parse_state.buf.strip():
            _raise_err_no_terminator()
        parse_state.buf = ''
        parse_state.current_direction = entities.Direction.UP
        if OPTION_NO_TRANSACTION in cmd.options:
            parse_state.parsed_migration = dataclasses.replace(
                parse_state.parsed_migration,
                disable_transactions_up=True,
            )
    elif cmd.command == 'Down':
        if parse_state.buf.strip():
            _raise_err_no_terminator()
        parse_state.buf = ''
        parse_state.current_direction = entities.Direction.DOWN
        if OPTION_NO_TRANSACTION in cmd.options:
            parse_state.parsed_migration = dataclasses.replace(
                parse_state.parsed_migration,
                disable_transactions_down=True,
            )
    elif cmd.command == 'StatementBegin':
        if parse_state.current_direction is not None:
            parse_state.ignore_semicolons = True
    elif cmd.command == 'StatementEnd':
        if parse_state.current_direction is not None:
            parse_state.statement_ended = parse_state.ignore_semicolons
            parse_state.ignore_semicolons = False
    else:
        raise MigrationParseError(
            'Invalid command: {}'.format(cmd.command),
        )


def parse_migration(name: str, content: typing.TextIO) -> entities.Migration:
    """Parse sql migration file."""
    parse_state = _ParseState(
        buf='',
        statement_ended=False,
        ignore_semicolons=False,
        current_direction=None,
        parsed_migration=entities.Migration(
            name=name,
            status=entities.MigrationStatus.UNAPPLIED,
        ),
    )
    for line in content:
        line = line.rstrip('\r\n')
        # ignore comment except beginning with '-- +'
        if line.startswith('-- ') and not line.startswith('-- +'):
            continue

        # handle any db-specific commands
        if line.startswith(SQL_CMD_PREFIX):
            cmd = _parse_command(line)
            _interpret_command(parse_state, cmd)

        if parse_state.current_direction is None:
            continue

        if not line.startswith('-- +'):
            parse_state.buf += line + '\n'

        # Wrap up the two supported cases: 1) basic with semicolon;
        # 2) psql statement
        # Lines that end with semicolon that are in a statement block
        # do not conclude statement.
        if (
                (
                    not parse_state.ignore_semicolons and
                    _ends_with_semicolon(line)
                ) or
                parse_state.statement_ended
        ):
            parse_state.statement_ended = False
            if parse_state.current_direction == entities.Direction.UP:
                parse_state.parsed_migration.up_statements.append(
                    parse_state.buf,
                )
            else:  # current_direction == Direction.DOWN:
                parse_state.parsed_migration.down_statements.append(
                    parse_state.buf,
                )
            parse_state.buf = ''

    # diagnose likely migration script errors
    if parse_state.ignore_semicolons:
        raise MigrationParseError(
            "saw '-- +migrate StatementBegin' "
            "with no matching '-- +migrate StatementEnd'",
        )

    if parse_state.current_direction is None:
        raise MigrationParseError(
            "no Up/Down annotations found, so no statements were executed. "
            "See https://github.com/rubenv/sql-migrate for details.",
        )

    # allow comment without sql instruction. Example:
    # -- +migrate Down
    # -- nothing to downgrade!
    if parse_state.buf.strip() and not parse_state.buf.startswith('-- +'):
        _raise_err_no_terminator()

    return parse_state.parsed_migration
