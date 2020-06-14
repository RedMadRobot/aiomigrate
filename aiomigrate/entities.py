"""Common project enums and structures."""

import dataclasses
import datetime
import enum
import typing


class MigrationStatus(enum.Enum):
    """Migration apply status."""

    UNAPPLIED = enum.auto()
    APPLIED = enum.auto()


@dataclasses.dataclass(frozen=True)
class Migration:
    """Migration."""

    name: str
    status: MigrationStatus
    apply_time: typing.Optional[datetime.datetime] = None
    up_statements: typing.List[str] = dataclasses.field(default_factory=list)
    down_statements: typing.List[str] = dataclasses.field(default_factory=list)
    disable_transactions_up: bool = False
    disable_transactions_down: bool = False


class Direction(enum.Enum):
    """Migration direction."""

    UP = enum.auto()
    DOWN = enum.auto()


@dataclasses.dataclass(frozen=True)
class MigrationOptions:
    """Migration options."""

    direction: Direction
    limit: typing.Optional[int]
