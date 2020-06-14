"""Plan migrations."""

import typing

from aiomigrate import entities


class MigrationPlanError(Exception):
    """Error planning migrations."""


class UnknownAppliedMigration(MigrationPlanError):
    """Unknown applied migrations found."""

    def __init__(self, migrations: typing.FrozenSet[str]) -> None:
        """Initialize the exception."""
        message = 'Unknown applied migrations found: {}'.format(
            migrations,
        )
        super(UnknownAppliedMigration, self).__init__(message)
        self.message = message
        self.migrations = migrations


class InvalidMigrationApplyOrder(MigrationPlanError):
    """Migrations are applied at an invalid order."""

    def __init__(self, last_applied: str, first_unapplied: str) -> None:
        """Initialize the exception."""
        message = (
            'Found migrations that are applied at an invalid order: '
            'migration {last_applied} was applied before migration '
            '{first_unapplied}'
        ).format(
            last_applied=last_applied,
            first_unapplied=first_unapplied,
        )
        super(InvalidMigrationApplyOrder, self).__init__(message)
        self.message = message
        self.last_applied = last_applied
        self.first_unapplied = first_unapplied


def plan_migrations(
        known_migrations: typing.List[entities.Migration],
        applied_migrations: typing.List[entities.Migration],
        direction: entities.Direction,
        limit: typing.Optional[int] = None,
) -> typing.List[entities.Migration]:
    """Plan migrations to apply/rollback."""
    if not applied_migrations:
        if direction == entities.Direction.UP:
            return sorted(known_migrations, key=lambda m: m.name)[:limit]
        # direction == Direction.DOWN
        return []
    known_mapping = {m.name: m for m in known_migrations}
    known_set = frozenset(known_mapping)
    applied_set = frozenset(m.name for m in applied_migrations)
    unknown_applied_migrations = applied_set - known_set
    if unknown_applied_migrations:
        raise UnknownAppliedMigration(unknown_applied_migrations)
    unapplied_migrations = known_set - applied_set
    last_applied = max(applied_set)
    if unapplied_migrations:
        first_unapplied = min(unapplied_migrations)
        if last_applied > first_unapplied:
            raise InvalidMigrationApplyOrder(
                last_applied=last_applied,
                first_unapplied=first_unapplied,
            )
    if direction == entities.Direction.UP:
        # return unapplied_sorted migrations
        ret = sorted(
            (known_mapping[name] for name in unapplied_migrations),
            key=lambda m: m.name,
        )
    else:  # direction == Direction.DOWN
        ret = sorted(
            (known_mapping[name] for name in applied_set),
            key=lambda m: m.name,
            reverse=True,
        )
    if limit is not None:
        ret = ret[:limit]
    return ret


def list_migrations(
        known_migrations: typing.List[entities.Migration],
        applied_migrations: typing.List[entities.Migration],
) -> typing.List[entities.Migration]:
    """List migrations with their apply statuses."""
    unapplied_sorted = plan_migrations(
        known_migrations,
        applied_migrations,
        entities.Direction.UP,
    )
    applied_sorted = sorted(applied_migrations, key=lambda m: m.name)
    return applied_sorted + unapplied_sorted
