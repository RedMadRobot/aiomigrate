"""Database driver interface."""

import abc
import dataclasses
import typing

import pkg_resources

# pylint: disable=too-few-public-methods


class Connection(metaclass=abc.ABCMeta):
    """Database connection."""

    @abc.abstractmethod
    def transaction(self) -> typing.AsyncContextManager[None]:
        """Start new transaction."""

    @abc.abstractmethod
    async def fetch(
            self,
            query: str,
            args: typing.Optional[typing.Tuple[typing.Any]] = None,
    ) -> typing.List[typing.Mapping[str, typing.Any]]:
        """Run a query and return the results as a list of records."""

    @abc.abstractmethod
    async def execute(
            self,
            query: str,
            args: typing.Optional[typing.Tuple[typing.Any]] = None,
    ) -> None:
        """Run a query."""

    @abc.abstractmethod
    async def sql_create_migration_table(self, migration_table_name: str) -> str:
        """Get create migration table SQL query."""

    @abc.abstractmethod
    async def sql_list_migrations(self, migration_table_name: str) -> str:
        """Get list migrations SQL query."""

    @abc.abstractmethod
    async def sql_apply_migration(self, migration_table_name: str) -> str:
        """Get apply migration SQL query."""

    @abc.abstractmethod
    async def sql_rollback_migration(self, migration_table_name: str) -> str:
        """Get rollback migration SQL query."""


class Pool(metaclass=abc.ABCMeta):
    """Connection pool."""

    @abc.abstractmethod
    def acquire(self) -> typing.AsyncContextManager[Connection]:
        """Acquire connection from the pool."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Close connection pool."""


class Driver(metaclass=abc.ABCMeta):
    """Database driver."""

    @abc.abstractmethod
    async def create_pool(self, dsn: str) -> Pool:
        """Create new connection pool."""


@dataclasses.dataclass(frozen=True)
class DriverExtensionMetadata:
    """Metadata of driver extension provided via entry point."""

    name: str
    distribution: str
    object_location: str


def get_driver(dsn: str) -> Driver:
    """Get driver by the given database DSN."""
    lookup_driver_name = dsn.split('://', 1)[0]
    available_drivers: typing.List[DriverExtensionMetadata] = []
    for entry_point in pkg_resources.iter_entry_points('aiomigrate.drivers'):
        metadata = DriverExtensionMetadata(
            name=entry_point.name,
            distribution=str(entry_point.dist),
            object_location=(
                '{}:{}'.format(entry_point.module_name, '.'.join(entry_point.attrs))
                if entry_point.attrs
                else entry_point.module_name
            ),
        )
        available_drivers.append(metadata)
        if entry_point.name == lookup_driver_name:
            found_driver_cls = entry_point.resolve()
            if not issubclass(found_driver_cls, Driver):
                raise RuntimeError((
                    '{metadata} found for database dsn \'{safe_dsn}\' is not a subclass '
                    'of Driver interface'
                ).format(
                    metadata=metadata,
                    safe_dsn=lookup_driver_name + '://',
                ))
            driver_instance: Driver = found_driver_cls()
            return driver_instance
    raise RuntimeError(
        (
            'No driver found for database dsn \'{safe_dsn}\', {known}'
        ).format(
            safe_dsn=lookup_driver_name + '://',
            known=(
                'available drivers are ' + ', '.join(
                    str(metadata)
                    for metadata in available_drivers
                )
                if available_drivers
                else 'no available drivers found at all'
            ),
        ),
    )
