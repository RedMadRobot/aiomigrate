"""PostgreSQL driver."""

import contextlib
import typing

import asyncpg
import asyncpg.pool

from aiomigrate import driver

# pylint: disable=invalid-overridden-method


def quote_identifier(identifier: str) -> str:
    """Quote identifier and make it SQL-safe.

    Acts like PostgreSQL's `quote_ident` built-in function, but always wraps
    identifier in a double quotes
    """
    return '"{}"'.format(identifier.replace('"', '""'))


def safe_sql_format(template: str, params: typing.Dict[str, str]) -> str:
    """Safe sql query format."""
    safe_dict = {key: quote_identifier(val) for key, val in params.items()}
    return template.format(safe_dict)


class Connection(driver.Connection):
    """Postgres connection."""

    def __init__(self, real_connection: asyncpg.Connection):
        """Initialize connection."""
        self.real_connection = real_connection

    @contextlib.asynccontextmanager
    async def transaction(self) -> typing.AsyncIterator[None]:
        """Start new transaction."""
        async with self.real_connection.transaction():
            yield

    async def fetch(
            self,
            query: str,
            args: typing.Optional[typing.Tuple[typing.Any]] = None,
    ) -> typing.List[typing.Mapping[str, typing.Any]]:
        """Execute an SQL-query and return the results as a list of records."""
        args_ = args if args is not None else tuple()
        records = await self.real_connection.fetch(query, *args_)
        return [dict(record) for record in records]

    async def execute(
            self,
            query: str,
            args: typing.Optional[typing.Tuple[typing.Any]] = None,
    ) -> None:
        """Run a query."""
        args_ = args if args is not None else tuple()
        await self.real_connection.execute(query, *args_)

    async def sql_create_migration_table(self, migration_table_name: str) -> str:
        """Get create migration table SQL query."""
        return safe_sql_format(
            (
                "CREATE TABLE IF NOT EXISTS {migration_table_name} ("
                "name text PRIMARY KEY, "
                "apply_time timestamp with time zone NOT NULL DEFAULT now()"
                ")"
            ),
            {'migration_table_name': migration_table_name},
        )

    async def sql_list_migrations(self, migration_table_name: str) -> str:
        """Get list migrations SQL query."""
        return safe_sql_format(
            "SELECT name, apply_time FROM {migration_table_name} ORDER BY name",
            {'migration_table_name': migration_table_name},
        )

    async def sql_apply_migration(self, migration_table_name: str) -> str:
        """Get apply migration SQL query."""
        return safe_sql_format(
            "INSERT INTO {migration_table_name} (name) VALUES ($1)",
            {'migration_table_name': migration_table_name},
        )

    async def sql_rollback_migration(self, migration_table_name: str) -> str:
        """Get rollback migration SQL query."""
        return safe_sql_format(
            "DELETE FROM {migration_table_name} WHERE name = $1",
            {'migration_table_name': migration_table_name},
        )


class Pool(driver.Pool):
    """Postgres connection pool."""

    def __init__(self, real_pool: asyncpg.pool.Pool) -> None:
        """Initialize pool."""
        self.real_pool = real_pool

    @contextlib.asynccontextmanager
    async def acquire(self) -> typing.AsyncIterator[Connection]:
        """Acquire connection from the pool."""
        async with self.real_pool.acquire() as conn:
            yield conn

    async def close(self) -> None:
        """Close connection pool."""
        await self.real_pool.close()


class Driver(driver.Driver):
    """Postgres driver."""

    async def create_pool(self, dsn: str) -> Pool:
        """Create new connection pool."""
        return Pool(await asyncpg.create_pool(dsn))
