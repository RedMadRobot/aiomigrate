"""Database helpers."""

import contextlib
import datetime
import typing

from aiomigrate import config
from aiomigrate import driver
from aiomigrate import entities
from aiomigrate import plan


@contextlib.asynccontextmanager
async def connection(
        db_driver: driver.Driver,
        cfg: config.Config,
) -> typing.AsyncIterator[driver.Connection]:
    """Open connection context."""
    pool = await db_driver.create_pool(cfg.dsn)
    async with pool.acquire() as conn:
        yield conn
    await pool.close()


async def ensure_migration_table(
        conn: driver.Connection,
        table: str,
) -> None:
    """Create migration table if not exists."""
    query = await conn.sql_create_migration_table(table)
    await conn.execute(query)


async def get_migration_records(
        conn: driver.Connection,
        table: str,
) -> typing.List[entities.Migration]:
    """Read migration table from database and returns list of migrations."""
    query = await conn.sql_list_migrations(table)
    records = await conn.fetch(query)
    return [
        entities.Migration(
            name=record['name'],
            status=entities.MigrationStatus.APPLIED,
            apply_time=record['apply_time'].astimezone(datetime.timezone.utc),
        )
        for record in records
    ]


async def mark_migration(
        conn: driver.Connection,
        table: str,
        direction: entities.Direction,
        migration: entities.Migration,
) -> None:
    """Mark migration as applied or rolled back."""
    if direction == entities.Direction.UP:
        query = await conn.sql_apply_migration(table)
    else:  # direction == db.Direction.DOWN
        query = await conn.sql_rollback_migration(table)
    await conn.execute(query, (migration.name,))


async def do_migrate(
        conn: driver.Connection,
        cfg: config.Config,
        options: entities.MigrationOptions,
        known_migrations: typing.List[entities.Migration],
) -> int:
    """Migrate or rollback and return number of applied/rolled back migrations."""
    await ensure_migration_table(conn, cfg.table)
    applied_migrations = await get_migration_records(conn, cfg.table)
    to_apply = plan.plan_migrations(
        known_migrations,
        applied_migrations,
        options.direction,
        options.limit,
    )
    for migration in to_apply:
        if options.direction == entities.Direction.UP:
            statements = migration.up_statements
            disable_transactions = migration.disable_transactions_up
        else:  # direction == db.Direction.DOWN:
            statements = migration.down_statements
            disable_transactions = migration.disable_transactions_down
        if not disable_transactions:
            async with conn.transaction():
                for statement in statements:
                    await conn.execute(statement)
                await mark_migration(conn, cfg.table, options.direction, migration)
        else:
            for statement in statements:
                await conn.execute(statement)
            await mark_migration(conn, cfg.table, options.direction, migration)
    return len(to_apply)


async def list_migrations(
        conn: driver.Connection,
        cfg: config.Config,
        known_migrations: typing.List[entities.Migration],
) -> typing.List[entities.Migration]:
    """List applied migrations."""
    await ensure_migration_table(conn, cfg.table)
    applied_migrations = await get_migration_records(conn, cfg.table)
    return plan.list_migrations(known_migrations, applied_migrations)
