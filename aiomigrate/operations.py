"""Migration operations: migrate up/down, redo migration, get status."""

import typing

from aiomigrate import config
from aiomigrate import db
from aiomigrate import driver
from aiomigrate import entities
from aiomigrate import files_loader


async def migrate(cfg: config.Config, options: entities.MigrationOptions) -> int:
    """Apply/undo database migrations."""
    db_driver = driver.get_driver(cfg.dsn)
    loader = files_loader.PackageLoader()
    known_migrations = loader.load(cfg.files_source)
    async with db.connection(db_driver, cfg) as conn:
        number = await db.do_migrate(conn, cfg, options, known_migrations)
    return number


async def migration_status(cfg: config.Config) -> typing.List[entities.Migration]:
    """Get migration status."""
    db_driver = driver.get_driver(cfg.dsn)
    loader = files_loader.PackageLoader()
    known_migrations = loader.load(cfg.files_source)
    async with db.connection(db_driver, cfg) as conn:
        migrations = await db.list_migrations(conn, cfg, known_migrations)
    return migrations
