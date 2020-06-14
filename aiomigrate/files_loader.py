"""Migration files loader."""

import abc
import io
import os.path
import pkgutil
import typing

import pkg_resources

from aiomigrate import entities
from aiomigrate import parser


class Loader(metaclass=abc.ABCMeta):
    """Migration files loader base."""

    @abc.abstractmethod
    def load(self, source: str) -> typing.List[entities.Migration]:
        """Load migrations from the given source."""


class PackageLoader(Loader):
    """Package migration files loader."""

    def load(self, source: str) -> typing.List[entities.Migration]:
        """Load migrations from the given package and dir.

        Source string should be formatted as "<package>:<dir>", e.g.:

        - mypkg:db/migrations
        - otherpkg:migrations
        - requirement:db/migrations
        """
        package_or_requirement, migration_dir = source.split(':', 1)
        ret: typing.List[entities.Migration] = []
        resource_names = pkg_resources.resource_listdir(
            package_or_requirement,
            migration_dir,
        )
        for name in resource_names:
            full_resource_name = os.path.join(migration_dir, name)
            content = pkgutil.get_data(package_or_requirement, full_resource_name)
            if content is not None:
                ret.append(parser.parse_migration(name, io.StringIO(content.decode())))
        return ret
