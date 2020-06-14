"""Config."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class Config:
    """Config."""

    dsn: str
    table: str
    files_loader: str
    files_source: str
