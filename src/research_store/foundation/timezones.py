from __future__ import annotations

from functools import cache
from importlib.metadata import version
from importlib.resources import files
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@cache
def pinned_zoneinfo(name: str) -> ZoneInfo:
    """Load an IANA zone from the project's pinned tzdata distribution."""

    resource = files("tzdata.zoneinfo")
    for component in name.split("/"):
        resource = resource.joinpath(component)
    if not resource.is_file():
        raise ZoneInfoNotFoundError(name)
    with resource.open("rb") as stream:
        return ZoneInfo.from_file(stream, key=name)


def timezone_data_version() -> str:
    return version("tzdata")
