import datetime as dt
from enum import Enum

import click


# TODO: Don't print options (only args)
def format_args(args):
    s = ""
    for name, value in args.items():
        if value is None:
            continue
        if isinstance(value, bool) and not value:
            continue
        name = name.replace("_", "-")
        s += f" --{name}"
        if isinstance(value, bool) and value:
            continue
        if isinstance(value, Enum):
            value = value.value
        if isinstance(value, dt.datetime):
            value = value.isoformat()
        s += f" {value}"
    return s[1:]


def bprint(key, value):
    click.secho(f"{key}: ", bold=True, nl=False)
    click.secho(str(value))
