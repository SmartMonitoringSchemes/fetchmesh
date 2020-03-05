import datetime as dt
from enum import Enum
from pathlib import Path

import click
import dateparser as dp


class DateParseParamType(click.ParamType):
    name = "DATE"

    def convert(self, value, param, ctx):
        return dp.parse(value)


class EnumChoice(click.ParamType):
    def __init__(self, enum, type_):
        self.enum = enum
        self.type_ = type_

    def convert(self, value, param, ctx):
        return self.enum(self.type_(value))

    @property
    def name(self):
        vals = [str(v.value) for v in self.enum.__members__.values()]
        return f"[{'|'.join(vals)}]"


class PathParamType(click.ParamType):
    name = "PATH"

    def convert(self, value, param, ctx):
        return Path(value)


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
