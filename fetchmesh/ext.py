import datetime as dt
import sys
from enum import Enum

import click
import rich
from click import Option


# TODO: Don't print options (only args)
def format_args(args, command):
    tokens = []
    for param in command.params:
        value = args.get(param.name)
        # Print nothing if:
        # 1) Param is optional and unspecified
        if value is None:
            continue
        # 2) Param is a flag and is false (i.e. is not set)
        if isinstance(value, bool) and not value:
            continue
        # Print the param name only if:
        # 1) It is an option (and not an argument)
        if isinstance(param, Option):
            tokens.append(param.opts[0])
        # Do not print the value if:
        # 1) Param is a flag
        if isinstance(value, bool) and value:
            continue
        # Special values
        if isinstance(value, Enum):
            value = value.value
        if isinstance(value, dt.datetime):
            value = value.isoformat()
        tokens.append(str(value))
    return " ".join(tokens)


def print_args(args, command):
    print_kv("Args", format_args(args, command))


def print_kv(key, value):
    rich.print(f"[bold]{key}:[/bold] {value}")
