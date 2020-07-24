import logging

import click

from .commands.csv import csv
from .commands.describe import describe
from .commands.fetch import fetch
from .commands.unpack import unpack


@click.group(context_settings=dict(max_content_width=120))
@click.option(
    "--debug",
    default=False,
    is_flag=True,
    show_default=True,
    help="Set the logging level to DEBUG",
)
def cli(debug):
    """
    \b
    fetchmesh is a Python library and a command line utility to facilitate
    the use of the RIPE Atlas anchoring mesh measurements.

    \b
    The documentation and the source code are available at
    https://github.com/maxmouchet/fetchmesh.
    """
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            datefmt="%Y/%m/%d %H:%M:%S",
            format="%(asctime)s %(levelname)s %(process)d %(name)s %(message)s",
        )


cli.add_command(csv)
cli.add_command(describe)
cli.add_command(fetch)
cli.add_command(unpack)
