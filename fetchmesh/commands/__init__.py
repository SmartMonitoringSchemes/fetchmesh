import logging

import click

from .csv import csv
from .describe import describe
from .fetch import fetch
from .unpack import unpack
from .upgrade import upgrade


@click.group(context_settings=dict(max_content_width=120))
@click.option(
    "--debug",
    default=False,
    is_flag=True,
    show_default=True,
    help="Set the logging level to DEBUG",
)
def main(debug):
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


main.add_command(csv)
main.add_command(describe)
main.add_command(fetch)
main.add_command(unpack)
main.add_command(upgrade)
