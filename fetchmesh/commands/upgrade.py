from subprocess import run

import click


@click.command()
def upgrade():
    """
    Upgrade fetchmesh to the latest version from GitHub.

    \b
    Please make sure that your SSH key associated to GitHub is present in your SSH agent.
    See https://docs.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent.
    """
    url = "git+ssh://git@github.com/SmartMonitoringSchemes/fetchmesh.git"
    run(["pip", "install", "--upgrade", url], check=True)
