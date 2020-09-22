.. _Development:

Development
===========

fetchmesh is developed on GitHub in the `SmartMonitoringSchemes <https://github.com/SmartMonitoringSchemes>`_ organization.
It uses `poetry <https://github.com/python-poetry/poetry>`_ for dependency management and packaging.

Workflow
--------

A typical development workflow is as follows:

.. code:: bash

    git clone git@github.com:maxmouchet/fetchmesh
    cd fetchmesh/

    # `poetry install` is required only once.
    # If the pyproject.toml file is modified, run `poetry update` instead.
    poetry install

    # Setup pre-commit, required only once.
    poetry run pre-commit install

    # Run fetchmesh in poetry virtualenv, make sure it works.
    poetry run fetchmesh

    # Make some code changes
    # [...]

    # Run fetchmesh again to test your changes
    poetry run fetchmesh ...

    # Run the test suite
    poetry run pytest

    # Review and commit the changes
    git diff
    git add ...
    git commit -m '...'

    # Fix pre-commit warnings if needed, and go back to the previous step.
    # [...]

    # Push the chnages
    git push

Documentation
-------------

This documentation is built using `sphinx <https://www.sphinx-doc.org/en/master/>`_.
To build the documentation locally run the following:

.. code:: bash

   poetry run make -C docs/ html
   # The website will be found in docs/_build/html/

   poetry run make -C docs/ latexpdf
   # The PDF will be found at docs/_build/latex/fetchmesh.pdf

Tools
-----

=================================================== =============== =========================================
Tool                                                Usage           Command
=================================================== =============== =========================================
`black <https://github.com/psf/black>`__            Code formatting ``poetry run pre-commit run --all-files``
`isort <https://github.com/timothycrosley/isort>`__ Import sorting  ``poetry run pre-commit run --all-files``
`mypy <https://github.com/python/mypy>`__           Static typing   ``poetry run pre-commit run --all-files``
`pylint <https://www.pylint.org/>`__                Linting         ``poetry run pre-commit run --all-files``
`pytest <https://docs.pytest.org/en/latest/>`__     Unit tests      ``poetry run pytest``
=================================================== =============== =========================================

Release
-------

To create a release:

.. code:: bash

   poetry version x.x.x
   git commit -m 'Version x.x.x'
   git tag vx.x.x
   git push && git push --tags

.. _github-workflows:

GitHub workflows
----------------

Two `GitHub Workflows <https://docs.github.com/en/actions/learn-github-actions>`_ are defined:

`.github/workflows/ci.yml`
    Run the tests on Linux, and check that the package installs correctly on Linux, macOS and Windows.

`.github/workflows/documentation.yml`
    Build this documentation, and upload it to the `gh-pages` branch.
