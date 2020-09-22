Getting Started
===============

Requirements
------------

fetchmesh is tested on Linux, macOS and Windows (see :ref:`github-workflows`).
It should work on any platform supported by Python.

fetchmesh requires at-least **Python 3.7** (released in June 2018), notably due to the use of `dataclasses <https://docs.python.org/3/library/dataclasses.html>`_.

If need be, a `Conda
environment <https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html#managing-environments>`_
with Python 3.7 can be created as follow:

.. code:: bash

   conda create -n python37 python=3.7
   conda activate python37
   python --version # Python 3.7.6

Installation
------------

For now, fetchmesh is a private tool, and as such it is not publised on PyPI, the public Python package index.
Instead, fetchmesh can be installed with `pip` directly from GitHub:

.. code:: bash

   pip install --upgrade pip
   pip install --upgrade git+ssh://git@github.com/SmartMonitoringSchemes/fetchmesh.git

or from a local copy:

.. code:: bash

   git clone git@github.com:SmartMonitoringSchemes/fetchmesh.git
   cd fetchmesh; pip install .

To verify the installation:

.. code:: bash

   fetchmesh --help
   # Usage: fetchmesh [OPTIONS] COMMAND [ARGS]...

If you want to make changes to the library, see the :ref:`Development` chapter.

Usage
-----

.. list-table:: fetchmesh commands overview
   :header-rows: 1

   * - Command
     - Description
     - Input
     - Output
   * - describe
     - Anchoring mesh overview
     - N/A
     - N/A
   * - fetch
     - Fetch measurements results
     - N/A
     - One ``ndjson`` file per measurement
   * - unpack
     - Split measurement results by pairs
     - ``ndjson`` files
     - One ``ndjson`` file per origin-destination pair
   * - csv
     - Convert measurement results to CSV
     - ``ndjson`` files
     - One or more ``csv`` files, depending on the mode

Use the ``--help`` flag to get more informations on a command,
e.g. \ ``fetchmesh fetch --help``.

Example workflow
~~~~~~~~~~~~~~~~

A typical workflow involves the following steps:

1. Fetch *raw* measurements results from Atlas API in ``ndjson`` format (one file per measurement, one measurement result per line).
2. Convert these measurement results in ``csv`` format, either in ``split`` mode (one file per origin-destination pair, two columns: timestamp, rtt), or in ``merge`` mode (one file, one line per origin-destination pair, one column per timestamp).

.. code:: bash

   # Fetch IPv4 ping results for 1% of the origin-destination pairs for the 1st of February 2020,
   # excluding self measurements and "reverse" measurements, using 4 concurrent requests.
   fetchmesh fetch --af 4 --type ping --no-self --half --sample-pairs 0.01 \
     --start-date 2020-02-01 --stop-date 2020-02-02 --jobs 4

   ls -lh ping_v4_1580511600_1580598000/
   # total 169M
   # -rw-r--r--. 1 maxmouchet maxmouchet 1.1M Mar  3 15:48 ping_v4_1580511600_1580598000_10105927_anchors.ndjson
   # -rw-r--r--. 1 maxmouchet maxmouchet 180K Mar  3 15:49 ping_v4_1580511600_1580598000_10206810_anchors.ndjson
   # ...

   head -n 1 ping_v4_1580511600_1580598000/ping_v4_1580511600_1580598000_1042404_anchors.ndjson
   # {"af": 4, "avg": 24.0117783333, "dst_addr": "213.225.160.239", "dst_name": "213.225.160.239", "dup": 0, "from": "193.135.150.58", "fw": 4970, "group_id": 1042404, "lts": 41, "max": 24.066907, "min": 23.976115, "msm_id": 1042404, "msm_name": "Ping", "prb_id": 6533, "proto": "ICMP", "rcvd": 3, "result": [{"rtt": 24.066907}, {"rtt": 23.976115}, {"rtt": 23.992313}], "sent": 3, "size": 32, "src_addr": "193.135.150.58", "step": 240, "stored_timestamp": 1580511732, "timestamp": 1580511644, "ttl": 61, "type": "ping"}

.. code:: bash

   # Generate a single CSV files with all the time series
   fetchmesh csv ping --mode merge ping_v4_1580511600_1580598000/*

   head -n 2 merge_1583317062.csv
   # pair,1580511600,1580511840,1580512080,...
   # 1042404_6533,23.976115,24.019383,24.106377,...
