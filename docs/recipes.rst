Recipes
=======

Alias resolution with kapar
---------------------------

https://www.caida.org/tools/measurement/kapar/

Install kapar
~~~~~~~~~~~~~~~~

.. code:: bash

   # Download and compile patched version (CAIDA version has a bug and doesn't compile)
   git clone https://github.com/maxmouchet/kapar.git
   cd kapar
   ./configure
   make

   # Download bogons files
   wget https://www.team-cymru.org/Services/Bogons/bogon-bn-agg.txt

   # Verification
   ./kapar/kapar --help

Fetch some traceroutes
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   fetchmesh fetch --type traceroute --af 4 --start-date "2020-07-20 12:00" --stop-date "2020-07-20 12:30" --sample-pairs 0.01 --jobs 4

Convert traceroutes to kapar input format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

================================================== =============================
Input                                              Output
================================================== =============================
RIPE Atlas traceroute results in ``ndjson`` format ``paths.txt`` in kapar format
================================================== =============================

.. code:: python

   from fetchmesh.io import AtlasRecordsReader
   from fetchmesh.transformers import TracerouteFlatIPTransformer

   def to_kapar_format(record, include_origin=True):
       lines = [
           # Only '#' is strictly necessary on this line. We include the metadata for reference, if needed.
           f"# timestamp={record['timestamp']} measurement={record['msm_id']} probe={record['prb_id']}"
       ]
       if include_origin:
           lines.append(record["from"])
       for replies in record["hops"]:
           # Insert 0.0.0.0 in place of missing values (will be filtered by kapar)
           lines.append(replies[0] or "0.0.0.0")
       return "\n".join(lines)

   # --------------------------------------------------------------------------------------------------------------
   # ! Edit this line with the correct path to the downloaded traceroutes (directly in `ndjson` format, not `csv`).
   path = "/home/maxmouchet/Clones/github.com/maxmouchet/fetchmesh/traceroute_v4_1595289600_1595334480/"
   # --------------------------------------------------------------------------------------------------------------

   transformers = [TracerouteFlatIPTransformer(drop_dup=True, drop_late=True, drop_private=True)]
   rdr = AtlasRecordsReader.glob(path, "*.ndjson", transformers=transformers)

   # This will take some time (~30s)
   output = [to_kapar_format(record) for record in rdr]

   with open("paths.txt", "w") as f:
       f.write("\n".join(output))

Perform inference with kapar
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+--------------------------------+-------------------------------------+
| Input                          | Output                              |
+================================+=====================================+
| ``bogon-bn-agg.txt``,          | ``kapar.aliases``,                  |
| ``paths.txt``                  | ``kapar.ifaces``, ``kapar.links``,  |
|                                | ``kapar.subnets``                   |
+--------------------------------+-------------------------------------+

.. code:: bash

   ./kapar/kapar -o alis -B bogon-bn-agg.txt -P paths.txt

Check the results
~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   head kapar.aliases
   # # found 8182 nodes, containing 12614 interfaces (0 redundant (omitted), 207 anonymous, 12407 named).
   # node N1:  95.59.172.33 87.245.238.25 178.210.33.75 89.218.74.77 92.46.59.234
   # node N2:  172.253.65.176 216.239.58.255 172.253.70.202 142.250.61.66 172.253.70.204
   # ...

   head kapar.links
   # # found 9892 links, containing 22209 interfaces (9735 implicit, 0 redundant (omitted), 207 anonymous, 12267 named).
   # link L1:  N5:129.143.66.64 N4763:129.143.66.65
   # link L2:  N1159:213.91.165.184 N6916:213.91.165.185
   # ...
