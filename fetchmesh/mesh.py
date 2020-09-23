import json
from collections import defaultdict
from itertools import product

from cached_property import cached_property

from .atlas import AtlasAnchor, AtlasClient, AtlasMeasurement
from .filters import AnchorFilter, MeasurementFilter


def serialize(o):
    if isinstance(o, (AtlasAnchor, AtlasMeasurement)):
        return o.to_dict()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


class AnchoringMeshPairs:
    """
    Anchoring Mesh pairs container.

    .. code-block:: python

        from fetchmesh.filters import PairSampler, SelfPairFilter
        from fetchmesh.mesh import AnchoringMesh

        mesh = AnchoringMesh.from_api()
        pairs = mesh.pairs

        # Keep 10% of the pairs
        pairs = pairs.filter(PairSampler(0.1))

        # Remove "self" measurements
        pairs = pairs.filter(SelfPairFilter())
    """

    def __init__(self, pairs):
        self._pairs = pairs

    def __getitem__(self, index):
        if isinstance(index, slice):
            return AnchoringMeshPairs(self._pairs[index])
        return self._pairs[index]

    def __len__(self):
        return len(self._pairs)

    def __eq__(self, o):
        return set(self._pairs) == set(o._pairs)

    def __hash__(self):
        return hash(set(self._pairs))

    def filter(self, f):
        if isinstance(f, list):
            pairs = self
            for x in f:
                pairs = pairs.filter(x)
            return pairs
        pairs = f(self._pairs)
        return AnchoringMeshPairs(pairs)

    def by_target(self, probes=True):
        d = defaultdict(list)
        for target, source in self._pairs:
            if probes:
                source = source.probe_id
            d[target].append(source)
        return list(d.items())

    @classmethod
    def from_anchors(cls, anchors):
        pairs = list(product(anchors, anchors))
        return cls(pairs)

    @classmethod
    def from_json(cls, path):
        with open(path) as f:
            d = json.load(f, object_hook=AtlasAnchor.from_dict)
        d = [tuple(x) for x in d]
        return cls(d)

    def to_json(self, path):
        with open(path, "w") as f:
            json.dump(self._pairs, f, default=serialize)


class AnchoringMesh:
    """
    Anchoring Mesh wrapper.

    .. code-block:: python

        from datetime import datetime
        from fetchmesh.atlas import MeasurementAF, MeasurementType
        from fetchmesh.filters import MeasurementDateFilter, MeasurementTypeFilter
        from fetchmesh.mesh import AnchoringMesh

        mesh = AnchoringMesh.from_api()

        # Keep anchors running between 2019-01-01 and 2019-01-02
        mesh = mesh.filter(MeasurementDateFilter.running(
            datetime(2019,1,1), datetime(2019,1,2)
        ))

        # Keep anchors participating in IPv6 ping measurements
        mesh = mesh.filter(MeasurementTypeFilter(
            MeasurementAF.IPv6, MeasurementType.Ping
        ))
    """

    def __init__(self, data):
        self._data = data

    def __eq__(self, o):
        return (self.anchors == o.anchors) and (self.measurements == o.measurements)

    @cached_property
    def anchors(self):
        """Anchoring mesh anchors."""
        return {x[0] for x in self._data}

    @cached_property
    def measurements(self):
        """Anchoring mesh measurements."""
        return {x[1] for x in self._data}

    @cached_property
    def pairs(self):
        return AnchoringMeshPairs.from_anchors(self.anchors)

    def filter(self, f):
        if isinstance(f, AnchorFilter):
            data = f(self._data, key=lambda x: x[0])
            return AnchoringMesh(data)
        if isinstance(f, MeasurementFilter):
            data = f(self._data, key=lambda x: x[1])
            return AnchoringMesh(data)
        if isinstance(f, list):
            mesh = self
            for x in f:
                mesh = mesh.filter(x)
            return mesh
        raise NotImplementedError(f"{type(f).__name__} is not supported")

    def find_measurement(self, anchor, af, type_):
        for target, measurement in self._data:
            if target == anchor and measurement.af == af and measurement.type == type_:
                return measurement
        return None

    def find_sibling(self, measurement, type_):
        for _, other in self._data:
            if (
                other.type == type_
                and other.af == measurement.af
                and other.anchor_probe == measurement.anchor_probe
            ):
                return other
        return None

    @classmethod
    def from_api(cls, client=AtlasClient()):
        """
        Instantiate the AnchoringMesh from ``anchor-measurements/?include=target,measurement``.
        """
        records = client.fetch_anchoring_measurements()
        data = []
        for x in records:
            anchor = AtlasAnchor.from_dict(x["target"])
            measurement = AtlasMeasurement.from_dict(x["measurement"])
            # Filter out "Anchoring Probes" measurements
            if measurement.is_anchoring_mesh:
                data.append((anchor, measurement))
        return cls(data)

    @classmethod
    def from_json(cls, path):
        with open(path) as f:
            data = json.load(f)
        data = [
            (AtlasAnchor.from_dict(x1), AtlasMeasurement.from_dict(x2))
            for x1, x2 in data
        ]
        return cls(data)

    def to_json(self, path):
        with open(path, "w") as f:
            json.dump(self._data, f, default=serialize)
