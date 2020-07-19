import datetime

from pytz import UTC

from fetchmesh.atlas import (
    AtlasAnchor,
    AtlasMeasurement,
    Country,
    MeasurementAF,
    MeasurementStatus,
    MeasurementType,
)


def test_anchor_dict():
    anchor = AtlasAnchor(
        id=966,
        probe_id=6263,
        fqdn="bg-sof-as8866-2.anchors.atlas.ripe.net",
        country=Country("BG"),
        as_v4=8866,
        as_v6=8866,
    )
    assert AtlasAnchor.from_dict(anchor.to_dict()) == anchor


def test_measurement_dict():
    measurement = AtlasMeasurement(
        id=14441445,
        af=MeasurementAF.IPv4,
        type=MeasurementType.Traceroute,
        status=MeasurementStatus.Ongoing,
        start_date=datetime.datetime(2018, 6, 16, 10, 41, 11, tzinfo=UTC),
        stop_date=None,
        description="Anchoring Mesh Measurement: Traceroute IPv4 for anchor de-dus-as39138.anchors.atlas.ripe.net",
        tags=("6368", "de-dus-as39138", "mesh", "anchoring"),
    )
    assert AtlasMeasurement.from_dict(measurement.to_dict()) == measurement
