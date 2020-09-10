import datetime as dt

from hypothesis import assume
from hypothesis.provisional import domains
from hypothesis.strategies import (
    booleans,
    composite,
    datetimes,
    integers,
    just,
    lists,
    none,
    one_of,
    sampled_from,
)
from pytz import UTC

from fetchmesh.atlas import MeasurementAF, MeasurementType
from fetchmesh.meta import AtlasResultsMeta


@composite
def atlas_datetimes(draw, **kwargs):
    def nomicrosecond(x):
        return x.replace(microsecond=0)

    return draw(
        datetimes(allow_imaginary=False, timezones=just(UTC), **kwargs).map(
            nomicrosecond
        )
    )


@composite
def atlas_results_metas(draw):
    # pylint: disable=E1120
    start_date = draw(atlas_datetimes())
    stop_date = draw(atlas_datetimes(min_value=start_date.replace(tzinfo=None)))

    return AtlasResultsMeta(
        af=draw(sampled_from(MeasurementAF)),
        type=draw(sampled_from(MeasurementType)),
        msm_id=draw(integers()),
        start_date=start_date,
        stop_date=stop_date,
        compressed=draw(booleans()),
    )


@composite
def collector_datetimes(draw):
    return draw(
        datetimes(
            min_value=dt.datetime(1000, 1, 1), max_value=dt.datetime(9999, 12, 31)
        ).map(lambda x: x.replace(minute=0, second=0, microsecond=0))
    )


@composite
def collector_fqdns(draw):
    return draw(domains()) + draw(sampled_from([".routeviews.org", ".ripe.net"]))
