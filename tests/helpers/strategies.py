import datetime as dt

from hypothesis import assume
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
