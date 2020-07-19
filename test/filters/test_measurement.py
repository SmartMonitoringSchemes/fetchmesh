import datetime as dt

from hypothesis import given
from strategies import atlas_results_metas

from fetchmesh.filters import MeasurementDateFilter, MeasurementTypeFilter


@given(meta=atlas_results_metas())
def test_type_measurement_filter(meta):
    f = MeasurementTypeFilter(meta.af, meta.type)
    assert f.keep(meta)


@given(meta=atlas_results_metas())
def test_running_measurement_filter(meta):
    f = MeasurementDateFilter.running(meta.start_date, meta.stop_date)
    assert f.keep(meta)
    assert f([meta, meta]) == [meta, meta]

    f = MeasurementDateFilter.running(
        meta.start_date - dt.timedelta(seconds=1), meta.stop_date
    )
    assert not f.keep(meta)
    assert f([meta, meta]) == []

    f = MeasurementDateFilter.running(
        meta.start_date, meta.stop_date + dt.timedelta(seconds=1)
    )
    assert not f.keep(meta)
