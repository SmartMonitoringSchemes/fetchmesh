import datetime as dt
from dataclasses import dataclass
from typing import Optional

from ..atlas import AtlasMeasurement, MeasurementAF, MeasurementType
from .abstract import StreamFilter


class MeasurementFilter(StreamFilter[AtlasMeasurement]):
    pass


@dataclass(frozen=True)
class MeasurementTypeFilter(MeasurementFilter):
    af: MeasurementAF
    type: MeasurementType

    def keep(self, measurement):
        return (measurement.af == self.af) and (measurement.type == self.type)


@dataclass(frozen=True)
class MeasurementDateFilter(MeasurementFilter):
    """
    Keep measurements where start_date_gte <= start_date <= start_date_lte
    and stop_date_gte <= stop_date <= stop_date_lte.
    """

    start_date_gte: Optional[dt.datetime] = None
    start_date_lte: Optional[dt.datetime] = None
    stop_date_gte: Optional[dt.datetime] = None
    stop_date_lte: Optional[dt.datetime] = None

    def keep(self, measurement):
        if measurement.start_date and self.inrange(
            self.start_date_gte, self.start_date_lte, measurement.start_date
        ):
            if not measurement.stop_date or self.inrange(
                self.stop_date_gte, self.stop_date_lte, measurement.stop_date
            ):
                return True
        return False

    @classmethod
    def running(cls, start_date_lte, stop_date_gte):
        return cls(start_date_lte=start_date_lte, stop_date_gte=stop_date_gte)

    @staticmethod
    def inrange(gte, lte, x):
        res = True
        if gte is not None:
            res &= x >= gte
        if lte is not None:
            res &= x <= lte
        return res
