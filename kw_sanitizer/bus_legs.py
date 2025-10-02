import re
import impuls


class SplitBusLegs(impuls.tasks.SplitTripLegs):
    """
    Default behaviour of `SplitTripLegs` but bus departures are marked with field `bus`=`'1'`
    This needs a few more adjustments
    """

    def __init__(self):
        super().__init__(replacement_bus_short_name_pattern=re.compile("\sBUS"))

    def get_departure_data(self, stop_time):
        return stop_time.get_extra_field("bus") == "1"

    def arrival_only(self, stop_time, previous_data):
        return apply_stop_suffix_if_bus(super().arrival_only(stop_time, previous_data))

    def departure_only(self, stop_time, current_data):
        return apply_stop_suffix_if_bus(super().departure_only(stop_time, current_data))

    # TODO() make contribution upstream for get_transfer to receive two stop_times
    def get_transfer(self, trip_a, trip_b, transfer_stop_id):
        """
        This "guesses" which side of the transfer was bus part and corrects stop_id of that part
        It is necessary, cause get_transfer only gets one stop_id
        """
        ret = super().get_transfer(trip_a, trip_b, transfer_stop_id)
        if "BUS" in trip_a.route_id:
            # From Bus to train
            ret.from_stop_id = ensure_bus_suffix(ret.from_stop_id)
            ret.to_stop_id = ensure_no_suffix(ret.to_stop_id)
        else:
            # Fom train to bus
            ret.from_stop_id = ensure_no_suffix(ret.from_stop_id)
            ret.to_stop_id = ensure_bus_suffix(ret.to_stop_id)
        print(ret)
        return ret


def apply_stop_suffix_if_bus(stop_time: impuls.model.StopTime):
    if stop_time.platform == "BUS":
        stop_time.stop_id = stop_time.stop_id + "_BUS"
        stop_time.set_extra_field("bus", 1) #This is set so `ApplyBusPlatforms` correctly handles the stops
    else:
        stop_time.set_extra_field("bus", 0)
    return stop_time


def ensure_bus_suffix(str) -> str:
    if "_BUS" in str:
        return str
    else:
        return str + "_BUS"


def ensure_no_suffix(str) -> str:
    if "_BUS" in str:
        return str[:-4]
    else:
        return str


class ApplyBusPlatforms(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            r.db.raw_execute(
                """
UPDATE stop_times
SET stop_id = stop_id || '_BUS'
WHERE json_extract(extra_fields_json, '$.bus') = '1' and stop_id NOT LIKE '%BUS'
"""
            )
