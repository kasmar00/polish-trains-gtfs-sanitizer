import impuls
import re

class SplitBusLegs(impuls.tasks.SplitTripLegs):
    def __init__(self, replacement_bus_short_name_pattern = None):
        super().__init__(replacement_bus_short_name_pattern=re.compile(".*BUS"))
    
    def arrival_only(self, stop_time, previous_data):
        return apply_stop_suffix_if_bus(super().arrival_only(stop_time, previous_data))
    
    def departure_only(self, stop_time, current_data):
        st = super().departure_only(stop_time, current_data)
        if (current_data):
            return apply_stop_suffix_if_bus(st)
        else:
            return st
    
    def get_transfer(self, trip_a, trip_b, transfer_stop_id):
        """
        This "guesses" which side of the transfer was bus part and corrects stop_id of that part
        It is necessary, cause get_transfer only gets one stop_id
        As source data only has departure stops there's some quirkines:
        - BUS Side always gets `BUS` platform,
        - train part gets stop for specific platform if its a BUS -> Train transfer (we know the departure platform)
        - train part gets generic (`station_id`) stop if its a Train -> Bus transfer (we don't know the arrival platform)
        """
        ret = super().get_transfer(trip_a, trip_b, transfer_stop_id)
        if "BUS" in trip_a.route_id:
            # From Bus to train
            ret.from_stop_id = ensure_bus_suffix(ret.from_stop_id)
            ret.to_stop_id = ret.to_stop_id
        else:
            # Fom train to bus
            ret.from_stop_id = ensure_no_suffix(ret.from_stop_id)
            ret.to_stop_id = ensure_bus_suffix(ret.to_stop_id)
        return ret

def apply_stop_suffix_if_bus(stop_time: impuls.model.StopTime):
    if stop_time.platform == "BUS":
        stop_time.stop_id = ensure_bus_suffix(stop_time.stop_id)
    else:
        stop_time.stop_id = ensure_no_suffix(stop_time.stop_id)
    return stop_time


def ensure_bus_suffix(str) -> str:
    return station_id(str) + "_BUS"


def ensure_no_suffix(str) -> str:
    return station_id(str)

def station_id(id_with_platform) -> str:
    return id_with_platform.split("_")[0]