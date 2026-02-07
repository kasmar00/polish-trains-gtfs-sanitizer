import impuls
from copy import copy
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Tuple, cast

@dataclass
class Variant:
    firstStop: str
    middleStop: Optional[str] = None
    lastStop: Optional[str] = None


@dataclass
class RouteSchema:
    id: str
    name: str
    variants: List[Variant]
    color: Optional[str] = None
    agencyId: Optional[int] = None


class MarkRoutesFromStops(impuls.Task):
    def __init__(self, routes: List[RouteSchema], trip_ids_finder = Optional[Callable]):
        self.routes = routes
        self._get_trip_ids = trip_ids_finder if trip_ids_finder else _get_trip_ids

        super().__init__()

    def execute(self, r):
        with r.db.transaction():
            for route in self.routes:
                r.db.create(
                    impuls.model.route.Route(
                        id=route.id,
                        agency_id=route.agencyId or 0,
                        short_name=route.name,
                        long_name=route.name,
                        color=route.color or "FFFFFF",
                        type=impuls.model.Route.Type.RAIL,
                    )
                )
            for trip_id, trip_number in _get_trip_ids(r.db):
                stops = list(_get_trip_stops(r.db, trip_id))
                matching = [
                    route
                    for route in self.routes
                    if self.route_stops_match(route, stops)
                ]
                if len(matching) > 1:
                    self.logger.warning(
                        f"Trip {trip_number} ({trip_id}) matches routes {[route.id for route in matching]}"
                    )

                if len(matching) == 1:
                    r.db.raw_execute(
                        "UPDATE trips SET route_id = ? WHERE trip_id = ?",
                        (matching[0].id, trip_id),
                    )

    def route_stops_match(self, route: RouteSchema, stops: List[str]) -> bool:
        for variant in route.variants:
            if not variant.lastStop or not variant.middleStop:
                if variant.firstStop == stops[0] or variant.firstStop == stops[-1]:
                    return True

            if variant.firstStop == stops[0] and variant.lastStop == stops[-1]:
                if variant.middleStop in stops:
                    return True
            if variant.firstStop == stops[-1] and variant.lastStop == stops[0]:
                if variant.middleStop in stops:
                    return True
        return False
    

def _get_trip_ids(db: impuls.DBConnection) -> Iterable[Tuple[str]]:
    yield from (
        cast(str, (i[0], i[1]))
        for i in db.raw_execute("SELECT trip_id, short_name FROM trips")
    )


def _get_trip_stops(db: impuls.DBConnection, trip_id: str) -> Iterable[str]:
    yield from (
        cast(str, i[0])
        for i in db.raw_execute(
            "SELECT stop_id FROM stop_times WHERE trip_id = ?", (trip_id,)
        )
    )
