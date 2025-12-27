from copy import copy
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple, cast
import impuls
import argparse
from common.extra_resources import NoSSLVerifyHttpResource
from kw_sanitizer.consts import GTFS_HEADERS
from polregio_mkuran_sanitizer.load_platforms import LoadPlatformData
from common.attribution import CreateFeedAttributions


class PolregioGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.LoadGTFS("polregio.zip", extra_fields=True),
                CutTrips(),
                MarkRoutesFromStops(),
                MarkRoutesFromShortName(),
                LoadPlatformData(),
                impuls.tasks.ModifyRoutesFromCSV("routes.csv"),
                CreateFeedAttributions(
                    "PolRegio", "https://polregio.pl/", "polregio.zip"
                ),
                impuls.tasks.SaveGTFS(
                    headers=GTFS_HEADERS, target="out/polregio_mkuran.zip"
                ),
            ],
            resources={
                "polregio.zip": impuls.HTTPResource.get(
                    "https://mkuran.pl/gtfs/polregio.zip"
                ),
                "platforms.json": impuls.HTTPResource.get(
                    "https://kasmar00.github.io/osm-plk-platform-validator/platforms-list.json"
                ),
                "routes.csv": impuls.LocalResource(
                    "polregio_mkuran_sanitizer/routes.csv"
                ),
            },
            options=options,
        )


@dataclass
class Cut:
    firstStop: str
    cutStop: str
    lastStop: str


class CutTrips(impuls.Task):
    cuts = [
        Cut("18408", "8474", "5900"),  # Bydgoszcz - Smętowo - Gdynia Główna
        Cut("18408", "8474", "6031"),  # Bydgoszcz - Smętowo - Gdynia Chylonia
        Cut("17202", "8474", "5900"),  # Laskowice Pomorskie - Smętowo - Gdynia Główna
        Cut("4705", "3004", "15602"),  # Słupsk - Szczecinek - Chojnice
    ]

    def execute(self, r):
        with r.db.transaction():
            for trip_id, trip_number in _get_trip_ids(r.db):
                stop_times = list(
                    r.db.typed_out_execute(
                        "SELECT * FROM stop_times WHERE trip_id=?",
                        impuls.model.StopTime,
                        (trip_id,),
                    )
                )
                legs = self.compute_legs(stop_times)
                if (len(legs)) == 1:
                    continue  # nothing to cut

                self.logger.info(f"About to cut {trip_number}")
                original_trip = r.db.retrieve(impuls.model.Trip, trip_id)
                r.db.raw_execute("DELETE FROM trips WHERE trip_id = ?", (trip_id,))
                for idx, leg in enumerate(legs):
                    trip: impuls.model.Trip = copy(original_trip)
                    trip.id = f"{trip_id}_{idx}"
                    trip.block_id = trip_id
                    r.db.create(trip)

                    for stop_time in leg:
                        stop_time.trip_id = trip.id
                        r.db.create(stop_time)

    def compute_legs(self, stop_times: List[impuls.model.StopTime]):
        stop_ids = [x.stop_id for x in stop_times]
        cut = self.should_cut_at(stop_ids)
        if not cut:
            return [stop_times]

        cut_index = stop_ids.index(cut)
        return [stop_times[: cut_index + 1], stop_times[cut_index:]]

    def should_cut_at(self, stop_ids: List[str]) -> Optional[str]:
        for cut in self.cuts:
            if cut.firstStop == stop_ids[0] and cut.lastStop == stop_ids[-1]:
                return cut.cutStop
            if cut.firstStop == stop_ids[-1] and cut.lastStop == stop_ids[0]:
                return cut.cutStop
        return None


@dataclass
class Variant:
    firstStop: str
    middleStop: Optional[str]
    lastStop: Optional[str]


@dataclass
class RouteSchema:
    id: str
    name: str
    color: str
    variants: List[Variant]


class MarkRoutesFromStops(impuls.Task):
    def execute(self, r):
        routes = [
            RouteSchema(
                "TROJ-S2",
                "S2",
                "AA2748",
                [
                    Variant(
                        "5900", "257530", "7534"
                    ),  # Gdynia Główna - Gdańsk Port Lotniczy - Gdańsk Wrzeszcz
                    Variant(
                        "7112", "7534", "257530"
                    ),  # Tczew - Gdańsk Wrzeszcz - Gdańsk Port Lotniczy
                ],
            ),
            RouteSchema("TROJ-R12", "R12", "F4d55A", [Variant("6791", None, None)]),
            RouteSchema(
                "TROJ-R22", "R22", "9ACACB", [Variant("264159", "4952", "4705")]
            ),
            RouteSchema(
                "TROJ-R16",
                "R16",
                "C97174",
                [
                    Variant("3004", "4705", "4705"),  # Szczecinek - Miastko - Słupsk
                    Variant(
                        "4705", "4713", "4556"
                    ),  # Słupsk - KObylnica Słupska - Miastko
                ],
            ),
            RouteSchema(
                "TROJ-R7",
                "R7",
                "A393b4",
                [
                    Variant(
                        "7112", "8607", "15602"
                    ),  # Tczew - Starogard Gdański - Chojnice
                    Variant("8607", None, None),  # Starogard Gdański -> Tczew
                ],
            ),
            RouteSchema(
                "TROJ-31",
                "R31",
                "6DA6D3",
                [
                    Variant("5900", "7112", "8474"),  # Gdynia Główna - Tczew - Smętowo
                    Variant(
                        "6031", "7112", "8474"
                    ),  # Gdynia Chylonia - Tczew - Smętowo
                ],
            ),
            RouteSchema(
                "TROJ-14",
                "R14",
                "A6ACD6",
                [
                    Variant(
                        "3004", "15594", "15602"
                    ),  # Szczecinek - Człuchów - Chojnice
                ],
            ),
            RouteSchema(
                "TROJ-11",
                "R11",
                "D87D29",
                [
                    Variant(
                        "18002", "257542", "7534"
                    ),  # Kartuzy - Gdańsk Kiełpinek - Gdańsk Wrzeszcz
                    Variant(
                        "18002", "257542", "7500"
                    ),  # Kartuzy - Gdańsk Kiełpinek - Gdańsk Główny
                ],
            ),
        ]

        with r.db.transaction():
            for route in routes:
                r.db.create(
                    impuls.model.route.Route(
                        id=route.id,
                        agency_id=0,
                        short_name=route.name,
                        long_name=route.name,
                        color=route.color,
                        type=impuls.model.Route.Type.RAIL,
                    )
                )
            for trip_id, trip_number in _get_trip_ids(r.db):
                stops = list(_get_trip_stops(r.db, trip_id))
                for route in routes:
                    matching = [
                        route
                        for route in routes
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


class MarkRoutesFromShortName(impuls.Task):
    # TODO: handle buses
    routes = {
        "PKM1": "WLKP-PKM1",
        "PKM2": "WLKP-PKM2",
        "PKM3": "WLKP-PKM3",
        "PKM4": "WLKP-PKM4",
        "PKM5": "WLKP-PKM5",
        "SKA1": "MPOL-SKA1",
        "SKA2": "MPOL-SKA2",
        "SKA3": "MPOL-SKA3",
        "K2": "MPOL-K2",
        "K22": "MPOL-K22",
        "K3": "MPOL-K3",
        "K32": "MPOL-K32",
        "K33": "MPOL-K33",
        "K5": "MPOL-K5",
        "K51": "MPOL-K51",
        "K52": "MPOL-K52",
        "K63": "MPOL-K63",
        "K7": "MPOL-K7",
        "K71": "MPOL-K71",
        "PKR": "MPOL-PKR",
        "S1": "ZACH-S1",
        "S2": "ZACH-S2",
        "S3": "ZACH-S3",
        "PKA": "PODK-PKA",
    }

    def execute(self, r):
        with r.db.transaction():
            for route_code, route_id in self.routes.items():
                r.db.create(
                    impuls.model.Route(
                        id=route_id,
                        agency_id=0,
                        short_name=route_code,
                        long_name=route_code,
                        type=impuls.model.route.Route.Type.RAIL,
                    )
                )
            for trip in r.db.retrieve_all(impuls.model.Trip):
                name_cut = trip.short_name.split()
                clear_name = False
                if len(name_cut) == 1:
                    continue
                else:
                    if "Podkarpacka Kolej Aglomeracyjna" in trip.short_name:
                        route_code = "PKA"
                        clear_name = True
                    elif "Podhalańska Kolej Regionalna" in trip.short_name:
                        route_code = "PKR"
                        clear_name = True
                    else:
                        route_code = name_cut[1]
                route_id = self.routes.get(route_code)
                if route_id:
                    trip.route_id = route_id
                    if clear_name:
                        trip.short_name = name_cut[0]
                    else:
                        trip.short_name = (
                            name_cut[0] + " " + " ".join(name_cut[2:])
                        ).strip()

                    r.db.update(trip)


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


def main() -> None:
    PolregioGTFS().run()
