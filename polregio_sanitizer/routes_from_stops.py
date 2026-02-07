import impuls
from copy import copy
from dataclasses import dataclass
from typing import List, Optional

from common.routes_from_stops import RouteSchema, Variant, _get_trip_ids


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


ROUTES = [
    RouteSchema(
        id="TROJ-S2",
        name="S2",
        color="AA2748",
        variants=[
            Variant(
                "5900", "257530", "7534"
            ),  # Gdynia Główna - Gdańsk Port Lotniczy - Gdańsk Wrzeszcz
            Variant(
                "7112", "7534", "257530"
            ),  # Tczew - Gdańsk Wrzeszcz - Gdańsk Port Lotniczy
        ],
    ),
    RouteSchema(
        id="TROJ-R3",
        name="R3",
        color="90BAE6",
        variants=[
            Variant("8151", "5900", "4705"),  # Elbląg - Gdynia Główna - Słupsk
            Variant("5900", "5447", "4705"),  # Gdynia Główna - Luzino - Słupsk
            Variant("8151", "5900", "5447"),  # Elbląg - Gdynia Główna - Luzino
            Variant("7112", "5900", "4705"),  # Tczew - Gdynia Główna - Słupsk
            Variant("6031", "7112", "8151"),  # Gdynia Chylonia - Tczew - Elbląg
            Variant("5900", "7112", "8151"),  # Gdynia Główna - Tczew - Elbląg
            Variant("5900", "7112", "7872"),  # Gdynia Główna - Tczew - Malbork
            Variant("7872", "7914", "8151"),  # Malbork - Fiszewo - Elbląg
            Variant("6031", "7112", "7872"),  # Gdynia Chylonia - Tczew - Malbork
        ],
    ),
    RouteSchema(
        id="TROJ-R31",
        name="R31",
        color="6DA6D3",
        variants=[
            Variant("5900", "7112", "8474"),  # Gdynia Główna - Tczew - Smętowo
            Variant("6031", "7112", "8474"),  # Gdynia Chylonia - Tczew - Smętowo
        ],
    ),
    # TODO: R5
    RouteSchema(
        id="TROJ-R7",
        name="R7",
        color="A393B4",
        variants=[
            Variant("7112", "8607", "15602"),  # Tczew - Starogard Gdański - Chojnice
            Variant("8607", None, None),  # Starogard Gdański -> Tczew
        ],
    ),
    # TODO: R8 (currently ZKA)
    # TODO: R9 (currently ZKA)
    RouteSchema(
        id="TROJ-R11",
        name="R11",
        color="D87D29",
        variants=[
            Variant(
                "18002", "257542", "7534"
            ),  # Kartuzy - Gdańsk Kiełpinek - Gdańsk Wrzeszcz
            Variant(
                "18002", "257542", "7500"
            ),  # Kartuzy - Gdańsk Kiełpinek - Gdańsk Główny
        ],
    ),
    RouteSchema(
        id="TROJ-R12",
        name="R12",
        color="F4d55A",
        variants=[Variant("6791", None, None)],
    ),
    RouteSchema(
        id="TROJ-R13",
        name="R13",
        color="E2ABCC",
        variants=[
            Variant("7872", "23101", "20503"),  # Malbork - Kwidzyn - Grudziądz
            Variant("7872", "8086", "23101"),  # Malbork - Sztum - Kwidzyn
        ],
    ),
    RouteSchema(
        id="TROJ-R14",
        name="R14",
        color="A6ACD6",
        variants=[
            Variant("3004", "15594", "15602"),  # Szczecinek - Człuchów - Chojnice
        ],
    ),
    RouteSchema(
        id="TROJ-R16",
        name="R16",
        color="C97174",
        variants=[
            Variant("3004", "4705", "4705"),  # Szczecinek - Miastko - Słupsk
            Variant("4705", "4713", "4556"),  # Słupsk - Kobylnica Słupska - Miastko
        ],
    ),
    # TODO: R17 (currently ZKA)
    # TODO: R20 - skipped cause it's only three stations out of 10-12
    RouteSchema(
        id="TROJ-R21",
        name="R21",
        color="7CA3BC",
        variants=[Variant("5355", None, None)],
    ),
    RouteSchema(
        id="TROJ-R22",
        name="R22",
        color="9ACACB",
        variants=[Variant("264159", "4952", "4705")],
    ),
]
