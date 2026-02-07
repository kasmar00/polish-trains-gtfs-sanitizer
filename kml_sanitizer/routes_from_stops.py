from typing import Iterable, Tuple, cast
import impuls
from common.routes_from_stops import RouteSchema, Variant

ROUTES = [
    RouteSchema(
        id="SKA1",
        name="SKA1",
        agencyId=1,
        variants=[
            Variant("241960"),  # Wieliczka Rynek-Kopalnia
            Variant("235879"),  # Kraków Lotnisko
        ],
    ),
    RouteSchema(
        id="SKA2",
        name="SKA2",
        agencyId=1,
        variants=[
            Variant("77107", "80101", "80416"),  # Oświęcim - Skawina - Kraków Główny
            Variant("80101"),  # Skawina
            Variant("64303"),  # Sędziszów
            Variant("79467"),  # Miechów
            Variant("79384"),  # Słomniki
            Variant("64337"),  # Kozłów
        ],
    ),
    RouteSchema(
        id="SKA3",
        name="SKA3",
        agencyId=1,
        variants=[
            Variant("77107", "77503", "80416"),  # Oświęcim - Trzebinia - Kraków Główny
            Variant("80630"),  # Tarnów
        ],
    ),
]


def get_trip_ids_for_ska(db: impuls.DBConnection) -> Iterable[Tuple[str]]:
    yield from (
        cast(str, (i[0], i[1]))
        for i in db.raw_execute(
            "SELECT trip_id, short_name FROM trips WHERE route_id = 'KMl'"
        )
    )
