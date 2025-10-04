import impuls
import argparse

from common.add_platforms import AddPlatforms
from common.attribution import CreateFeedAttributions
from kw_sanitizer.bus_legs import ApplyBusPlatforms, SplitBusLegs
from kw_sanitizer.consts import GTFS_HEADERS
from kw_sanitizer.preprocess import FixInitially


class CreatePKMRoutes(impuls.Task):
    """
    PKM route numbers are supplied in a custom field `PKM`. The field has format: `1`, `12`, `21`, `0` (`0` meaning not a PKM route).
    If the number is double digit, that means that up to Poznań Główny station the trip runs on route
        corresponding to the first digit, and after this station runs on route corresponding to the second digit.
        This implementation, as a simplification, and marks such trip as running wholy on the route of the second digit.
    """

    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: impuls.TaskRuntime) -> None:
        pkm_in_db = r.db.raw_execute(
            """
            SELECT DISTINCT json_extract(trips.extra_fields_json, '$.PKM') as pkm
            FROM trips
            WHERE pkm IS NOT NULL and pkm IS NOT '0'
            """
        ).all()
        clean_pkms = {x[0][-1] for x in pkm_in_db}
        with r.db.transaction():
            for pkm in clean_pkms:

                r.db.create(
                    impuls.model.Route(
                        id=f"PKM{pkm}",
                        agency_id="KW",
                        short_name=f"PKM{pkm}",
                        long_name=f"PKM{pkm}",
                        type=impuls.model.Route.Type.RAIL,
                    )
                )

                r.db.raw_execute(
                    """
                    UPDATE trips
                    SET route_id = ?
                    WHERE json_extract(extra_fields_json, '$.PKM') LIKE ?
                    """,
                    (f"PKM{pkm}", f"%{pkm}"),
                )


class CleanTripNames(impuls.Task):
    """
    short_trip_name comes in format: `12345 EZT`, `32432 SZYNOBUS`, `3455/3 EZT/BUS`
    Here we clean the suffix and move it to `traktion` field.
    """

    def execute(self, r):
        with r.db.transaction():
            for trip in r.db.retrieve_all(impuls.model.Trip).all():
                short_name, traktion = trip.short_name.split(" ")
                trip.short_name = short_name  # TODO: add route short name here

                if "/BUS" in traktion:
                    if "BUS" in trip.route_id:
                        traktion = "BUS"
                    else:
                        traktion = traktion.split("/")[0]
                trip.set_extra_field(
                    "traktion", traktion
                )  # unless someone has a better idea for a field name
                r.db.update(trip)


class KolejeWielkopolskieGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                FixInitially(),
                impuls.tasks.LoadGTFS("kw.zip", extra_fields=True),
                CreatePKMRoutes(),
                impuls.tasks.ModifyStopsFromCSV("stops.csv"),
                AddPlatforms(),
                SplitBusLegs(),
                impuls.tasks.ModifyRoutesFromCSV("routes.csv"),
                CleanTripNames(),
                ApplyBusPlatforms(),
                CreateFeedAttributions(
                    operator_name="Koleje Wielkopolskie",
                    operator_url="https://koleje-wielkopolskie.com.pl/",
                    feed_resource_name="kw.zip"
                ),
                impuls.tasks.SaveGTFS(headers=GTFS_HEADERS, target="out/kw.zip"),
            ],
            resources={
                "kw.zip": impuls.HTTPResource.get(
                    "http://ws.kolejewlkp.pl:83/gtfs_kw.zip"
                ),
                "routes.csv": impuls.LocalResource("kw_sanitizer/routes.csv"),
                "stops.csv": impuls.LocalResource("kw_sanitizer/stops.csv"),
                "platforms.json": impuls.HTTPResource.get(
                    "https://kasmar00.github.io/osm-plk-platform-validator/platforms-list.json"
                ),
            },
            options=options,
        )


def main() -> None:
    KolejeWielkopolskieGTFS().run()
