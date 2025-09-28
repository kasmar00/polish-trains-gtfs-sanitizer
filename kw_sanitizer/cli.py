import impuls
import argparse
import zipfile
import os

from common.add_platforms import AddPlatforms
from kw_sanitizer.consts import GTFS_HEADERS, POLAND_TZ


class FixInitially(impuls.Task):
    def __init__(self) -> None:
        super().__init__()

    new_content = """route_id,route_short_name,route_long_name,route_type,agency_id
KW,KW, ,2,KW
"""

    def execute(self, r: impuls.TaskRuntime) -> None:
        file = r.resources["kw.zip"]
        with zipfile.ZipFile("kw.out.zip", "w") as zout:
            with zipfile.ZipFile(file.open_binary()) as zin:
                for item in zin.infolist():
                    if item.filename != "routes.txt":
                        zout.writestr(item, zin.read(item.filename))
                zout.writestr("routes.txt", self.new_content)
        os.replace("kw.out.zip", file.stored_at)


class CreatePKMRoutes(impuls.Task):
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
    def execute(self, r):
        with r.db.transaction():
            for trip in r.db.retrieve_all(impuls.model.Trip).all():
                short_name, traktion = trip.short_name.split(" ")
                trip.short_name = short_name #TODO: add route short name here

                if "/BUS" in traktion:
                    if "BUS" in trip.route_id:
                        traktion = "BUS"
                    else:
                        traktion = traktion.split("/")[0]
                trip.set_extra_field(
                    "traktion", traktion
                )  # unless someone has a better idea
                r.db.update(trip)


class SplitBusLegs(impuls.tasks.SplitTripLegs):
    """
    Default behaviour of `SplitTripLegs` but bus departures are marked with field `bus`=`'1'`
    """

    def get_departure_data(self, stop_time):
        return stop_time.get_extra_field("bus") == "1"


class CreateFeedAttributions(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            source_timestamp = r.resources["kw.zip"].last_modified.astimezone(
                POLAND_TZ
            )
            r.db.create(
                impuls.model.FeedInfo(
                    publisher_name="Marcin Kasznia",
                    publisher_url="https://gtfs.kasznia.net",
                    lang="pl",
                    version=source_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
            r.db.create_many(
                impuls.model.Attribution,
                [
                    impuls.model.Attribution(
                        id=1,
                        organization_name="Platform locations © OpenStreetMap contributors under ODbL",
                        url="https://openstreetmap.org/copyright",
                    ),
                    impuls.model.Attribution(
                        id=2,
                        organization_name="Koleje Wielkopolskie",
                        is_operator=True,
                        url="https://koleje-wielkopolskie.com.pl/",
                    ),
                    impuls.model.Attribution(
                        id="3",
                        organization_name="Marcin Kasznia",
                        is_producer=True,
                        url="https://gtfs.kasznia.net",
                    ),
                ],
            )


class KolejeWielkopolskieGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                FixInitially(),
                impuls.tasks.LoadGTFS("kw.zip", extra_fields=True),
                CreatePKMRoutes(),
                SplitBusLegs(),
                impuls.tasks.ModifyRoutesFromCSV("routes.csv"),
                CleanTripNames(),
                impuls.tasks.ModifyStopsFromCSV("stops.csv"),
                AddPlatforms(),
                CreateFeedAttributions(),
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
