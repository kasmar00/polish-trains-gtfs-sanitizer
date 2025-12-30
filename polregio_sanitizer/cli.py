import impuls
import argparse
from kw_sanitizer.consts import GTFS_HEADERS
from polregio_sanitizer.load_platforms import LoadPlatformData
from common.attribution import CreateFeedAttributions
from polregio_sanitizer.routes_from_short_name import MarkRoutesFromShortName
from polregio_sanitizer.routes_from_stops import CutTrips, MarkRoutesFromStops


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
                impuls.tasks.SaveGTFS(headers=GTFS_HEADERS, target="out/polregio.zip"),
            ],
            resources={
                "polregio.zip": impuls.HTTPResource.get(
                    "https://mkuran.pl/gtfs/polregio.zip"
                ),
                "platforms.json": impuls.HTTPResource.get(
                    "https://kasmar00.github.io/osm-plk-platform-validator/platforms-list.json"
                ),
                "routes.csv": impuls.LocalResource("polregio_sanitizer/routes.csv"),
            },
            options=options,
        )


def main() -> None:
    PolregioGTFS().run()
