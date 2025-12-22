import impuls
import argparse
import re
from kw_sanitizer.consts import GTFS_HEADERS
from common.extra_resources import NoSSLVerifyHttpResource
from typing import cast

from polregio_sanitizer.add_platform_locations import AddPlatformLocations
from polregio_sanitizer.add_stations import GenerateParentsForStops


class PolregioGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                # Plan:
                # 1. Get Polregio official
                impuls.tasks.LoadGTFS("polregio.zip", extra_fields=True),
                # 2. generate parents for stops
                GenerateParentsForStops(),
                # 2. Curate station names and locations from mkuran gtfs
                impuls.tasks.ModifyStopsFromCSV("stops.csv"),
                # 3. Curate platform/track locations (remember: bus to have parent location, not found platforms to have parent location)
                AddPlatformLocations(),
                # 4. Add trip names from mkuran gtfs

                # 4. Curate routes - extract specific routes we know of (K27, etc) based on trip names
                # CreateFeedAttributions(
                #     operator_name="Łódzka Kolej Aglomeracyjna",
                #     operator_url="https://lka.lodzkie.pl/",
                #     feed_resource_name="lka.zip",
                # ),
                # impuls.tasks.RemoveUnusedEntities(),
                # impuls.tasks.ModifyRoutesFromCSV("routes.csv"),
                impuls.tasks.SaveGTFS(headers=GTFS_HEADERS, target="out/polregio.zip"),
            ],
            resources={
                "polregio.zip": NoSSLVerifyHttpResource.get(
                    "https://transfer.polregio.pl/public/file/2xpjhbomseoindotgttcsg/GTFS.zip"
                ),
                # "routes.csv": impuls.LocalResource("polregio_sanitizer/routes.csv"),
                "stops.csv": impuls.resource.ZippedResource(
                    r=impuls.HTTPResource.get("https://mkuran.pl/gtfs/polregio.zip"),
                    file_name_in_zip="stops.txt",
                ),
                "trips.csv": impuls.resource.ZippedResource(
                    r=impuls.HTTPResource.get("https://mkuran.pl/gtfs/polregio.zip"),
                    file_name_in_zip="trips.txt",
                ),
                "platforms.json": impuls.HTTPResource.get(
                    "https://kasmar00.github.io/osm-plk-platform-validator/platforms-list.json"
                ),
            },
        )

def main() -> None:
    PolregioGTFS().run()
