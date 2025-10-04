import argparse
import impuls

from common.add_platforms import AddPlatforms
from common.attribution import CreateFeedAttributions
from kml_sanitizer.apply_platforms_from_headsigns import ApplyPlatformsFromHeadsigns
from kml_sanitizer.normalize_stop_names import NormalizeStopNames
from kw_sanitizer.consts import GTFS_HEADERS


class KolejeMalopolskieGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.LoadGTFS("kml.zip", extra_fields=True),
                NormalizeStopNames(),
                AddPlatforms(),
                ApplyPlatformsFromHeadsigns(),
                impuls.tasks.GenerateTripHeadsign(),
                impuls.tasks.SplitTripLegs(),  # TODO: handle arrival on bus stop
                # TODO: normalize trip names
                # TODO: curate route names
                CreateFeedAttributions(
                    operator_name="Koleje Małopolskie",
                    operator_url="https://kolejemalopolskie.com.pl/",
                    feed_resource_name="kml.zip",
                ),
                impuls.tasks.SaveGTFS(headers=GTFS_HEADERS, target="out/kml.zip"),
            ],
            resources={
                "kml.zip": impuls.HTTPResource.get(
                    "https://kolejemalopolskie.com.pl/rozklady_jazdy/kml-ska-gtfs.zip"
                ),
                "platforms.json": impuls.HTTPResource.get(
                    "https://kasmar00.github.io/osm-plk-platform-validator/platforms-list.json"
                ),
            },
            options=options,
        )


def main() -> None:
    KolejeMalopolskieGTFS().run()
