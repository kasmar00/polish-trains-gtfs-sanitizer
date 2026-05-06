import impuls
import argparse

from common.attribution import CreateFeedAttributions
from kw_sanitizer.consts import GTFS_HEADERS
from lka_bus_sanitizer.curate_stop_names import CurateStopNames
from lka_bus_sanitizer.merge_stops import MergeStopsByNameAndCode
from lka_bus_sanitizer.curate_bus_routes import CurateBusRoutes
from common.lka_divide import DivideLKARoutes
from lka_combiner.cli import LKACombiner


class LodzkaKolejAglomeracyjnaGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.LoadGTFS("lka.zip", extra_fields=True),
                DivideLKARoutes(bus=True, rail_replacement_bus=True),
                CreateFeedAttributions(
                    operator_name="Łódzka Kolej Aglomeracyjna",
                    operator_url="https://lka.lodzkie.pl/",
                    feed_resource_name="lka.zip",
                ),
                CurateStopNames(),
                MergeStopsByNameAndCode(),
                impuls.tasks.ExecuteSQL(
                    "Remove Headsigns", "UPDATE trips SET headsign = '' "
                ),
                impuls.tasks.GenerateTripHeadsign(),
                impuls.tasks.RemoveUnusedEntities(),
                CurateBusRoutes(),
                impuls.tasks.SaveGTFS(headers=GTFS_HEADERS, target="out/lka_bus.zip"),
            ],
            resources={
                "lka.zip": impuls.LocalResource("out/lka_combined.zip"),
                "routes_match.csv": impuls.LocalResource("lka_bus_sanitizer/routes_match.csv"),
            },
        )


def main() -> None:
    LKACombiner().run()
    LodzkaKolejAglomeracyjnaGTFS().run()
