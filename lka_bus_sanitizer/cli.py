import impuls
import argparse

from common.attribution import CreateFeedAttributions
from kw_sanitizer.consts import GTFS_HEADERS
from lka_bus_sanitizer.merge_stops import MergeStopsByNameAndCode


class LodzkaKolejAglomeracyjnaGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.LoadGTFS("lka.zip", extra_fields=True),
                impuls.tasks.ExecuteSQL(
                    "Remove non-bus routes",
                    """
                    DELETE FROM routes
                    WHERE type NOT LIKE 3;
                    """,
                ),
                impuls.tasks.ExecuteSQL(
                    "Set agency id to LKA",
                    """
                    UPDATE agencies
                    SET agency_id = 'LKA'
                """,
                ),
                impuls.tasks.ExecuteSQL(
                    "Add agency id on routes",
                    """
                    UPDATE routes
                    SET agency_id = 'LKA'
                """,
                ),
                CreateFeedAttributions(
                    operator_name="Łódzka Kolej Aglomeracyjna",
                    operator_url="https://lka.lodzkie.pl/",
                    feed_resource_name="lka.zip",
                ),
                impuls.tasks.ModifyStopsFromCSV("stops.csv"),
                MergeStopsByNameAndCode(),
                impuls.tasks.GenerateTripHeadsign(),
                impuls.tasks.RemoveUnusedEntities(),
                impuls.tasks.ModifyRoutesFromCSV("routes.csv"),
                impuls.tasks.SaveGTFS(headers=GTFS_HEADERS, target="out/lka.zip"),
            ],
            resources={
                "lka.zip": impuls.HTTPResource.get(
                    "https://kolej-lka.pl/pliki/pn0e6eg45qcl4hd5/gtfs-2024-2025/zip/"
                ),
                "routes.csv": impuls.LocalResource("lka_bus_sanitizer/routes.csv"),
                "stops.csv": impuls.LocalResource("lka_bus_sanitizer/stops.csv"),
            },
        )


def main() -> None:
    LodzkaKolejAglomeracyjnaGTFS().run()
