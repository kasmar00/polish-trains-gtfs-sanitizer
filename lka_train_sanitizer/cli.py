import impuls
import argparse

from common.attribution import CreateFeedAttributions
from kw_sanitizer.consts import GTFS_HEADERS
from common.lka_divide import DivideLKARoutes
from lka_combiner.cli import LKACombiner


class LodzkaKolejAglomeracyjnaGTFS(impuls.App):
    def prepare(
        self, args: argparse.Namespace, options: impuls.PipelineOptions
    ) -> impuls.Pipeline:
        return impuls.Pipeline(
            tasks=[
                impuls.tasks.LoadGTFS("lka.zip", extra_fields=True),
                DivideLKARoutes(train=True),
                CreateFeedAttributions(
                    operator_name="Łódzka Kolej Aglomeracyjna",
                    operator_url="https://lka.lodzkie.pl/",
                    feed_resource_name="lka.zip",
                ),
                impuls.tasks.RemoveUnusedEntities(),
                impuls.tasks.ModifyRoutesFromCSV("routes.csv"),
                impuls.tasks.ExecuteSQL(
                    "Fix Pleszew",
                    "UPDATE stops SET lat = 51.89250, lon = 17.73247 WHERE name = 'Pleszew' ",
                ),
                impuls.tasks.SaveGTFS(headers=GTFS_HEADERS, target="out/lka_train.zip"),
            ],
            resources={
                "lka.zip": impuls.LocalResource("out/lka_combined.zip"),
                "routes.csv": impuls.LocalResource("lka_train_sanitizer/routes.csv"),
            },
        )


def main() -> None:
    LKACombiner().run()
    LodzkaKolejAglomeracyjnaGTFS().run()
