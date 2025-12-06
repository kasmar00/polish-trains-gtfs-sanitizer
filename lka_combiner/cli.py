import argparse
import impuls
from impuls.model import Date
from impuls.multi_file import (
    IntermediateFeed,
    IntermediateFeedProvider,
    prune_outdated_feeds
)
from impuls.resource import HTTPResource, ZippedResource

from kw_sanitizer.consts import GTFS_HEADERS


class LKACombiner(impuls.App):
    def prepare(
        self,
        args: argparse.Namespace,
        options: impuls.PipelineOptions,
    ) -> impuls.multi_file.MultiFile[impuls.Resource]:
        return impuls.multi_file.MultiFile(
            options=options,
            intermediate_provider=SourceFeedProvider(),
            intermediate_pipeline_tasks_factory=lambda feed: [
                impuls.tasks.LoadGTFS(feed.resource_name)
            ],
            final_pipeline_tasks_factory=lambda _: [
                impuls.tasks.SaveGTFS(
                    headers=GTFS_HEADERS,
                    target="out/lka_combined.zip",
                )
            ],
            additional_resources={},
        )


class SourceFeedProvider(IntermediateFeedProvider[ZippedResource]):
    def __init__(self, for_date: Date | None = None) -> None:
        self.for_date = for_date or Date.today()

    def needed(self) -> list[IntermediateFeed[ZippedResource]]:
        URLS = {
            "2023-12-16": "https://kolej-lka.pl/pliki/pn0e6eg45qcl4hd5/gtfs-2023-2024/zip/",
            "2024-12-15": "https://kolej-lka.pl/pliki/pn0e6eg45qcl4hd5/gtfs-2024-2025/zip/",
            "2025-12-14": "https://kolej-lka.pl/pliki/pn0e6eg45qcl4hd5/gtfs-2025-2026/zip/",
        }
        feeds: list[IntermediateFeed[ZippedResource]] = []

        for key, url in URLS.items():
            feed = IntermediateFeed(
                HTTPResource.get(url),
                resource_name=f"lka-{key}.zip",
                version=f"lka-{key}",
                start_date=Date.from_ymd_str(key),
            )
            feeds.append(feed)

        prune_outdated_feeds(feeds, self.for_date)
        return feeds


def main() -> None:
    LKACombiner().run()
