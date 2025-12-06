import argparse
import os
import zipfile
import impuls
from impuls.model import Date
from impuls.multi_file import (
    IntermediateFeed,
    IntermediateFeedProvider,
    prune_outdated_feeds
)
from impuls.resource import HTTPResource, ZippedResource

from kw_sanitizer.consts import GTFS_HEADERS


class KolejeSlaskieSanitizer(impuls.App):
    def prepare(
        self,
        args: argparse.Namespace,
        options: impuls.PipelineOptions,
    ) -> impuls.multi_file.MultiFile[impuls.Resource]:
        return impuls.multi_file.MultiFile(
            options=options,
            intermediate_provider=SourceFeedProvider(),
            intermediate_pipeline_tasks_factory=lambda feed: [
                FixInitially(feed),
                impuls.tasks.LoadGTFS(feed.resource_name)
            ],
            final_pipeline_tasks_factory=lambda _: [
                impuls.tasks.SaveGTFS(
                    headers=GTFS_HEADERS,
                    target="out/ks.zip",
                )
            ],
            additional_resources={},
        )

class FixInitially(impuls.Task):
    def __init__(self, res: IntermediateFeed) -> None:
        self.res = res 
        super().__init__()

    def execute(self, r: impuls.TaskRuntime) -> None:
        file = r.resources[self.res.resource_name]
        with zipfile.ZipFile(f"fixed-{self.res.version}.out.zip", "w") as zout:
            with zipfile.ZipFile(file.open_binary()) as zin:
                for item in zin.infolist():
                    if item.filename != "shapes.txt":
                        zout.writestr(item, zin.read(item.filename))
                    else:
                        content = zin.read(item.filename).decode("utf-8")
                        processed_lines = [
                            line.replace(" ", "") for line in content.splitlines()
                        ]
                        zout.writestr("shapes.txt", "\n".join(processed_lines))
        os.replace(f"fixed-{self.res.version}.out.zip", file.stored_at)

class SourceFeedProvider(IntermediateFeedProvider[ZippedResource]):
    def __init__(self, for_date: Date | None = None) -> None:
        self.for_date = for_date or Date.today()

    def needed(self) -> list[IntermediateFeed[ZippedResource]]:
        URLS = {
            "2024-12-15": "https://koleje-ks.pl/gtfs/2024-2025.zip",
            "2025-12-14": "https://koleje-ks.pl/gtfs/2025-2026.zip",
        }
        feeds: list[IntermediateFeed[ZippedResource]] = []

        for key, url in URLS.items():
            feed = IntermediateFeed(
                HTTPResource.get(url),
                resource_name=f"ks-{key}.zip",
                version=f"ks-{key}",
                start_date=Date.from_ymd_str(key),
            )
            feeds.append(feed)

        prune_outdated_feeds(feeds, self.for_date)
        return feeds


def main() -> None:
    KolejeSlaskieSanitizer().run()
