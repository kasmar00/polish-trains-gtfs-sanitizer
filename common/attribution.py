from zoneinfo import ZoneInfo
import impuls

POLAND_TZ = ZoneInfo("Europe/Warsaw")


class CreateFeedAttributions(impuls.Task):
    def __init__(self, operator_name, operator_url, feed_resource_name):
        self.name = (operator_name,)
        self.url = operator_url
        self.feed = feed_resource_name
        super().__init__()

    def execute(self, r):
        with r.db.transaction():
            source_timestamp = r.resources[self.feed].last_modified.astimezone(
                POLAND_TZ
            )
            r.db.update(
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
                        is_producer=True,
                        url="https://openstreetmap.org/copyright",
                    ),
                    impuls.model.Attribution(
                        id=2,
                        organization_name=self.name,
                        is_operator=True,
                        url=self.url,
                    ),
                    impuls.model.Attribution(
                        id="3",
                        organization_name="Marcin Kasznia",
                        is_producer=True,
                        url="https://gtfs.kasznia.net",
                    ),
                ],
            )
