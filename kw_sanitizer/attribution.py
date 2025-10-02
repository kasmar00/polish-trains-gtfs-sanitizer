import impuls
from kw_sanitizer.consts import POLAND_TZ


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
                        is_producer=True,
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