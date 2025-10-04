import impuls
import json


class AddPlatforms(impuls.Task):
    @staticmethod
    def load_platforms(path: impuls.tools.types.StrPath) -> None:
        with open(path, "r", encoding="utf-8") as f:
            platforms = json.load(f)
        return platforms

    def execute(self, r):
        platforms = self.load_platforms(r.resources["platforms.json"].stored_at)

        stations = r.db.retrieve_all(impuls.model.Stop).all()
        with r.db.transaction():
            for station in stations:
                station.name = station.name.strip()

                station_platforms = platforms.get(station.name)
                if not station_platforms:
                    self.logger.warning(
                        "Station %s is not in platform list, skipping", station.name
                    )
                    continue

                parent_id = f"{station.id}_parent"
                station.parent_station = parent_id

                # TODO: fix parent, bus and "main" station locations to be from OSM

                ent = [
                    impuls.model.Stop(
                        id=parent_id,
                        name=station.name,
                        lon=station.lon,
                        lat=station.lat,
                        location_type=impuls.model.Stop.LocationType.STATION,
                    ),
                    impuls.model.Stop(
                        id=f"{station.id}_BUS",
                        name=station.name,
                        lon=station.lon,
                        lat=station.lat,
                        platform_code="BUS",
                        parent_station=parent_id,
                    ),
                ]
                for platform in station_platforms:
                    if not platform["location"]:
                        platform["location"] = [station.lat, station.lon]
                    ent.append(
                        impuls.model.Stop(
                            id=f"{station.id}_{platform['platform']}_{platform['track']}",
                            name=station.name,
                            lon=platform["location"][0],
                            lat=platform["location"][1],
                            platform_code=f"{platform['platform']} / {platform['track']}",
                            parent_station=parent_id,
                        )
                    )
                r.db.create_many(impuls.model.Stop, ent)

            r.db.update_many(impuls.model.Stop, stations)
