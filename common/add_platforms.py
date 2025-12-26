import impuls
import json
from common.osm_stations import fetch_osm_stations

class AddPlatforms(impuls.Task):
    def __init__(self, name: str | None = None) -> None:
        super().__init__(name)
        osm_data = fetch_osm_stations()
        self.osm_stations_by_name = {elem["tags"].get("name"): elem for elem in osm_data if "tags" in elem}

    @staticmethod
    def load_platforms(path: impuls.tools.types.StrPath) -> None:
        with open(path, "r", encoding="utf-8") as f:
            platforms = json.load(f)
        return platforms
    
    def get_lon_lat_from_osm_with_fallback(self, station: impuls.model.Stop) -> tuple[float, float]:
        osm_station = self.osm_stations_by_name.get(station.name)
        if not osm_station:
            self.logger.error(f"Missing station in OSM: '{station.name}'")
            return station.lon, station.lat
        lat: float = osm_station.get("lat")
        lon: float = osm_station.get("lon")

        return (lon, lat)
        

    def execute(self, r: impuls.TaskRuntime) -> None:
        platforms = self.load_platforms(r.resources["platforms.json"].stored_at)

        stations = r.db.retrieve_all(impuls.model.Stop).all()
        with r.db.transaction():
            for station in stations:
                station.name = station.name.strip()

                lon,lat = self.get_lon_lat_from_osm_with_fallback(station)
                station.lon = lon
                station.lat = lat

                station_platforms = platforms.get(station.name)
                if not station_platforms:
                    self.logger.warning(
                        "Station %s is not in platform list, skipping", station.name
                    )
                    continue

                parent_id = f"{station.id}_parent"
                station.parent_station = parent_id

                ent = [
                    impuls.model.Stop(
                        id=parent_id,
                        name=station.name,
                        lon=lon,
                        lat=lat,
                        location_type=impuls.model.Stop.LocationType.STATION,
                    ),
                    impuls.model.Stop(
                        id=f"{station.id}_BUS",
                        name=station.name,
                        lon=lon,
                        lat=lat,
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
