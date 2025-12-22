import impuls
import json
import re


class AddPlatformLocations(impuls.Task):
    @staticmethod
    def load_platforms(path: impuls.tools.types.StrPath) -> None:
        with open(path, "r", encoding="utf-8") as f:
            platforms = json.load(f)
        return platforms

    def execute(self, r):
        with r.db.transaction():
            stop_query = r.db.retrieve_all(impuls.model.Stop)

            platform_locations = self.load_platforms(
                r.resources["platforms.json"].stored_at
            )

            stops_to_update = []

            while stop := stop_query.one():
                if stop.platform_code == "BUS" or "_" not in stop.id:
                    continue

                name = stop.name.strip()
                station_platforms = platform_locations.get(name)

                if not station_platforms:
                    self.logger.warning(
                        "Station %s is not in platform list, skipping", name
                    )
                    continue

                try:
                    roman_platform, track = stop.platform_code.split("/")
                    arabic_platform = roman_to_arabic(roman_platform)
                except:
                    self.logger.warning(
                        "Malformed platform_code: stop: %s, platform_code: %s, stop_id: %s",
                        name,
                        stop.platform_code,
                        stop.id,
                    )
                    continue

                platform_match = [
                    x for x in station_platforms if x["platform"] == arabic_platform
                ]

                if len(platform_match) == 0:
                    self.logger.warning(
                        "No matches for %s platform %s in %s",
                        name,
                        arabic_platform,
                        station_platforms,
                    )
                    continue

                if len(platform_match) == 1:
                    platform = platform_match[0]
                else:
                    track_match = [x for x in platform_match if x["track"] == track]

                    if len(track_match) == 0:
                        if track.isnumeric():
                            self.logger.warning(
                                "No matches for %s track %s in %s",
                                name,
                                track,
                                platform_match,
                            )
                            continue
                        clean_track = re.sub("[^0-9]", "", track)
                        clean_track_match = [
                            x for x in platform_match if x["track"] == clean_track
                        ]

                        if len(clean_track_match) == 0:
                            self.logger.warning(
                                "No matches for %s track %s in %s",
                                name,
                                track,
                                platform_match,
                            )
                            continue

                        platform = clean_track_match[0]
                    else:
                        platform = track_match[0]

                if not platform:
                    self.logger.warning("Station %s nas no track %s", name, track)
                    continue

                try:
                    stop.lon = platform["location"][0]
                    stop.lat = platform["location"][1]
                except Exception as e:
                    self.logger.error(
                        "For %s platform exception occured: %s", platform, e
                    )

                stops_to_update.append(stop)

            r.db.update_many(impuls.model.Stop, stops_to_update)


def roman_to_arabic(number: str) -> str:
    mapping = {
        "I": "1",
        "II": "2",
        "III": "3",
        "IIII": "4",
        "IV": "4",
        "V": "5",
        "VI": "6",
        "VII": "7",
        "VIII": "8",
        "VIIII": "9",
        "IX": "9",
        "X": "10",
        "XI": "11",
    }

    if number not in mapping.keys():
        return roman_to_arabic(number[:-1]) + number[-1]

    return mapping[number]
