from typing import List, Tuple
import impuls


class ApplyPlatformsFromHeadsigns(impuls.Task):
    def execute(self, r):
        with r.db.transaction():

            available_platforms = r.db.retrieve_all(impuls.model.Stop).all()

            with r.db.retrieve_all(impuls.model.StopTime) as query:
                for stop_time in query.all():
                    parsed = parse_headsign(stop_time.stop_headsign)
                    if not parsed:
                        self.logger.warning("Unable to parse headsign %s", stop_time.stop_headsign)
                        continue

                    platform, track = parsed
                    matched_stop = match_platform(
                        available_platforms, stop_time.stop_id, platform, track
                    )

                    if not matched_stop:
                        self.logger.error(
                            "No matching platform/track for %s on station %s",
                            stop_time.stop_headsign,
                            stop_time.stop_id,
                        )
                        continue

                    stop_time.stop_id = matched_stop.id
                    stop_time.platform = matched_stop.platform_code

                    try:
                        r.db.update(stop_time)
                    except:
                        self.logger.error(
                            "Error while updating stop_id to %s for heading %s",
                            stop_time.stop_id,
                            stop_time.stop_headsign,
                        )
                


def match_platform(
    available_platforms: List[impuls.model.Stop], stop_id, platform, track
) -> impuls.model.Stop | None:
    station_platforms = [
        x
        for x in available_platforms
        if stop_id in x.id and x.platform_code.startswith(str(platform))
    ]

    if len(station_platforms) == 1:
        return station_platforms[0]

    for x in station_platforms:
        if x.platform_code.endswith(track):
            return x

    return None


def parse_headsign(headsign: str) -> Tuple[int, int]:
    """
    headsign is formatted as: "peron I, tor 7", "peron IV, tor 2" or "BUS"
    """
    if "BUS" in headsign:
        return ("BUS", None)
    if "," not in headsign:
        return None
    platform_part, track_part = headsign.split(",")
    platform = roman_to_arabic[platform_part.split()[1]]
    track = track_part.split()[1]
    return (platform, track)


roman_to_arabic = {
    "I": 1,
    "II": 2,
    "III": 3,
    "IV": 4,
    "V": 5,
    "VI": 6,
    "VII": 7,
    "VIII": 8,
    "IX": 9,
    "X": 10,
}
