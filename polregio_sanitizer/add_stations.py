import impuls
import re


class GenerateParentsForStops(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            stops = r.db.retrieve_all(impuls.model.Stop)
            stops_to_update = []
            stop_ids_to_create = set()
            parent_ids_found = set()

            replacements = {
                "280983": "63438",
                "265741": "62844",
                "60665": "178452",
                "265740": "62836",
                "178501": "45401",
                "280623": "84509",
            }
            while stop := stops.one():
                id: str = stop.id
                if is_stop_id_parent_id(id):
                    parent_ids_found.add(id)
                    stop.location_type = impuls.model.Stop.LocationType.STATION
                    stops_to_update.append(stop)
                else:
                    parent_id = clear_platform_suffix_from_stop_id(id)
                    stop_ids_to_create.add(parent_id)
                    stop.parent_station = replacements.get(parent_id, parent_id)
                    stop.location_type = impuls.model.Stop.LocationType.STOP
                    stops_to_update.append(stop)

            stop_ids_to_create -= parent_ids_found

            parents = []

            for parent_id in stop_ids_to_create:
                if parent_id in ["280514", "17822", "178501"]:
                    continue
                parents.append(
                    impuls.model.Stop(
                        id=replacements.get(parent_id, parent_id),
                        name="",
                        lat=0.0,
                        lon=0.0,
                        location_type=impuls.model.Stop.LocationType.STATION,
                    )
                )
            
            parents.append(
                    impuls.model.Stop(
                        id="280514",
                        name="Mnichów",
                        lat=50.7079,
                        lon=20.3336,
                        location_type=impuls.model.Stop.LocationType.STATION,
                    )
                )
            parents.append(
                    impuls.model.Stop(
                        id="17822",
                        name="Babi Dół",
                        lat=54.3018,
                        lon=18.2887,
                        location_type=impuls.model.Stop.LocationType.STATION,
                    )
                )

            r.db.create_many(impuls.model.Stop, parents)

            r.db.update_many(impuls.model.Stop, stops_to_update)


def is_stop_id_parent_id(id: str) -> bool:
    """Checks if stop id is a parent stop id by scanning for non numeric charachters"""
    return id.isnumeric()


def clear_platform_suffix_from_stop_id(id: str) -> str:
    if is_stop_id_parent_id(id):
        return id

    prefix = id.split("_")[0]
    return re.sub("[^0-9]", "", prefix)
