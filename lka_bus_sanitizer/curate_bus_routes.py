import impuls
import csv
from typing import List, TypedDict

class Match(TypedDict):
    id: str
    color: str
    stop_a: str
    stop_b: str

class CurateBusRoutes(impuls.Task):
    def execute(self, r):
        self.routes: List[Match] = []
        with open(r.resources["routes_match.csv"].stored_at, mode="r") as f:
            lines = csv.reader(f)
            next(lines)
            for line in lines:
                self.routes.append(
                    Match(
                        id = line[0],
                        color = line[1],
                        stop_a = line[2],
                        stop_b = line[3],
                    )
                )
        
        with r.db.transaction():
            routes = r.db.retrieve_all(impuls.model.Route).all()
            for route in routes:
                if "ZKA" in route.short_name:
                    continue

                candidates: List[Match] = []

                for candidate in self.routes:
                    if candidate["stop_a"] in route.long_name and candidate["stop_b"] in route.long_name:
                        candidates.append(candidate)

                if len(candidates)!=1:
                    if len(set([match["id"] for match in candidates]))>1:
                        self.logger.warning(f"Route has more than one matching: {route}, {candidates}")
                        continue
                
                match = candidates[0]
                route.color = match["color"]
                route.short_name = match["id"]
                r.db.update(route)

                