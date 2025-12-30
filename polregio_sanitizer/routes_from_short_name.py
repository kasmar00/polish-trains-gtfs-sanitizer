import impuls

class MarkRoutesFromShortName(impuls.Task):
    routes = {
        "PKM1": "WLKP-PKM1",
        "PKM2": "WLKP-PKM2",
        "PKM3": "WLKP-PKM3",
        "PKM4": "WLKP-PKM4",
        "PKM5": "WLKP-PKM5",
        "SKA1": "MPOL-SKA1",
        "SKA2": "MPOL-SKA2",
        "SKA3": "MPOL-SKA3",
        "K2": "MPOL-K2",
        "K22": "MPOL-K22",
        "K3": "MPOL-K3",
        "K32": "MPOL-K32",
        "K33": "MPOL-K33",
        "K5": "MPOL-K5",
        "K51": "MPOL-K51",
        "K52": "MPOL-K52",
        "K63": "MPOL-K63",
        "K7": "MPOL-K7",
        "K71": "MPOL-K71",
        "PKR": "MPOL-PKR",
        "S1": "ZACH-S1",
        "S2": "ZACH-S2",
        "S3": "ZACH-S3",
        "PKA": "PODK-PKA",
    }

    def execute(self, r):
        with r.db.transaction():
            for route_code, route_id in self.routes.items():
                r.db.create(
                    impuls.model.Route(
                        id=route_id,
                        agency_id=0,
                        short_name=route_code,
                        long_name=route_code,
                        type=impuls.model.route.Route.Type.RAIL,
                    )
                )
                r.db.create(
                    impuls.model.Route(
                        id=f"{route_id}_BUS",
                        agency_id=0,
                        short_name=route_code,
                        long_name=route_code,
                        type=impuls.model.route.Route.Type.BUS,
                    )
                )
            for trip in r.db.retrieve_all(impuls.model.Trip):
                name_cut = trip.short_name.split()
                clear_name = False
                if len(name_cut) == 1:
                    continue
                else:
                    if "Podkarpacka Kolej Aglomeracyjna" in trip.short_name:
                        route_code = "PKA"
                        clear_name = True
                    elif "Podhalańska Kolej Regionalna" in trip.short_name:
                        route_code = "PKR"
                        clear_name = True
                    else:
                        route_code = name_cut[1]
                route_id = self.routes.get(route_code)
                if route_id:
                    if "BUS" in trip.route_id:
                        trip.route_id = f"{route_id}_BUS"
                    else:
                        trip.route_id = route_id
                    if clear_name:
                        trip.short_name = name_cut[0]
                    else:
                        trip.short_name = (
                            name_cut[0] + " " + " ".join(name_cut[2:])
                        ).strip()

                    r.db.update(trip)