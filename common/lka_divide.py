import impuls


class DivideLKARoutes(impuls.Task):
    def __init__(
        self, bus: bool = False, train: bool = False, rail_replacement_bus: bool = False
    ):

        self.bus = bus
        self.train = train
        self.bus_replacement = rail_replacement_bus
        super().__init__(None)

    def execute(self, r):
        with r.db.transaction():

            # General cleanup
            r.db.raw_execute(
                """
                    UPDATE agencies
                    SET agency_id = 'LKA'
                """
            )
            r.db.raw_execute(
                """
                    UPDATE routes
                    SET agency_id = 'LKA'
                """
            )

            r.db.create(
                impuls.model.Agency(
                    id="KWRRB",
                    name="Kolej Wąskotorowa Rogów - Rawa - Biała",
                    url="https://kolejrogowska.pl/",
                    phone="+48 887 796 045",
                    timezone="Europe/Warsaw",
                )
            )

            routes = r.db.retrieve_all(impuls.model.Route).all()

            for route in routes:
                self.update_route(route, r)

    def update_route(self, route: impuls.model.Route, r: impuls.TaskRuntime):
        to_drop = False
        if route.type == impuls.model.Route.Type.RAIL:
            if "1110449" in route.id:
                # kolej rogowska
                route.agency_id = "KWRRB"
                route.short_name = "KWRRB"
                if self.train:
                    r.db.update(route)
                else:
                    to_drop = True
            elif (
                len(route.id) > 12
            ):  # bus stop id's are 6 charachters, with an `_` makes the route.id 13 chars
                # rail replacement bus
                route.type = impuls.model.Route.Type.BUS
                route.short_name = "ZKA ŁKA"
                if self.bus_replacement:
                    r.db.update(route)
                else:
                    to_drop = True
            else:
                # train
                route.short_name = "ŁKA"
                if self.train:
                    r.db.update(route)
                else:
                    to_drop = True

        elif route.type == impuls.model.Route.Type.BUS:
            # bus
            if not self.bus:
                to_drop = True

        if to_drop:
            r.db.raw_execute(
                """
                    DELETE FROM routes
                    WHERE route_id = ?
                """,
                (route.id,),
            )
