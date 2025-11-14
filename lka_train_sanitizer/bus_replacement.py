import impuls

class MarkBusReplacementAsBus(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            routes = r.db.retrieve_all(impuls.model.Route).all()
            for route in routes:
                if len(route.id) > 11:
                    route.type = impuls.model.Route.Type.BUS
                    route.short_name = "ZKA ŁKA"
                
            r.db.update_many(impuls.model.Route, routes)
                    