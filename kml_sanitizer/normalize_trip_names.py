import impuls

class NormalizeTripNames(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            for trip in r.db.retrieve_all(impuls.model.Trip):
                trip.short_name = trip.short_name.split()[0]
                r.db.update(trip)