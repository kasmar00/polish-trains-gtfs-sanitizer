import impuls
import re

class CurateStopNames(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            stops = r.db.retrieve_all(impuls.model.Stop).all()

            for stop in stops:
                
                stop.name = re.sub(" [Kk]ier.*", "", stop.name)
                r.db.update(stop)