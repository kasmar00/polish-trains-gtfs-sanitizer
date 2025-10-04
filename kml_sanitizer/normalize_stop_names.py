from string import capwords
import impuls


class NormalizeStopNames(impuls.Task):
    def execute(self, r):
        with r.db.transaction():
            for stop in r.db.retrieve_all(impuls.model.Stop):
                stop.name = " ".join(
                    [capwords(word, sep="-") for word in stop.name.split()]
                ).replace("Koło", "koło")
                r.db.update(stop)
