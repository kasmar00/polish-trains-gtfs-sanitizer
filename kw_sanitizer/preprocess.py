import os
import zipfile
import impuls

class FixInitially(impuls.Task):
    def __init__(self) -> None:
        super().__init__()

    new_content = """route_id,route_short_name,route_long_name,route_type,agency_id
KW,KW, ,2,KW
"""

    def execute(self, r: impuls.TaskRuntime) -> None:
        file = r.resources["kw.zip"]
        with zipfile.ZipFile("kw.out.zip", "w") as zout:
            with zipfile.ZipFile(file.open_binary()) as zin:
                for item in zin.infolist():
                    if item.filename != "routes.txt":
                        zout.writestr(item, zin.read(item.filename))
                zout.writestr("routes.txt", self.new_content)
        os.replace("kw.out.zip", file.stored_at)