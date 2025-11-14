import impuls

class MergeStopsByNameAndCode(impuls.Task):
    """
    Merge stops that have the same name and stop_code.
    """

    def execute(self, r: impuls.TaskRuntime) -> None:
        with r.db.transaction():
            cursor = r.db.raw_execute(
                """
                SELECT
                    name,
                    code,
                    GROUP_CONCAT(stop_id) AS stop_ids,
                    COUNT(*) AS count
                FROM stops
                GROUP BY name, code
                HAVING count > 1 AND code != '' AND code IS NOT NULL
                """
            )

            for row in cursor.all():
                stop_name = row[0]
                stop_code = row[1]
                stop_ids = row[2].split(",")

                # Keep the first stop_id, remove others
                main_stop_id = stop_ids[0]
                duplicate_stop_ids = stop_ids[1:]

                # Update references in other tables (e.g., stop_times)
                r.db.raw_execute(
                    f"""
                    UPDATE stop_times
                    SET stop_id = ?
                    WHERE stop_id IN ({','.join(['?'] * len(duplicate_stop_ids))})
                    """,
                    tuple([main_stop_id] + duplicate_stop_ids,)
                )

                # Remove duplicate stops
                r.db.raw_execute(
                    f"""
                    DELETE FROM stops
                    WHERE stop_id IN ({','.join(['?'] * len(duplicate_stop_ids))})
                    """,
                    duplicate_stop_ids,
                )
                if not stop_code.isnumeric():
                    r.db.raw_execute(
                        """
                        UPDATE stops
                        SET code = ''
                        WHERE stop_id = ?
                        """,
                        (main_stop_id, )
                    )