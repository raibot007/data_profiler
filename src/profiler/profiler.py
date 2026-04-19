from concurrent.futures import ThreadPoolExecutor
from utils.helpers import normalize_type

class Profiler:
    def __init__(self, adapter, config):
        self.adapter = adapter
        self.config = config

    def profile_table(self, table):
        schema = self.adapter.get_schema(table)
        row_count = self.adapter.get_row_count(table)

        columns = []

        with ThreadPoolExecutor(max_workers=self.config["workers"]) as executor:
            futures = []

            for col in schema:
                col_name = col[1]

                futures.append(
                    executor.submit(
                        self.adapter.profile_column,
                        table,
                        col_name,
                        self.config
                    )
                )

            for col, future in zip(schema, futures):
                stats = future.result()

                columns.append({
                    "name": col[1],
                    "type": normalize_type(col[2]),
                    "nullable": not col[3],
                    "stats": {
                        "min": stats[0],
                        "max": stats[1],
                        "distinct_count": stats[2]
                    }
                })

        return {
            "table": table,
            "row_count": row_count,
            "columns": columns
        }
