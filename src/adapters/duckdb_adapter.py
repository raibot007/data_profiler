import duckdb

class DuckDBAdapter:
    def __init__(self, db_path):
        self.conn = duckdb.connect(db_path)

    def list_tables(self):
        return [t[0] for t in self.conn.execute("SHOW TABLES").fetchall()]

    def get_schema(self, table):
        return self.conn.execute(f"PRAGMA table_info({table})").fetchall()

    def get_row_count(self, table):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def profile_column(self, table, column, config):
        sample_clause = ""

        if config["sampling"]["enabled"]:
            pct = config["sampling"]["percent"]
            sample_clause = f"USING SAMPLE {pct}%"

        query = f"""
        SELECT
            MIN({column}),
            MAX({column}),
            APPROX_COUNT_DISTINCT({column})
        FROM {table}
        {sample_clause}
        """

        return self.conn.execute(query).fetchone()
