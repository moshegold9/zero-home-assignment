import os
from dataclasses import dataclass
from typing import Any

import trino


@dataclass(frozen=True)
class TrinoConfig:
    host: str
    port: int
    user: str

class TrinoClient:
    """
    Minimal Trino client using the official `trino` Python library (DBAPI).
    """

    def __init__(self) -> None:
        self._config = TrinoConfig(
            host=os.getenv("TRINO_HOST", "localhost"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "trino")
        )

    def execute_sql_script(self, sql_text: str) -> None:
        """
        Execute a semicolon-separated SQL script, skipping `--` comment lines and blanks.
        """
        for raw in sql_text.split(";"):
            stmt = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
            if stmt:
                self.execute(stmt)

    def execute_sql_file(self, sql_path: str) -> None:
        with open(sql_path, "r", encoding="utf-8") as f:
            self.execute_sql_script(f.read())



    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        conn = trino.dbapi.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return []
        finally:
            try:
                cur.close()
            finally:
                conn.close()


