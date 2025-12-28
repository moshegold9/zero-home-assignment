import os
from dataclasses import dataclass
import re
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
        rendered = self._render_sql(sql_text)
        for raw in rendered.split(";"):
            stmt = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
            if stmt:
                self.execute(stmt)

    def execute_sql_file(self, sql_path: str, variables: dict[str, str] | None = None) -> None:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_text = f.read()
        if variables:
            sql_text = self._render_sql(sql_text, variables)
        self.execute_sql_script(sql_text)

    def _render_sql(self, sql_text: str, variables: dict[str, str] | None = None) -> str:
        """
        Replace ${VAR} placeholders in SQL using os.environ merged with `variables`.
        Raises if a placeholder has no value.
        """
        mapping: dict[str, str] = dict(os.environ)
        if variables:
            mapping.update({k: str(v) for k, v in variables.items()})

        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in mapping or mapping[key] == "":
                raise ValueError(f"Missing SQL template variable: {key}")
            return mapping[key]

        return re.sub(r"\$\{([A-Z0-9_]+)\}", repl, sql_text)



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


