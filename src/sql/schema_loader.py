"""
Parse DDL from sql/demo_cases.sql to extract table schemas.

Returns:
    SCHEMAS: dict of table_name -> list of (column_name, column_type)
    ALLOWED_TABLES: set of table names (only tables with INSERT data)
    SCHEMA_TEXT: human-readable schema string for LLM prompts
"""

import re
from pathlib import Path

_sql_file = Path(__file__).parent.parent.parent / "sql" / "demo_cases.sql"

SCHEMAS: dict[str, list[tuple[str, str]]] = {}
ALLOWED_TABLES: set[str] = set()
SCHEMA_TEXT: str = ""


def _parse():
    global SCHEMAS, ALLOWED_TABLES, SCHEMA_TEXT

    if not _sql_file.exists():
        return

    content = _sql_file.read_text(encoding="utf-8")

    # Find tables that have INSERT data (only these are queryable)
    insert_pattern = re.compile(r"INSERT\s+INTO\s+(\w+)\s+VALUES", re.IGNORECASE)
    tables_with_data = {m.group(1) for m in insert_pattern.finditer(content)}

    # Match CREATE TABLE blocks
    create_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(?:[\w]+[.])?"  # optional database prefix
        r"(\w+)\s*\((.*?)\)\s*ENGINE\s*=",
        re.DOTALL | re.IGNORECASE,
    )

    for match in create_pattern.finditer(content):
        table_name = match.group(1)
        # Only include tables that have actual data
        if table_name not in tables_with_data:
            continue

        columns_block = match.group(2)

        columns: list[tuple[str, str]] = []
        for line in columns_block.split("\n"):
            line = line.strip().rstrip(",")
            if not line:
                continue
            first_token = line.split()[0]
            if first_token.upper() in ("ORDER", "PARTITION", "PRIMARY", "INDEX", "CONSTRAINT"):
                continue

            parts = line.split(None, 1)
            if len(parts) == 2:
                col_name, col_rest = parts
                col_name = col_name.strip("`")
                # Extract full type including params like Decimal(18, 2)
                depth = 0
                type_end = len(col_rest)
                for i, ch in enumerate(col_rest):
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                    elif ch in (" ", "\t") and depth == 0:
                        type_end = i
                        break
                col_type = col_rest[:type_end].strip()
                # Strip trailing COMMENT
                if "COMMENT" in col_type.upper():
                    col_type = col_type[:col_type.upper().index("COMMENT")].strip()
                columns.append((col_name, col_type))

        if columns:
            SCHEMAS[table_name] = columns
            ALLOWED_TABLES.add(table_name)

    # Build human-readable schema text for LLM prompts
    lines = []
    for tname, cols in sorted(SCHEMAS.items()):
        col_list = ", ".join(f"{c} {tp}" for c, tp in cols)
        lines.append(f"- {tname}({col_list})")
    SCHEMA_TEXT = "\n".join(lines)


_parse()
