import re
import sqlglot
from dataclasses import dataclass

from src.sql.schema_loader import ALLOWED_TABLES

# DDL/DML keywords that are not allowed
_FORBIDDEN_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "CREATE", "TRUNCATE", "GRANT", "REVOKE",
}

# SQL timeout in seconds
SQL_TIMEOUT = 30


@dataclass
class ValidationResult:
    success: bool
    errors: list[str]
    sanitized_sql: str | None = None


class SqlValidator:
    """Validate and sanitize SQL queries using keyword checks and AST parsing."""

    @staticmethod
    def validate(sql: str) -> ValidationResult:
        errors: list[str] = []

        # 1. Keyword check: reject DDL/DML
        upper_sql = sql.strip().upper()
        for keyword in _FORBIDDEN_KEYWORDS:
            # Match keyword at start or after whitespace/semicolon
            if re.search(rf"(?:^|[\s;]){keyword}\b", upper_sql):
                errors.append(f"Keyword '{keyword}' is not allowed")
        if errors:
            return ValidationResult(success=False, errors=errors)

        # 2. AST parsing via sqlglot for table whitelist
        try:
            ast = sqlglot.parse_one(sql, dialect="clickhouse")
        except sqlglot.errors.ParseError as e:
            return ValidationResult(success=False, errors=[f"SQL parse error: {e}"])

        # Extract CTE alias names (WITH clause temporary tables)
        cte_names = {
            node.alias if isinstance(node, sqlglot.exp.CTE)
            else node.name for node in ast.walk()
            if isinstance(node, (sqlglot.exp.CTE,))
        }

        # Extract table names from AST
        tables = {
            node.name for node in ast.walk()
            if isinstance(node, sqlglot.exp.Table) and node.name
        }

        # Exclude CTE aliases from whitelist check
        tables -= cte_names

        # Whitelist check
        for table in tables:
            if table not in ALLOWED_TABLES:
                errors.append(f"Table '{table}' is not in the allowed tables list")
        if errors:
            return ValidationResult(success=False, errors=errors)

        # 3. Reject SELECT without WHERE and LIMIT (full table scan)
        is_select = isinstance(ast, sqlglot.exp.Select)
        if is_select:
            has_where = ast.find(sqlglot.exp.Where) is not None
            has_limit = ast.find(sqlglot.exp.Limit) is not None
            if not has_where and not has_limit:
                return ValidationResult(
                    success=False,
                    errors=["SELECT without WHERE and LIMIT is not allowed (full table scan risk)"],
                )

        # 4. Auto-add LIMIT if missing
        sanitized_sql = sql
        if is_select and not has_limit:
            sanitized_sql = f"{sql.rstrip(';').rstrip()} LIMIT 1000"

        return ValidationResult(success=True, errors=[], sanitized_sql=sanitized_sql)
