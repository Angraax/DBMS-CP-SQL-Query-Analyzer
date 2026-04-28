"""
SQL Query Analyzer v2.0 - Flask Backend
========================================
Enhanced with: database upload/switching, schema viewer,
optimized query generation, and split-screen UI.

Tech: Flask + SQLite
"""

import os
import re
import sqlite3
import shutil
from flask import Flask, render_template, request, jsonify
from werkzeug.utils import secure_filename

# ─────────────────────────────────────────────────────────────
# Flask App Configuration
# ─────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB max upload

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(BASE_DIR, "analyzer.db")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
ALLOWED_EXTENSIONS = {".db", ".sqlite", ".sqlite3"}

# Ensure uploads directory exists
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Track currently selected database (per-server; simple approach)
current_db = {"path": DEFAULT_DB, "name": "Default Sample Database"}


# ─────────────────────────────────────────────────────────────
# Database Helpers
# ─────────────────────────────────────────────────────────────
def get_db(db_path=None):
    """Get a database connection with row factory enabled."""
    path = db_path or current_db["path"]
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """
    Create the default SQLite database, tables, and populate with sample data.
    Runs automatically on first launch.
    """
    conn = sqlite3.connect(DEFAULT_DB)
    cursor = conn.cursor()

    # Create 'users' table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
    """)

    # Create 'orders' table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            product TEXT NOT NULL,
            amount INTEGER NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    # Populate 'users' (only if empty)
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            [
                (1, "Alice Johnson", "alice@example.com"),
                (2, "Bob Smith", "bob@example.com"),
                (3, "Charlie Brown", "charlie@example.com"),
                (4, "Diana Prince", "diana@example.com"),
                (5, "Ethan Hunt", "ethan@example.com"),
                (6, "Fiona Gallagher", "fiona@example.com"),
                (7, "George Miller", "george@example.com"),
            ],
        )

    # Populate 'orders' (only if empty)
    cursor.execute("SELECT COUNT(*) FROM orders")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO orders (id, user_id, product, amount) VALUES (?, ?, ?, ?)",
            [
                (1, 1, "Laptop", 1200),
                (2, 1, "Mouse", 25),
                (3, 2, "Keyboard", 75),
                (4, 3, "Monitor", 450),
                (5, 4, "Headphones", 150),
                (6, 5, "Webcam", 90),
                (7, 2, "USB Hub", 35),
                (8, 6, "Desk Lamp", 60),
            ],
        )

    conn.commit()
    conn.close()
    print(f"[OK] Default database initialized at: {DEFAULT_DB}")


# ─────────────────────────────────────────────────────────────
# Schema Extraction
# ─────────────────────────────────────────────────────────────
def get_schema(db_path=None):
    """
    Fetch the full schema from the given (or current) database.
    Returns a list of tables, each with columns, types, and index info.
    """
    conn = get_db(db_path)
    cursor = conn.cursor()
    schema = []

    try:
        # Get all table names
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [row["name"] for row in cursor.fetchall()]

        for table in tables:
            # Get column info
            cursor.execute(f"PRAGMA table_info({table})")
            columns = []
            for col in cursor.fetchall():
                columns.append({
                    "name": col["name"],
                    "type": col["type"] or "BLOB",
                    "pk": bool(col["pk"]),
                    "notnull": bool(col["notnull"]),
                })

            # Get index info
            cursor.execute(f"PRAGMA index_list({table})")
            indexes = []
            for idx in cursor.fetchall():
                idx_name = idx["name"]
                cursor.execute(f"PRAGMA index_info({idx_name})")
                idx_columns = [ic["name"] for ic in cursor.fetchall()]
                indexes.append({
                    "name": idx_name,
                    "columns": idx_columns,
                    "unique": bool(idx["unique"]),
                })

            schema.append({
                "table": table,
                "columns": columns,
                "indexes": indexes,
            })
    except sqlite3.Error as e:
        schema = [{"error": str(e)}]
    finally:
        conn.close()

    return schema


def _get_table_columns_map(db_path=None):
    """
    Return a dict: { table_name: [col1, col2, ...] }
    Used for optimized query generation.
    """
    schema = get_schema(db_path)
    result = {}
    for table_info in schema:
        if "table" in table_info:
            result[table_info["table"].lower()] = [
                c["name"] for c in table_info["columns"]
            ]
    return result


# ─────────────────────────────────────────────────────────────
# 1. Query Validation
# ─────────────────────────────────────────────────────────────
def validate_query(query: str) -> dict:
    """
    Validate the SQL query for safety.
    Only SELECT queries are allowed.
    """
    if not query or not query.strip():
        return {"valid": False, "error": "Query cannot be empty."}

    cleaned = query.strip().rstrip(";")
    cleaned = re.sub(r"--.*$", "", cleaned, flags=re.MULTILINE).strip()
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL).strip()

    dangerous_keywords = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
        "CREATE", "TRUNCATE", "EXEC", "EXECUTE",
    ]
    first_word = cleaned.split()[0].upper() if cleaned.split() else ""

    if first_word != "SELECT":
        return {
            "valid": False,
            "error": f"Only SELECT queries are allowed. Detected: '{first_word}'.",
        }

    upper_query = cleaned.upper()
    for kw in dangerous_keywords:
        if re.search(rf"\b{kw}\b", upper_query):
            return {
                "valid": False,
                "error": f"Forbidden keyword detected: '{kw}'. Only read-only SELECT queries are permitted.",
            }

    return {"valid": True, "error": None}


# ─────────────────────────────────────────────────────────────
# 2. Query Analysis Engine
# ─────────────────────────────────────────────────────────────
def analyze_query(query: str) -> dict:
    """
    Perform context-aware static analysis on the SQL query.

    Classification rules:
      ISSUES  (❌) — SELECT *, Cartesian product, invalid structure
      WARNINGS(⚠️) — missing WHERE, full-scan risk, JOIN complexity
      Missing WHERE is ALWAYS a warning, never an issue.
      JOINs are NEVER treated as issues.
    """
    issues = []
    warnings = []
    suggestions = []

    upper = query.upper().strip()
    tables = _extract_tables(query)
    num_tables = len(tables)
    has_where = bool(re.search(r"\bWHERE\b", upper))
    has_join = bool(re.search(r"\bJOIN\b", upper))
    has_limit = bool(re.search(r"\bLIMIT\b", upper))
    has_select_star = bool(re.search(r"\bSELECT\s+\*", upper))

    # ── ISSUES (only clear structural problems) ──
    if has_select_star:
        issues.append(
            "Using <code>SELECT *</code> retrieves all columns, which is "
            "inefficient. Specify only the columns you need."
        )

    # Cartesian product: multiple tables with no JOIN and no WHERE
    if num_tables >= 2 and not has_join and not has_where:
        issues.append(
            f"Possible <strong>Cartesian product</strong> detected across "
            f"{num_tables} tables ({', '.join(tables)}). No JOIN or WHERE "
            f"condition links them."
        )

    # ── WARNINGS (performance concerns, not structural errors) ──
    # Missing WHERE — always a warning, never an issue
    if not has_where:
        if has_limit:
            warnings.append(
                "No <code>WHERE</code> clause detected. A <code>LIMIT</code> "
                "is present, but filtering would improve performance."
            )
        else:
            warnings.append(
                "No <code>WHERE</code> clause detected — query will return "
                "all rows. Risks fetching an excessively large result set."
            )

    # JOIN without ON condition
    if has_join and not re.search(r"\bON\b", upper):
        warnings.append(
            "A <code>JOIN</code> without an <code>ON</code> condition may "
            "cause a Cartesian product."
        )

    # JOIN complexity (informational, not an error)
    join_count = len(re.findall(r"\bJOIN\b", upper))
    if join_count > 1:
        warnings.append(
            f"Query uses <strong>{join_count} JOINs</strong>. Ensure all "
            f"join columns are indexed for best performance."
        )

    subquery_count = upper.count("(SELECT")
    if subquery_count > 0:
        warnings.append(
            f"Detected <strong>{subquery_count}</strong> nested "
            f"subquer{'y' if subquery_count == 1 else 'ies'}. "
            "Consider using JOINs or CTEs instead."
        )

    if re.search(r"LIKE\s+['\"]%", upper):
        warnings.append(
            "Using <code>LIKE</code> with a leading wildcard prevents "
            "index usage."
        )

    or_count = len(re.findall(r"\bOR\b", upper))
    if or_count >= 2:
        warnings.append(
            f"Multiple <code>OR</code> conditions ({or_count}) detected. "
            "Consider <code>IN (...)</code> or <code>UNION</code>."
        )

    # ── SUGGESTIONS ──
    if has_select_star and tables:
        suggestions.append(
            f"Replace <code>SELECT *</code> with specific column names "
            f"from <code>{', '.join(tables)}</code>."
        )

    if not has_where:
        suggestions.append(
            "Add a <code>WHERE</code> clause to filter rows."
        )

    where_columns = _extract_where_columns(query)
    for col_info in where_columns:
        suggestions.append(
            f"Consider adding an index on column "
            f"<code>{col_info}</code>."
        )

    if not has_limit:
        suggestions.append(
            "Consider adding a <code>LIMIT</code> clause."
        )

    if re.search(r"\bORDER BY\b", upper) and not has_limit:
        suggestions.append(
            "Using <code>ORDER BY</code> without <code>LIMIT</code> "
            "sorts the entire result set."
        )

    # Context-aware summary for well-written queries
    if not issues and not warnings:
        suggestions.insert(0,
            "Query is structurally correct. Minor performance "
            "improvements may still be possible."
        )

    return {
        "issues": issues, "warnings": warnings,
        "suggestions": suggestions,
    }


def _extract_tables(query: str) -> list:
    """Extract table names referenced in FROM and JOIN clauses."""
    tables = []
    upper = query.upper()
    from_match = re.search(
        r"\bFROM\s+(.+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bJOIN\b|$)",
        upper, re.DOTALL,
    )
    if from_match:
        for part in from_match.group(1).split(","):
            tname = part.strip().split()[0] if part.strip() else ""
            if tname and tname != "(SELECT":
                tables.append(tname.lower())

    join_matches = re.findall(r"\bJOIN\s+(\w+)", upper)
    tables.extend([t.lower() for t in join_matches])
    return list(dict.fromkeys(tables))


def _extract_where_columns(query: str) -> list:
    """Extract column names used in WHERE conditions for index suggestions."""
    columns = []
    where_match = re.search(
        r"\bWHERE\b\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        query, re.IGNORECASE | re.DOTALL,
    )
    if where_match:
        col_matches = re.findall(
            r"(\w+)\s*(?:=|!=|<>|>=|<=|>|<|LIKE|IN)\s*",
            where_match.group(1), re.IGNORECASE,
        )
        sql_noise = {"AND", "OR", "NOT", "IS", "NULL", "BETWEEN", "EXISTS", "SELECT", "FROM"}
        for col in col_matches:
            if col.upper() not in sql_noise:
                columns.append(col)
    return list(dict.fromkeys(columns))


# ─────────────────────────────────────────────────────────────
# 3. Execution Plan Analysis
# ─────────────────────────────────────────────────────────────
def get_execution_plan(query: str) -> dict:
    """
    Run EXPLAIN QUERY PLAN and interpret the results.
    Provides actionable, context-aware suggestions per table.
    """
    conn = get_db()
    cursor = conn.cursor()
    raw_plan = []
    interpretation = []

    # Pre-analyse the query to give context-aware SCAN suggestions
    upper_query = query.upper()
    has_join = bool(re.search(r"\bJOIN\b", upper_query))
    has_where = bool(re.search(r"\bWHERE\b", upper_query))

    try:
        cleaned = query.strip().rstrip(";")
        cursor.execute(f"EXPLAIN QUERY PLAN {cleaned}")
        rows = cursor.fetchall()

        for row in rows:
            detail = (
                row["detail"] if "detail" in row.keys()
                else str(row[3])
            )
            raw_plan.append(detail)
            detail_upper = detail.upper()

            if (
                re.search(r"\bSCAN\b", detail_upper)
                and "SEARCH" not in detail_upper
            ):
                table_match = re.search(
                    r"SCAN(?:\s+TABLE)?\s+(\w+)", detail_upper
                )
                table_name = (
                    table_match.group(1).lower()
                    if table_match else "unknown"
                )
                # Contextual suggestion based on query structure
                if has_join:
                    hint = (
                        f" — suggest indexing join column"
                    )
                elif has_where:
                    hint = (
                        f" — suggest indexing filter column"
                    )
                else:
                    hint = ""
                interpretation.append({
                    "type": "warning",
                    "icon": "!!",
                    "message": (
                        f"Full table scan detected on <code>{table_name}</code> "
                        f"— may impact performance on large datasets.{hint}"
                    ),
                })
            elif re.search(r"\bSEARCH\b", detail_upper):
                table_match = re.search(
                    r"SEARCH(?:\s+TABLE)?\s+(\w+)", detail_upper
                )
                table_name = (
                    table_match.group(1).lower()
                    if table_match else "unknown"
                )
                index_match = re.search(
                    r"USING.*?INDEX\s+(\w+)", detail_upper
                )
                pk_match = re.search(
                    r"USING\s+INTEGER\s+PRIMARY\s+KEY", detail_upper
                )
                if index_match:
                    index_name = index_match.group(1).lower()
                elif pk_match:
                    index_name = "INTEGER PRIMARY KEY"
                else:
                    index_name = "primary key"
                interpretation.append({
                    "type": "success",
                    "icon": "OK",
                    "message": (
                        f"<strong>Indexed search</strong> on "
                        f"<code>{table_name}</code> using "
                        f"<code>{index_name}</code>. Efficient."
                    ),
                })
            elif "USE TEMP B-TREE" in detail_upper:
                interpretation.append({
                    "type": "info",
                    "icon": "i",
                    "message": (
                        "A <strong>temporary B-tree</strong> is being "
                        "created (ORDER BY / GROUP BY). An index on the "
                        "sort columns could help."
                    ),
                })
            else:
                interpretation.append({
                    "type": "info",
                    "icon": "i",
                    "message": detail,
                })
    except sqlite3.Error as e:
        raw_plan.append(f"Error: {str(e)}")
        interpretation.append({
            "type": "error",
            "icon": "X",
            "message": (
                f"Failed to generate execution plan: "
                f"<code>{str(e)}</code>"
            ),
        })
    finally:
        conn.close()

    return {"raw": raw_plan, "interpretation": interpretation}


# ─────────────────────────────────────────────────────────────
# 4. Alias & Structure Extraction Helpers
# ─────────────────────────────────────────────────────────────

# SQL keywords that should never be mistaken for a table alias
_SQL_KEYWORDS = {
    "ON", "USING", "WHERE", "GROUP", "ORDER", "LIMIT", "HAVING",
    "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "FULL", "NATURAL",
    "OUTER", "AND", "OR", "NOT", "SET", "SELECT", "FROM", "AS",
    "BY", "ASC", "DESC", "BETWEEN", "IN", "LIKE", "IS", "NULL",
    "CASE", "WHEN", "THEN", "ELSE", "END", "EXISTS", "UNION",
}


def extract_aliases(query: str) -> dict:
    """
    Parse FROM and JOIN clauses to build a table → alias mapping.

    Examples:
        "FROM users"                   → {"users": "users"}
        "FROM users u"                 → {"users": "u"}
        "FROM users AS u"              → {"users": "u"}
        "FROM users u JOIN orders o"   → {"users": "u", "orders": "o"}

    Returns: { table_name_lower: alias_string }
    """
    aliases = {}
    # Normalise whitespace for reliable regex matching
    cleaned = re.sub(r"\s+", " ", query.strip())

    # ── 1) FROM clause: text between FROM and the first JOIN / WHERE / etc. ──
    from_pos = re.search(r"\bFROM\b", cleaned, re.IGNORECASE)
    if from_pos:
        after_from = cleaned[from_pos.end():].strip()
        # Find where the FROM clause ends
        end_match = re.search(
            r"\b(?:(?:INNER|LEFT|RIGHT|CROSS|FULL|NATURAL)\s+)?JOIN\b"
            r"|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b",
            after_from, re.IGNORECASE,
        )
        from_clause = after_from[:end_match.start()].strip() if end_match else after_from.strip()

        # Handle comma-separated tables: FROM users u, orders o
        for part in from_clause.split(","):
            part = part.strip()
            if not part or part.startswith("("):
                continue
            tokens = part.split()
            table = tokens[0]
            if len(tokens) >= 3 and tokens[1].upper() == "AS":
                # FROM users AS u
                aliases[table.lower()] = tokens[2]
            elif len(tokens) >= 2 and tokens[1].upper() not in _SQL_KEYWORDS:
                # FROM users u
                aliases[table.lower()] = tokens[1]
            else:
                # FROM users (no alias)
                aliases[table.lower()] = table.lower()

    # ── 2) JOIN clauses ──
    join_re = (
        r"\b(?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?"
        r"|CROSS\s+|FULL\s+(?:OUTER\s+)?|NATURAL\s+)?JOIN\s+(\w+)"
        r"(?:\s+(?:AS\s+)?(\w+))?"
    )
    for m in re.finditer(join_re, cleaned, re.IGNORECASE):
        table = m.group(1).lower()
        alias_candidate = m.group(2)
        if alias_candidate and alias_candidate.upper() not in _SQL_KEYWORDS:
            aliases[table] = alias_candidate
        else:
            aliases[table] = table

    return aliases


def _has_join(query: str) -> bool:
    """Check if query contains any JOIN clause."""
    return bool(re.search(r"\bJOIN\b", query, re.IGNORECASE))


def _has_where(query: str) -> bool:
    """Check if query contains a WHERE clause."""
    return bool(re.search(r"\bWHERE\b", query, re.IGNORECASE))


def _has_limit(query: str) -> bool:
    """Check if query contains a LIMIT clause."""
    return bool(re.search(r"\bLIMIT\b", query, re.IGNORECASE))


def _has_select_star(query: str) -> bool:
    """Check if query uses SELECT *."""
    return bool(re.search(r"\bSELECT\s+\*", query, re.IGNORECASE))


def is_safe_to_optimize(query: str) -> dict:
    """
    Determine whether optimisation is warranted and what actions are safe.

    Decision matrix:
      ┌─────────────────────────┬───────────────────────────────────────┐
      │ Condition               │ Allowed action                       │
      ├─────────────────────────┼───────────────────────────────────────┤
      │ SELECT *                │ Replace with explicit columns        │
      │ No WHERE + single table │ Add sample WHERE (if 'id' exists)    │
      │ No LIMIT + no JOIN      │ Add LIMIT 100                        │
      │ Has JOIN                │ Do NOT add WHERE or LIMIT            │
      │ Already has WHERE       │ Do NOT touch WHERE                   │
      └─────────────────────────┴───────────────────────────────────────┘

    Returns dict with boolean flags and a reason when no optimisation needed.
    """
    has_join = _has_join(query)
    has_where = _has_where(query)
    has_limit = _has_limit(query)
    has_star = _has_select_star(query)
    tables = _extract_tables(query)
    is_single_table = len(tables) == 1

    # SELECT * can be replaced whenever schema is available (checked later)
    can_replace_star = has_star

    # WHERE clause: only for single-table, non-JOIN queries without existing WHERE
    can_add_where = not has_where and is_single_table and not has_join

    # LIMIT: only for simple non-JOIN queries without existing LIMIT
    can_add_limit = not has_limit and not has_join

    should_optimize = can_replace_star or can_add_where or can_add_limit

    reason = ""
    if not should_optimize:
        reason = "Query is already optimized \u2014 no changes applied"

    return {
        "should_optimize": should_optimize,
        "can_add_where": can_add_where,
        "can_add_limit": can_add_limit,
        "can_replace_star": can_replace_star,
        "reason": reason,
    }


def validate_optimized_query(optimized_sql: str) -> bool:
    """
    Validate an optimised query by running EXPLAIN QUERY PLAN.
    Returns True only if the query produces a non-empty, valid execution plan.
    """
    conn = get_db()
    cursor = conn.cursor()
    try:
        cleaned = optimized_sql.strip().rstrip(";")
        cursor.execute(f"EXPLAIN QUERY PLAN {cleaned}")
        rows = cursor.fetchall()
        return len(rows) > 0
    except sqlite3.Error:
        return False
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
# 4b. Safe Optimized Query Generator
# ─────────────────────────────────────────────────────────────
def generate_optimized_query(query: str) -> dict:
    """
    Generate a safe, context-aware optimised version of the SQL query.

    Safety guarantees:
      1. Respects table aliases — never uses raw table names when aliases exist
      2. Only adds WHERE for single-table queries whose table has an 'id' column
      3. Never adds WHERE or LIMIT to JOIN queries
      4. Never duplicates an existing WHERE clause
      5. Validates the optimised query via EXPLAIN before returning it
      6. Falls back to the original query if optimisation would break SQL

    Returns dict with keys: original, optimized, changes, status, status_message
      status: "optimized" | "already_optimal" | "skipped"
    """
    original = query.strip().rstrip(";")

    # ── Step 1: Check if optimisation is warranted ──
    safety = is_safe_to_optimize(original)
    if not safety["should_optimize"]:
        return {
            "original": query.strip(),
            "optimized": query.strip(),
            "changes": [],
            "status": "already_optimal",
            "status_message": safety["reason"],
        }

    optimized = original
    changes = []

    # ── Step 2: Extract structural info ──
    aliases = extract_aliases(original)
    tables = _extract_tables(original)
    col_map = _get_table_columns_map()
    has_join = _has_join(original)

    # ── Step 3: Replace SELECT * with explicit columns (alias-aware) ──
    if safety["can_replace_star"] and col_map:
        all_columns = []
        for table in tables:
            t_lower = table.lower()
            if t_lower in col_map:
                # Use the alias if one exists, otherwise use the table name
                prefix = aliases.get(t_lower, t_lower)
                if has_join or len(tables) > 1:
                    # Multi-table: always prefix with alias for clarity
                    all_columns.extend(
                        [f"{prefix}.{c}" for c in col_map[t_lower]]
                    )
                else:
                    # Single table: no prefix needed
                    all_columns.extend(col_map[t_lower])

        if all_columns:
            col_str = ", ".join(all_columns)
            optimized = re.sub(
                r"\bSELECT\s+\*",
                f"SELECT {col_str}",
                optimized,
                count=1,
                flags=re.IGNORECASE,
            )
            changes.append(
                f"Replaced <code>SELECT *</code> with specific columns: "
                f"<code>{col_str}</code>"
            )

    # ── Step 4: Add WHERE clause (single-table only, requires 'id' column) ──
    if safety["can_add_where"]:
        target_table = tables[0].lower() if tables else None
        # Only proceed if the table actually has an 'id' column
        if (
            target_table
            and target_table in col_map
            and "id" in [c.lower() for c in col_map[target_table]]
        ):
            # For single-table queries, no prefix is needed
            condition = "id > 0"

            # Insert WHERE before GROUP BY / ORDER BY / LIMIT, or at the end
            insert_patterns = [
                r"\bGROUP\s+BY\b", r"\bORDER\s+BY\b", r"\bLIMIT\b"
            ]
            inserted = False
            for pat in insert_patterns:
                match = re.search(pat, optimized, re.IGNORECASE)
                if match:
                    pos = match.start()
                    optimized = (
                        optimized[:pos]
                        + f"WHERE {condition} "
                        + optimized[pos:]
                    )
                    inserted = True
                    break
            if not inserted:
                optimized = optimized + f" WHERE {condition}"

            changes.append(
                f"Added sample <code>WHERE</code> clause: "
                f"<code>{condition}</code>"
            )

    # ── Step 5: Add LIMIT (non-JOIN queries only) ──
    if safety["can_add_limit"]:
        optimized = optimized + " LIMIT 100"
        changes.append(
            "Added <code>LIMIT 100</code> to prevent unbounded result sets"
        )

    # ── Step 6: If no changes were actually made, return as already optimal ──
    if not changes:
        return {
            "original": query.strip(),
            "optimized": query.strip(),
            "changes": [],
            "status": "already_optimal",
            "status_message": "Query is already optimized \u2014 no changes applied",
        }

    optimized = optimized.strip() + ";"

    # ── Step 7: Validate the optimised query via EXPLAIN ──
    # If the optimised SQL is invalid, discard it and return the original.
    if not validate_optimized_query(optimized):
        return {
            "original": query.strip(),
            "optimized": query.strip(),
            "changes": [],
            "status": "skipped",
            "status_message": (
                "Optimization skipped to prevent invalid query"
            ),
        }

    return {
        "original": query.strip(),
        "optimized": optimized,
        "changes": changes,
        "status": "optimized",
        "status_message": "Optimized Query Generated",
    }


def compute_complexity_score(query: str, plan_raw: list = None) -> dict:
    """
    Compute a context-aware complexity score.

    Scoring rules:
      SELECT *              → +2
      Missing WHERE + SCAN  → +1  (only when full scan is present)
      Single JOIN           → +1
      Multiple JOINs (>1)   → +2
      Subquery              → +2 each
      Full table scan       → +3 each (from EXPLAIN output)

    Classification: 0-2 LOW, 3-5 MEDIUM, 6+ HIGH
    """
    score = 0
    breakdown = []
    upper = query.upper()

    # Count scans up-front so we can use the info in WHERE scoring
    scan_count = 0
    if plan_raw:
        scan_count = sum(
            1 for line in plan_raw
            if "SCAN" in line.upper() and "SEARCH" not in line.upper()
        )

    # ── SELECT * ──
    if re.search(r"\bSELECT\s+\*", upper):
        score += 2
        breakdown.append({
            "rule": "SELECT *", "points": 2,
            "detail": "Retrieving all columns is inefficient",
        })

    # ── Missing WHERE — only penalise when a full scan confirms it matters ──
    has_where = bool(re.search(r"\bWHERE\b", upper))
    if not has_where and scan_count > 0:
        score += 1
        breakdown.append({
            "rule": "Missing WHERE", "points": 1,
            "detail": "No filter with full table scan present",
        })

    # ── JOINs — single join is lightweight, multiple are heavier ──
    join_count = len(re.findall(r"\bJOIN\b", upper))
    if join_count == 1:
        score += 1
        breakdown.append({
            "rule": "JOIN (×1)", "points": 1,
            "detail": "Single table join — minimal overhead",
        })
    elif join_count > 1:
        score += 2
        breakdown.append({
            "rule": f"JOIN (×{join_count})", "points": 2,
            "detail": "Multiple joins increase complexity",
        })

    # ── Subqueries ──
    subquery_count = upper.count("(SELECT")
    if subquery_count > 0:
        pts = 2 * subquery_count
        score += pts
        breakdown.append({
            "rule": f"Subquery (×{subquery_count})", "points": pts,
            "detail": "Nested subqueries add execution overhead",
        })

    # ── Full table scans (from EXPLAIN output) ──
    if scan_count > 0:
        pts = 3 * scan_count
        score += pts
        breakdown.append({
            "rule": f"Full Table Scan (×{scan_count})",
            "points": pts,
            "detail": "SCAN detected in execution plan",
        })

    # ── Classification ──
    if score <= 2:
        level, color, emoji = "LOW", "#22c55e", "\U0001f7e2"
        description = "Simple query with minimal performance impact."
    elif score <= 5:
        level, color, emoji = "MEDIUM", "#f59e0b", "\U0001f7e1"
        description = "Moderate complexity — review suggestions."
    else:
        level, color, emoji = "HIGH", "#ef4444", "\U0001f534"
        description = "Complex query — optimization recommended."

    return {
        "score": score, "level": level, "color": color,
        "emoji": emoji, "description": description,
        "breakdown": breakdown,
    }


def _extract_join_conditions(query: str) -> list:
    """
    Extract column pairs from JOIN ... ON conditions.

    Example:
        "JOIN orders o ON u.id = o.user_id"
        → [("u", "id", "o", "user_id")]

    Returns list of (left_alias, left_col, right_alias, right_col).
    """
    conditions = []
    # Match: ON <alias>.<col> = <alias>.<col>
    pattern = r"\bON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)"
    for m in re.finditer(pattern, query, re.IGNORECASE):
        conditions.append((
            m.group(1).lower(), m.group(2).lower(),
            m.group(3).lower(), m.group(4).lower(),
        ))
    return conditions


def generate_index_suggestion(query: str, plan_raw: list = None) -> dict:
    """
    Smart index recommendation engine.

    Strategy:
      1. Identify tables being full-scanned (SCAN in EXPLAIN)
      2. Check if scanned tables participate in JOIN ON conditions
         → suggest index on the join column of the scanned table
      3. Check if scanned tables are filtered via WHERE
         → suggest index on the WHERE column
      4. Fallback: if a table is scanned but has no WHERE/JOIN column,
         suggest indexing its most commonly useful column
      5. NEVER report "no index issues" when SCANs are present
    """
    suggestions = []
    tables = _extract_tables(query)
    where_columns = _extract_where_columns(query)

    # ── 1) Identify full-scanned tables from EXPLAIN ──
    scanned_tables = set()
    if plan_raw:
        for line in plan_raw:
            line_upper = line.upper()
            if "SCAN" in line_upper and "SEARCH" not in line_upper:
                match = re.search(
                    r"SCAN(?:\s+TABLE)?\s+(\w+)", line_upper
                )
                if match:
                    scanned_tables.add(match.group(1).lower())

    col_map = _get_table_columns_map()
    aliases = extract_aliases(query)
    # Build reverse map: alias → table_name
    alias_to_table = {v.lower(): k for k, v in aliases.items()}

    seen_sql = set()

    def _add(table, column, reason):
        existing_cols = check_existing_indexes(table)
        if column.lower() in existing_cols:
            suggestions.append({
                "table": table, "column": column,
                "sql": None, "reason": f"Index already exists on {column} — no action needed",
            })
            return

        idx_name = f"idx_{table}_{column}"
        sql = f"CREATE INDEX {idx_name} ON {table}({column});"
        if sql not in seen_sql:
            seen_sql.add(sql)
            suggestions.append({
                "table": table, "column": column,
                "sql": sql, "reason": reason,
            })

    # ── 2) JOIN condition analysis: index the FK side on scanned tables ──
    join_conditions = _extract_join_conditions(query)
    for left_alias, left_col, right_alias, right_col in join_conditions:
        # Resolve aliases to real table names
        left_table = alias_to_table.get(left_alias, left_alias)
        right_table = alias_to_table.get(right_alias, right_alias)

        # Suggest index on whichever side is being scanned
        if left_table in scanned_tables and left_table in col_map:
            if left_col in [c.lower() for c in col_map[left_table]]:
                _add(left_table, left_col,
                     f"Column '{left_col}' used in JOIN condition "
                     f"on '{left_table}' which has a full table scan")
        if right_table in scanned_tables and right_table in col_map:
            if right_col in [c.lower() for c in col_map[right_table]]:
                _add(right_table, right_col,
                     f"Column '{right_col}' used in JOIN condition "
                     f"on '{right_table}' which has a full table scan")

    # ── 3) WHERE column analysis: index filtered columns on scanned tables ──
    for col in where_columns:
        col_lower = col.lower()
        # Handle alias.column notation
        if "." in col_lower:
            alias_part, col_part = col_lower.split(".", 1)
            resolved_table = alias_to_table.get(alias_part, alias_part)
            if resolved_table in scanned_tables and resolved_table in col_map:
                if col_part in [c.lower() for c in col_map[resolved_table]]:
                    _add(resolved_table, col_part,
                         f"Column '{col_part}' used in WHERE on "
                         f"'{resolved_table}' which has a full table scan")
        else:
            for table in tables:
                t_lower = table.lower()
                if t_lower in col_map and col_lower in [
                    c.lower() for c in col_map[t_lower]
                ]:
                    if t_lower in scanned_tables or not plan_raw:
                        _add(t_lower, col_lower,
                             f"Column '{col}' used in WHERE on "
                             f"'{t_lower}' which has a full table scan")

    # ── 4) Fallback: scanned tables with no specific column suggestion ──
    if not suggestions and scanned_tables:
        for t in scanned_tables:
            if t in col_map and len(col_map[t]) > 1:
                # Pick the second column (first non-id) as a general hint
                first_col = (
                    col_map[t][1]
                    if col_map[t][0].lower() == "id"
                    else col_map[t][0]
                )
                _add(t, first_col.lower(),
                     f"Full table scan on '{t}' — consider indexing "
                     f"frequently filtered columns")

    return {
        "suggestions": suggestions,
        "has_suggestions": len(suggestions) > 0,
    }


# ─────────────────────────────────────────────────────────────
# 7. Performance Comparison
# ─────────────────────────────────────────────────────────────
def compare_performance(query: str, optimized_data: dict) -> dict:
    """
    Compare EXPLAIN QUERY PLAN of original vs optimised query.
    Gracefully handles cases where optimisation was skipped or
    the query is already optimal — avoids showing empty "No plan data".

    Args:
        query:          The original SQL query string.
        optimized_data: The full dict returned by generate_optimized_query().
    """
    status = optimized_data.get("status", "optimized")
    status_message = optimized_data.get(
        "status_message", "No optimization applied"
    )

    # If optimisation did not produce a different query, skip comparison
    if status != "optimized":
        return {
            "original": [],
            "optimized": [],
            "improved": False,
            "improvements": ["Comparison skipped — query is already structurally optimized. No alternative execution plan generated."],
            "skipped": True,
        }

    optimized_query = optimized_data["optimized"]

    conn = get_db()
    cursor = conn.cursor()

    def _get_plan_lines(sql):
        try:
            cleaned = sql.strip().rstrip(";")
            cursor.execute(f"EXPLAIN QUERY PLAN {cleaned}")
            rows = cursor.fetchall()
            return [
                row["detail"] if "detail" in row.keys() else str(row[3])
                for row in rows
            ]
        except sqlite3.Error:
            return []

    original_plan = _get_plan_lines(query)
    optimized_plan = _get_plan_lines(optimized_query)
    conn.close()

    # Classify each plan line into scan / search / info
    def _classify(lines):
        result = []
        for line in lines:
            upper = line.upper()
            if "SCAN" in upper and "SEARCH" not in upper:
                table_match = re.search(
                    r"SCAN(?:\s+TABLE)?\s+(\w+)", upper
                )
                tname = (
                    table_match.group(1).lower()
                    if table_match
                    else "unknown"
                )
                result.append({
                    "text": f"SCAN {tname}",
                    "type": "scan",
                    "icon": "\u274c",
                    "label": "Full Table Scan",
                })
            elif "SEARCH" in upper:
                table_match = re.search(
                    r"SEARCH(?:\s+TABLE)?\s+(\w+)", upper
                )
                tname = (
                    table_match.group(1).lower()
                    if table_match
                    else "unknown"
                )
                idx_match = re.search(
                    r"USING.*?INDEX\s+(\w+)", upper
                )
                idx = (
                    idx_match.group(1).lower()
                    if idx_match
                    else "primary key"
                )
                result.append({
                    "text": f"SEARCH {tname} USING INDEX ({idx})",
                    "type": "search",
                    "icon": "\u2705",
                    "label": "Indexed Access",
                })
            else:
                result.append({
                    "text": line,
                    "type": "info",
                    "icon": "\u2139\ufe0f",
                    "label": "Info",
                })
        return result

    original_classified = _classify(original_plan)
    optimized_classified = _classify(optimized_plan)

    # Determine improvement
    orig_scans = sum(
        1 for x in original_classified if x["type"] == "scan"
    )
    opt_scans = sum(
        1 for x in optimized_classified if x["type"] == "scan"
    )
    orig_searches = sum(
        1 for x in original_classified if x["type"] == "search"
    )
    opt_searches = sum(
        1 for x in optimized_classified if x["type"] == "search"
    )

    improvements = []
    if orig_scans > opt_scans:
        improvements.append("Reduced full table scans")
    if opt_searches > orig_searches:
        improvements.append("Increased indexed access")
    if orig_scans > 0 and opt_scans < orig_scans:
        improvements.append("Reduced data scanning")

    improved = len(improvements) > 0
    if not improved and orig_scans == opt_scans:
        improvements.append(
            "Query structure improved but execution plan is similar"
        )

    return {
        "original": original_classified,
        "optimized": optimized_classified,
        "improved": improved,
        "improvements": improvements,
        "skipped": False,
    }


# ─────────────────────────────────────────────────────────────
# 8. File Upload Validation
# ─────────────────────────────────────────────────────────────
def validate_sqlite_file(filepath: str) -> bool:
    """Verify that a file is a valid SQLite database."""
    try:
        conn = sqlite3.connect(filepath)
        cursor = conn.cursor()
        # Try reading the sqlite_master table (only works for valid SQLite files)
        cursor.execute("SELECT count(*) FROM sqlite_master")
        conn.close()
        return True
    except Exception:
        return False


# ═════════════════════════════════════════════════════════════
# Flask Routes
# ═════════════════════════════════════════════════════════════

@app.route("/")
def index():
    """Render the main page. Clears all uploaded databases on every refresh."""
    # Purge uploaded databases so nothing persists across refreshes
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    # Reset active database back to the default
    current_db["path"] = DEFAULT_DB
    current_db["name"] = "Default Sample Database"

    return render_template("index.html")


@app.route("/schema", methods=["GET"])
def schema():
    """Return the schema of the currently selected database."""
    return jsonify({
        "schema": get_schema(),
        "db_name": current_db["name"],
    })


def check_existing_indexes(table_name: str) -> list:
    """Return a list of indexed columns for the given table using PRAGMA index_list."""
    conn = get_db()
    cursor = conn.cursor()
    indexed_columns = []
    try:
        cursor.execute(f"PRAGMA index_list('{table_name}')")
        indexes = cursor.fetchall()
        for idx in indexes:
            idx_name = idx["name"]
            cursor.execute(f"PRAGMA index_info('{idx_name}')")
            cols = cursor.fetchall()
            for col in cols:
                indexed_columns.append(col["name"].lower())
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return indexed_columns


def generate_summary(analysis: dict, plan: dict, complexity: dict) -> str:
    """Generate final summary block mandatory message."""
    scan_present = any(
        "Full table scan detected" in x.get("message", "")
        for x in plan.get("interpretation", [])
    )
    
    if len(analysis.get("issues", [])) > 0 or complexity.get("level") == "HIGH":
        return "The query contains inefficiencies that may impact performance and should be optimized."
    elif scan_present:
        return "The query is well-structured but can be optimized further by reducing full table scans using indexing."
    else:
        return "This query is structurally correct with minor optimization opportunities."


@app.route("/analyze", methods=["POST"])
def analyze():
    """Analyze the submitted SQL query against the current database."""
    data = request.get_json()
    if not data or "query" not in data:
        return jsonify({"error": "No query provided."}), 400

    query = data["query"].strip()

    # Step 1: Validate
    validation = validate_query(query)
    if not validation["valid"]:
        return jsonify({"valid": False, "error": validation["error"]})

    # Step 2: Static analysis
    analysis = analyze_query(query)

    # Step 3: Execution plan
    plan = get_execution_plan(query)

    # Step 4: Complexity Score (uses EXPLAIN raw output for SCAN detection)
    complexity = compute_complexity_score(query, plan.get("raw", []))

    # Step 5: Smart Index Recommendation
    index_suggestion = generate_index_suggestion(query, plan.get("raw", []))

    # Step 6: Optimized query (safe, alias-aware)
    optimized = generate_optimized_query(query)

    # Step 7: Performance Comparison (original vs optimized)
    # Pass full optimized dict so comparison can handle skipped cases
    comparison = compare_performance(query, optimized)

    # Step 8: Final Summary Block
    summary = generate_summary(analysis, plan, complexity)

    return jsonify({
        "valid": True,
        "analysis": analysis,
        "plan": plan,
        "complexity": complexity,
        "index_suggestion": index_suggestion,
        "optimized": optimized,
        "comparison": comparison,
        "summary": summary,
    })


@app.route("/upload", methods=["POST"])
def upload_database():
    """
    Handle SQLite database file upload.
    Validates file type and SQLite format.
    """
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded."}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"success": False, "error": "No file selected."}), 400

    # Validate extension
    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({
            "success": False,
            "error": f"Invalid file type '{ext}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        }), 400

    # Save to uploads directory
    filepath = os.path.join(UPLOAD_DIR, filename)
    file.save(filepath)

    # Validate it's actually a SQLite file
    if not validate_sqlite_file(filepath):
        os.remove(filepath)
        return jsonify({
            "success": False,
            "error": "File is not a valid SQLite database.",
        }), 400

    # Switch to uploaded database
    current_db["path"] = filepath
    current_db["name"] = filename

    return jsonify({
        "success": True,
        "db_name": filename,
        "schema": get_schema(filepath),
    })


@app.route("/switch-db", methods=["POST"])
def switch_database():
    """Switch between default and uploaded database."""
    data = request.get_json()
    db_choice = data.get("database", "default")

    if db_choice == "default":
        current_db["path"] = DEFAULT_DB
        current_db["name"] = "Default Sample Database"
    else:
        # Check if the uploaded DB file still exists
        filepath = os.path.join(UPLOAD_DIR, db_choice)
        if os.path.exists(filepath) and validate_sqlite_file(filepath):
            current_db["path"] = filepath
            current_db["name"] = db_choice
        else:
            return jsonify({
                "success": False,
                "error": f"Database file '{db_choice}' not found or invalid.",
            }), 404

    return jsonify({
        "success": True,
        "db_name": current_db["name"],
        "schema": get_schema(),
    })


@app.route("/uploaded-dbs", methods=["GET"])
def list_uploaded_dbs():
    """List all uploaded database files."""
    files = []
    if os.path.exists(UPLOAD_DIR):
        for f in os.listdir(UPLOAD_DIR):
            ext = os.path.splitext(f)[1].lower()
            if ext in ALLOWED_EXTENSIONS:
                files.append(f)
    return jsonify({"databases": files, "current": current_db["name"]})


@app.route("/reset-db", methods=["POST"])
def reset_database():
    """Reset to default database."""
    current_db["path"] = DEFAULT_DB
    current_db["name"] = "Default Sample Database"
    return jsonify({
        "success": True,
        "db_name": current_db["name"],
        "schema": get_schema(),
    })


@app.route("/sample-queries", methods=["GET"])
def sample_queries():
    """Return sample queries for the user to try."""
    return jsonify([
        {"label": "Select All Users (has issues)", "query": "SELECT * FROM users"},
        {"label": "Cartesian Product (dangerous)", "query": "SELECT * FROM users, orders"},
        {"label": "Proper JOIN with Filter",
         "query": "SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100"},
        {"label": "Nested Subquery",
         "query": "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 200)"},
        {"label": "Aggregation with GROUP BY",
         "query": "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_spent FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total_spent DESC"},
        {"label": "Well-Optimized Query",
         "query": "SELECT name, email FROM users WHERE id = 3 LIMIT 1"},
    ])


# ─────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_database()
    print("[>>] SQL Query Analyzer v2.0 running at http://127.0.0.1:5000")
    app.run(debug=True, port=5000)
