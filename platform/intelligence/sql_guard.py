"""Read-only SQL guard for C5.

``execute_analytical_query`` translates curated intents into fixed SQL, so the
SQL body is authored here, not by the LLM. But defense-in-depth still applies:
*every* statement that reaches DuckDB is validated to be a single read-only
SELECT (or WITH...SELECT) before execution, on top of the connection itself
being opened ``read_only=True``. Two independent layers, either of which is
sufficient to block a write.

The guard is deliberately strict and allowlist-shaped:

* exactly ONE statement (no ``;``-chained second statement),
* the leading keyword is ``SELECT`` or ``WITH`` (a CTE that must terminate in a
  SELECT),
* no DML/DDL/PRAGMA/ATTACH/COPY/CALL keyword appears as a statement verb,
* no DuckDB ``COPY ... TO`` / ``EXPORT`` exfiltration.

It raises :class:`UnsafeSQLError` on any violation. Callers surface that as a
clean tool error, never as a 500.
"""

from __future__ import annotations

import re


class UnsafeSQLError(ValueError):
    """Raised when a statement is not a single read-only SELECT."""


# Statement verbs that mutate state or escape the read sandbox. Matched as whole
# words, case-insensitive, anywhere a statement could begin. We are conservative:
# if any of these appear as a token, we reject — curated SELECTs never need them.
_FORBIDDEN_TOKENS = frozenset(
    {
        "insert",
        "update",
        "delete",
        "merge",
        "upsert",
        "create",
        "alter",
        "drop",
        "truncate",
        "replace",
        "attach",
        "detach",
        "copy",
        "export",
        "import",
        "install",
        "load",
        "pragma",
        "set",
        "reset",
        "call",
        "vacuum",
        "checkpoint",
        "grant",
        "revoke",
        "begin",
        "commit",
        "rollback",
    }
)

# Strip /* */ block comments and -- line comments before inspection so a verb
# hidden in a comment cannot smuggle past, and a ';' in a comment cannot be
# mistaken for a statement separator.
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\n]*")
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _strip_comments(sql: str) -> str:
    sql = _BLOCK_COMMENT.sub(" ", sql)
    sql = _LINE_COMMENT.sub(" ", sql)
    return sql


def _strip_string_literals(sql: str) -> str:
    """Blank out single-quoted literals so a keyword inside data is ignored.

    e.g. ``where status = 'cancelled'`` must not trip the token scan, and
    ``'no; semicolons'`` must not look like a statement separator.
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    in_str = False
    while i < n:
        ch = sql[i]
        if in_str:
            if ch == "'":
                # Doubled '' is an escaped quote inside the literal.
                if i + 1 < n and sql[i + 1] == "'":
                    i += 2
                    continue
                in_str = False
            out.append(" ")
            i += 1
            continue
        if ch == "'":
            in_str = True
            out.append(" ")
            i += 1
            continue
        out.append(ch)
        i += 1
    if in_str:
        raise UnsafeSQLError("unterminated string literal in SQL")
    return "".join(out)


def assert_read_only_select(sql: str) -> str:
    """Validate ``sql`` is a single read-only SELECT; return the EXECUTABLE SQL.

    The returned string is the ORIGINAL SQL with only a trailing ``;`` trimmed —
    string literals and column references are preserved verbatim so it runs as
    written. Validation is performed on a *separate*, comment-and-literal-stripped
    copy, so a keyword hidden in a string literal (``where status = 'cancelled'``)
    cannot trip the scan, while the real literal still reaches DuckDB intact.

    Raises:
        UnsafeSQLError: if the statement is empty, multi-statement, does not
            start with SELECT/WITH, or contains any forbidden statement verb.
    """
    if not sql or not sql.strip():
        raise UnsafeSQLError("empty SQL")

    # Inspection copy: comments removed, string contents blanked.
    cleaned = _strip_string_literals(_strip_comments(sql)).strip()
    if not cleaned:
        raise UnsafeSQLError("SQL is only comments/whitespace")

    # Reject anything after a statement-terminating semicolon. A single trailing
    # ';' is allowed (and stripped); an inner ';' means a second statement.
    inspect_body = cleaned.rstrip().rstrip(";").rstrip()
    if ";" in inspect_body:
        raise UnsafeSQLError("multiple statements are not allowed")

    lowered = inspect_body.lower()
    first = _WORD.match(lowered)
    if first is None or first.group(0) not in {"select", "with"}:
        raise UnsafeSQLError("only SELECT (or WITH ... SELECT) statements are allowed")

    tokens = set(_WORD.findall(lowered))
    bad = tokens & _FORBIDDEN_TOKENS
    if bad:
        raise UnsafeSQLError(f"forbidden keyword(s) in SQL: {', '.join(sorted(bad))}")

    # A WITH must ultimately SELECT — guarantee a SELECT token is present.
    if "select" not in tokens:
        raise UnsafeSQLError("read-only query must contain a SELECT")

    # Return the ORIGINAL (executable) body, only trailing-semicolon trimmed.
    return sql.strip().rstrip(";").rstrip()
