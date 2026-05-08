"""Delete test bundles and their dependent rows from a krewhub SQLite DB.

This is intentionally a local maintenance script, not a public API route.

Usage:
    # Dry-run default
    KREWHUB_DATABASE_PATH=~/.krewhub/krewhub.db \
      uv run python dev/cleanup_test_bundles.py --recipe-id rec_xxx

    # Apply
    KREWHUB_DATABASE_PATH=~/.krewhub/krewhub.db \
      uv run python dev/cleanup_test_bundles.py --recipe-id rec_xxx --apply

    # Add explicit prompts or bundle ids
    uv run python dev/cleanup_test_bundles.py \
      --prompt-like "real claude lifecycle demo" \
      --bundle-id bun_123 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_PROMPT_PATTERNS = (
    "test",
    "smoke",
    "verify",
    "claim-flow",
    "codex-claim",
    "real-agent",
    "browser-driven",
    "agent-tab",
    "sandbox check",
    "sse",
    "real e2e flow",
    "e2e real flow",
    "e2e bootstrap",
    "real claude lifecycle demo",
)


def _db_path() -> Path:
    return Path(
        os.environ.get(
            "KREWHUB_DATABASE_PATH",
            str(Path.home() / ".krewhub" / "krewhub.db"),
        )
    ).expanduser()


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _placeholders(values: list[str]) -> str:
    return ",".join("?" for _ in values)


def _select_bundle_ids(
    db: sqlite3.Connection,
    *,
    recipe_id: str | None,
    owner_account_id: str | None,
    prompt_patterns: tuple[str, ...],
    bundle_ids: tuple[str, ...],
) -> list[str]:
    clauses: list[str] = []
    params: list[str] = []

    if bundle_ids:
        clauses.append(f"id IN ({_placeholders(list(bundle_ids))})")
        params.extend(bundle_ids)

    prompt_clauses = []
    for pattern in prompt_patterns:
        prompt_clauses.append("LOWER(prompt) LIKE ?")
        params.append(f"%{pattern.lower()}%")
    if prompt_clauses:
        clauses.append("(" + " OR ".join(prompt_clauses) + ")")

    if not clauses:
        raise SystemExit("No cleanup selector provided.")

    where = "(" + " OR ".join(clauses) + ")"
    if recipe_id:
        where += " AND recipe_id = ?"
        params.append(recipe_id)
    if owner_account_id:
        where += " AND owner_account_id = ?"
        params.append(owner_account_id)

    rows = db.execute(
        f"SELECT id FROM bundles WHERE {where} ORDER BY created_at",
        params,
    ).fetchall()
    return [row["id"] for row in rows]


def _count(db: sqlite3.Connection, sql: str, params: list[str]) -> int:
    row = db.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def _delete_optional(
    db: sqlite3.Connection,
    table: str,
    sql: str,
    params: list[str],
) -> int:
    if not _table_exists(db, table):
        return 0
    return db.execute(sql, params).rowcount


def cleanup(
    db: sqlite3.Connection,
    *,
    bundle_ids: list[str],
    apply: bool,
) -> dict[str, int]:
    if not bundle_ids:
        return {
            "bundles": 0,
            "tasks": 0,
            "events": 0,
            "digests": 0,
            "sandboxes": 0,
            "task_usage": 0,
            "watch_log": 0,
        }

    bundle_ph = _placeholders(bundle_ids)
    task_rows = db.execute(
        f"SELECT id FROM tasks WHERE bundle_id IN ({bundle_ph})",
        bundle_ids,
    ).fetchall()
    task_ids = [row["id"] for row in task_rows]
    task_ph = _placeholders(task_ids) if task_ids else "NULL"

    counts = {
        "bundles": len(bundle_ids),
        "tasks": len(task_ids),
        "events": _count(
            db,
            f"""SELECT COUNT(*) FROM events
                WHERE bundle_id IN ({bundle_ph})
                   OR task_id IN ({task_ph})""",
            bundle_ids + task_ids,
        ),
        "digests": _count(
            db,
            f"SELECT COUNT(*) FROM digests WHERE bundle_id IN ({bundle_ph})",
            bundle_ids,
        ) if _table_exists(db, "digests") else 0,
        "sandboxes": _count(
            db,
            f"SELECT COUNT(*) FROM sandboxes WHERE task_id IN ({task_ph})",
            task_ids,
        ) if task_ids and _table_exists(db, "sandboxes") else 0,
        "task_usage": _count(
            db,
            f"SELECT COUNT(*) FROM task_usage WHERE task_id IN ({task_ph})",
            task_ids,
        ) if task_ids and _table_exists(db, "task_usage") else 0,
        "watch_log": _count(
            db,
            f"""SELECT COUNT(*) FROM watch_log
                WHERE resource_id IN ({_placeholders(bundle_ids + task_ids)})""",
            bundle_ids + task_ids,
        ) if _table_exists(db, "watch_log") else 0,
    }

    if not apply:
        return counts

    with db:
        if task_ids:
            _delete_optional(
                db,
                "sandboxes",
                f"DELETE FROM sandboxes WHERE task_id IN ({task_ph})",
                task_ids,
            )
            _delete_optional(
                db,
                "task_usage",
                f"DELETE FROM task_usage WHERE task_id IN ({task_ph})",
                task_ids,
            )
        _delete_optional(
            db,
            "events",
            f"""DELETE FROM events
                WHERE bundle_id IN ({bundle_ph})
                   OR task_id IN ({task_ph})""",
            bundle_ids + task_ids,
        )
        _delete_optional(
            db,
            "digests",
            f"DELETE FROM digests WHERE bundle_id IN ({bundle_ph})",
            bundle_ids,
        )
        if _table_exists(db, "watch_log"):
            ids = bundle_ids + task_ids
            db.execute(
                f"DELETE FROM watch_log WHERE resource_id IN ({_placeholders(ids)})",
                ids,
            )
        db.execute(f"DELETE FROM tasks WHERE bundle_id IN ({bundle_ph})", bundle_ids)
        db.execute(f"DELETE FROM bundles WHERE id IN ({bundle_ph})", bundle_ids)

    return counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(_db_path()))
    parser.add_argument("--recipe-id")
    parser.add_argument("--owner-account-id")
    parser.add_argument("--prompt-like", action="append", default=[])
    parser.add_argument("--bundle-id", action="append", default=[])
    parser.add_argument("--no-default-patterns", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    path = Path(args.db).expanduser()
    if not path.exists():
        raise SystemExit(f"DB not found: {path}")

    patterns = tuple(args.prompt_like)
    if not args.no_default_patterns:
        patterns = DEFAULT_PROMPT_PATTERNS + patterns

    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    bundle_ids = _select_bundle_ids(
        db,
        recipe_id=args.recipe_id,
        owner_account_id=args.owner_account_id,
        prompt_patterns=patterns,
        bundle_ids=tuple(args.bundle_id),
    )

    if args.apply and bundle_ids:
        db.execute("PRAGMA wal_checkpoint(FULL)")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = path.with_suffix(path.suffix + f".bak-{stamp}")
        shutil.copy2(path, backup)
        print(f"backup={backup}")

    counts = cleanup(db, bundle_ids=bundle_ids, apply=args.apply)
    mode = "APPLIED" if args.apply else "DRY_RUN"
    print(f"{mode} bundle_ids={','.join(bundle_ids) if bundle_ids else '-'}")
    for key, value in counts.items():
        print(f"{key}={value}")
    db.close()


if __name__ == "__main__":
    main()
