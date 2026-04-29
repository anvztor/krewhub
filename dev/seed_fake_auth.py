"""Seed dev-user-1 with a recipe, bundle, and paired agent runtime.

Used in conjunction with KREWHUB_KREW_DEV_FAKE_AUTH=1. Idempotent — running
this twice is a no-op.

Usage:
    KREWHUB_DATABASE_PATH=/path/to/krewhub.db \
        uv run python dev/seed_fake_auth.py
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from krewhub.db.connection import close_db, init_db


async def main() -> None:
    db = await init_db()

    now = datetime.now(timezone.utc).isoformat()

    # Cookbook + recipe owned by dev-user-1
    await db.execute(
        "INSERT OR IGNORE INTO cookbooks (id, name, owner_id, created_at) "
        "VALUES (?,?,?,?)",
        ("cb_dev", "Dev Cookbook", "dev-user-1", now),
    )
    await db.execute(
        "INSERT OR IGNORE INTO recipes (id, name, repo_url, default_branch, "
        "created_by, created_at, cookbook_id) VALUES (?,?,?,?,?,?,?)",
        (
            "r_dev",
            "Dev Recipe",
            "https://example.invalid/dev",
            "main",
            "dev-user-1",
            now,
            "cb_dev",
        ),
    )

    # Agent runtime paired with dev-user-1
    await db.execute(
        "INSERT OR IGNORE INTO agent_runtimes (id, agent_id, account_id, "
        "host_info, status, last_seen_at, started_at) VALUES (?,?,?,?,?,?,?)",
        ("rt-dev-1", "agent_dev", "dev-user-1", "{}", "online", now, now),
    )

    # Bundle owned by dev-user-1, paired with rt-dev-1
    await db.execute(
        "INSERT OR IGNORE INTO bundles (id, recipe_id, prompt, status, "
        "created_by, created_at, owner_account_id, default_agent_runtime_id) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            "BUN_DEV1",
            "r_dev",
            "Dev bundle prompt",
            "open",
            "dev-user-1",
            now,
            "dev-user-1",
            "rt-dev-1",
        ),
    )

    await db.commit()
    print("Seeded dev-user-1: cookbook cb_dev, recipe r_dev, runtime rt-dev-1, "
          "bundle BUN_DEV1")
    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
