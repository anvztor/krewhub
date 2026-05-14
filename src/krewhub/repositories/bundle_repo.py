from __future__ import annotations

from datetime import datetime

import aiosqlite

from krewhub.models import Bundle, BundleStatus


class StaleResourceError(Exception):
    """Raised when an update targets a stale resource_version."""

    def __init__(self, resource_type: str, resource_id: str) -> None:
        self.resource_type = resource_type
        self.resource_id = resource_id
        super().__init__(
            f"Conflict: {resource_type} {resource_id} has been modified"
        )


class BundleRepo:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def create(self, bundle: Bundle) -> Bundle:
        import json

        repo_spec_json = json.dumps(bundle.repo_spec) if bundle.repo_spec else None
        await self._db.execute(
            """INSERT INTO bundles
               (id, cookbook_id, repo_spec, prompt, status,
                created_by, created_at,
                claimed_at, cooked_at, digested_at, blocked_reason,
                graph_code, graph_mermaid,
                resource_version, generation,
                owner_account_id, default_agent_runtime_id, sandbox_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (bundle.id, bundle.cookbook_id, repo_spec_json,
             bundle.prompt, bundle.status,
             bundle.created_by, bundle.created_at.isoformat(),
             bundle.claimed_at.isoformat() if bundle.claimed_at else None,
             bundle.cooked_at.isoformat() if bundle.cooked_at else None,
             bundle.digested_at.isoformat() if bundle.digested_at else None,
             bundle.blocked_reason,
             bundle.graph_code, bundle.graph_mermaid,
             bundle.resource_version, bundle.generation,
             bundle.owner_account_id, bundle.default_agent_runtime_id,
             bundle.sandbox_id),
        )
        await self._db.commit()
        return bundle

    async def set_sandbox(self, bundle_id: str, sandbox_id: str) -> None:
        """Bind a freshly-provisioned sandbox to the bundle."""
        await self._db.execute(
            """UPDATE bundles
               SET sandbox_id = ?,
                   resource_version = resource_version + 1
               WHERE id = ?""",
            (sandbox_id, bundle_id),
        )
        await self._db.commit()

    async def set_default_agent_runtime(
        self, bundle_id: str, runtime_id: str,
    ) -> Bundle | None:
        await self._db.execute(
            """UPDATE bundles
               SET default_agent_runtime_id = ?,
                   resource_version = resource_version + 1
               WHERE id = ?""",
            (runtime_id, bundle_id),
        )
        await self._db.commit()
        return await self.get(bundle_id)

    async def get(self, bundle_id: str) -> Bundle | None:
        cursor = await self._db.execute(
            "SELECT * FROM bundles WHERE id = ?", (bundle_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_bundle(row)

    async def list_by_cookbook(self, cookbook_id: str) -> list[Bundle]:
        """List bundles for a cookbook, with MAX(tasks.updated_at) joined.

        latest_task_activity_at falls back to bundle.created_at when the
        bundle has no tasks (LEFT JOIN + COALESCE). cookrew-beta uses
        this column to derive its active/idle bucket without needing a
        per-bundle round trip."""
        cursor = await self._db.execute(
            """SELECT b.*,
                      COALESCE(
                          (SELECT MAX(t.updated_at) FROM tasks t WHERE t.bundle_id = b.id),
                          b.created_at
                      ) AS latest_task_activity_at
                 FROM bundles b
                WHERE b.cookbook_id = ?
                ORDER BY b.created_at DESC""",
            (cookbook_id,),
        )
        rows = await cursor.fetchall()
        return [_row_to_bundle(r) for r in rows]

    async def update_status(
        self,
        bundle_id: str,
        status: BundleStatus,
        *,
        claimed_at: datetime | None = None,
        cooked_at: datetime | None = None,
        digested_at: datetime | None = None,
        blocked_reason: str | None = None,
        expected_version: int | None = None,
    ) -> Bundle | None:
        parts: list[str] = ["status = ?", "resource_version = resource_version + 1"]
        params: list[object] = [status]

        if claimed_at is not None:
            parts.append("claimed_at = ?")
            params.append(claimed_at.isoformat())
        if cooked_at is not None:
            parts.append("cooked_at = ?")
            params.append(cooked_at.isoformat())
        if digested_at is not None:
            parts.append("digested_at = ?")
            params.append(digested_at.isoformat())
        if blocked_reason is not None:
            parts.append("blocked_reason = ?")
            params.append(blocked_reason)

        where = "id = ?"
        params.append(bundle_id)

        if expected_version is not None:
            where += " AND resource_version = ?"
            params.append(expected_version)

        cursor = await self._db.execute(
            f"UPDATE bundles SET {', '.join(parts)} WHERE {where}",
            params,
        )
        await self._db.commit()

        if expected_version is not None and cursor.rowcount == 0:
            existing = await self.get(bundle_id)
            if existing is not None:
                raise StaleResourceError("bundle", bundle_id)
            return None

        return await self.get(bundle_id)

    async def reopen_for_rerun(self, bundle_id: str) -> Bundle | None:
        await self._db.execute(
            """UPDATE bundles
               SET status = 'open',
                   blocked_reason = NULL,
                   resource_version = resource_version + 1
               WHERE id = ?""",
            (bundle_id,),
        )
        await self._db.commit()
        return await self.get(bundle_id)

    async def attach_graph(
        self,
        bundle_id: str,
        *,
        graph_code: str,
        graph_mermaid: str,
    ) -> Bundle | None:
        """Persist validated graph code + rendered mermaid on a bundle.

        Called by the bundle service after the orchestrator A2A response
        passes the sandbox. The GraphRunnerController will then pick up the
        bundle by virtue of its non-NULL graph_code.

        Bumps resource_version so cookrew SSE picks up the change.
        """
        await self._db.execute(
            """UPDATE bundles
               SET graph_code = ?, graph_mermaid = ?,
                   resource_version = resource_version + 1
               WHERE id = ?""",
            (graph_code, graph_mermaid, bundle_id),
        )
        await self._db.commit()
        return await self.get(bundle_id)

    async def list_runnable(self) -> list[Bundle]:
        """Return bundles whose graph code is set and status is 'open'.

        These are the candidates the GraphRunnerController will execute.
        Ordered by created_at so older bundles run first (FIFO fairness).
        Excludes already-digested bundles to prevent re-run after crash.
        """
        cursor = await self._db.execute(
            """SELECT * FROM bundles
               WHERE graph_code IS NOT NULL
                 AND status = 'open'
                 AND digested_at IS NULL
               ORDER BY created_at"""
        )
        rows = await cursor.fetchall()
        return [_row_to_bundle(r) for r in rows]


def _row_to_bundle(row: aiosqlite.Row) -> Bundle:
    import json

    keys = set(row.keys())
    repo_spec_raw = row["repo_spec"] if "repo_spec" in keys else None
    repo_spec = json.loads(repo_spec_raw) if repo_spec_raw else None

    if "latest_task_activity_at" in keys and row["latest_task_activity_at"]:
        latest_activity = datetime.fromisoformat(row["latest_task_activity_at"])
    else:
        latest_activity = datetime.fromisoformat(row["created_at"])

    return Bundle(
        id=row["id"],
        cookbook_id=row["cookbook_id"] if "cookbook_id" in keys else None,
        repo_spec=repo_spec,
        prompt=row["prompt"],
        status=row["status"],
        created_by=row["created_by"],
        created_at=datetime.fromisoformat(row["created_at"]),
        claimed_at=datetime.fromisoformat(row["claimed_at"]) if row["claimed_at"] else None,
        cooked_at=datetime.fromisoformat(row["cooked_at"]) if row["cooked_at"] else None,
        digested_at=datetime.fromisoformat(row["digested_at"]) if row["digested_at"] else None,
        blocked_reason=row["blocked_reason"],
        graph_code=row["graph_code"] if "graph_code" in keys else None,
        graph_mermaid=row["graph_mermaid"] if "graph_mermaid" in keys else None,
        resource_version=row["resource_version"],
        generation=row["generation"],
        owner_account_id=row["owner_account_id"] if "owner_account_id" in keys else None,
        default_agent_runtime_id=(
            row["default_agent_runtime_id"]
            if "default_agent_runtime_id" in keys
            else None
        ),
        sandbox_id=row["sandbox_id"] if "sandbox_id" in keys else None,
        latest_task_activity_at=latest_activity,
    )
