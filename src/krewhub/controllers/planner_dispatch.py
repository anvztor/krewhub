"""PlannerDispatchController — kicks off graph planning for empty bundles.

Closes the loop between bundle creation and graph execution:

    cookrew POST /recipes/{r}/bundles {prompt, tasks: []}
        ↓
    krewhub creates bundle (status=open, graph_code=null, no tasks)
        ↓
    PlannerDispatchController (this controller, every 2s):
        - finds empty bundles
        - picks an online agent with capability "generate-graph"
        - POSTs A2A message/send to its endpoint with bundle_id metadata
        ↓
    krewcli PlannerOrchestratorExecutor receives the request, generates
    pydantic-graph code, and POSTs it back to /api/v1/bundles/{id}/graph
        ↓
    krewhub BundleService.attach_graph_artifact validates + renders +
    creates tasks; the bundle now has graph_code set and tasks populated
        ↓
    GraphRunnerController picks the bundle up on its next reconcile and
    runs the graph end-to-end via dispatch_cycle.

Trust model: this controller does NOT validate or even look at the code
that comes back. The validation gate is the sandbox in attach_graph; this
controller only owns the *trigger* side of the loop.

In-flight tracking: an in-memory `_dispatched: set[str]` prevents
double-dispatch across reconcile cycles within one process. The set is
purged when a bundle leaves the empty state (graph_code set or tasks
created), so successful dispatches free their slot naturally on the next
reconcile pass.

Failure handling: if no planner agent is online, the bundle is left
alone — that's transient cluster state, not a bundle problem. Same for
network errors POSTing to the planner. We retry on the next reconcile.
The bundle is NEVER marked BLOCKED here; only the sandbox or runner
mark bundles BLOCKED.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

import aiosqlite
import httpx

from krewhub.controllers.base import BaseController
from krewhub.repositories.agent_repo import AgentRepo
from krewhub.repositories.bundle_repo import BundleRepo
from krewhub.repositories.recipe_repo import RecipeRepo
from krewhub.services.graph_runtime.agent_picker import pick_agent_for_kind
from krewhub.watch.service import WatchService

if TYPE_CHECKING:
    from krewhub.models import AgentPresence, Bundle, Recipe

logger = logging.getLogger(__name__)


# Capability key planner agents must advertise to be picked.
PLANNER_CAPABILITY = "generate-graph"

# A2A `message/send` to a gateway planner is a *synchronous* call that
# blocks until the CLI subprocess finishes generating graph code. For
# Claude/codex-driven codegen that routinely takes 60-300s. Setting the
# timeout too tight causes the controller to think the dispatch failed,
# re-dispatch on the next reconcile, and end up with multiple in-flight
# codegens racing to attach to the same bundle — the second one loses
# with HTTP 409 "already has graph_code attached" and the planner loops.
# Give it a comfortable ceiling; the in-flight dedup below is the
# primary guard, this is just defense in depth.
_DEFAULT_HTTP_TIMEOUT = 600.0


class PlannerDispatchController(BaseController):
    """Triggers graph planning by dispatching empty bundles to a planner agent."""

    def __init__(
        self,
        db: aiosqlite.Connection,
        watch: WatchService,
        *,
        interval: float = 2.0,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(db, watch, interval=interval)
        # Allow tests to inject a fake httpx client; production owns its own.
        self._http = http or httpx.AsyncClient(
            timeout=_DEFAULT_HTTP_TIMEOUT, follow_redirects=True,
        )
        self._owns_http = http is None
        self._dispatched: set[str] = set()

    async def stop(self) -> None:
        await super().stop()
        if self._owns_http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Reconcile
    # ------------------------------------------------------------------

    async def reconcile(self) -> None:
        empty_bundles = await self._find_empty_bundles()
        if not empty_bundles:
            # Nothing to do — also opportunistically clear stale dispatched
            # ids that no longer correspond to empty bundles.
            self._dispatched.clear()
            return

        # Purge dispatched ids whose bundles are no longer empty.
        empty_ids = {b.id for b in empty_bundles}
        self._dispatched &= empty_ids

        recipe_repo = RecipeRepo(self._db)
        agent_repo = AgentRepo(self._db)

        for bundle in empty_bundles:
            if bundle.id in self._dispatched:
                continue

            recipe = await recipe_repo.get(bundle.recipe_id)
            if recipe is None or recipe.cookbook_id is None:
                logger.warning(
                    "PlannerDispatch: bundle %s has no resolvable cookbook (recipe=%s)",
                    bundle.id, bundle.recipe_id,
                )
                continue

            agents = await agent_repo.list_by_cookbook(recipe.cookbook_id)
            logger.info(
                "PlannerDispatch: bundle %s cookbook %s — %d agents: %s",
                bundle.id, recipe.cookbook_id, len(agents),
                [(a.agent_id, a.status, bool(a.endpoint_url), a.capabilities) for a in agents],
            )
            planner = pick_agent_for_kind(
                agents, PLANNER_CAPABILITY, exclude=set(),
            )
            # pick_agent_for_kind falls back to the first eligible agent
            # whether or not it has the capability — guard against that
            # since dispatching a non-planner is worse than waiting.
            if planner is None or PLANNER_CAPABILITY not in {
                cap.lower() for cap in planner.capabilities
            }:
                logger.info(
                    "PlannerDispatch: bundle %s has no online planner agent in cookbook %s (picked=%s caps=%s)",
                    bundle.id, recipe.cookbook_id,
                    planner.agent_id if planner else None,
                    planner.capabilities if planner else None,
                )
                continue

            # Reserve the slot BEFORE the POST. A2A `message/send` is a
            # long synchronous call — we don't want a second reconcile
            # cycle to re-dispatch the same bundle while the first POST
            # is still in flight. `_dispatch` frees the slot below if
            # the planner is unreachable (connect error); read timeouts
            # are treated as "still running server-side" and keep the
            # slot reserved until the bundle row flips out of the
            # empty-bundle query (purged by `_dispatched &= empty_ids`
            # on the next reconcile).
            self._dispatched.add(bundle.id)
            ok = await self._dispatch(bundle, recipe, planner)
            if ok:
                logger.info(
                    "PlannerDispatch: dispatched bundle %s to planner %s",
                    bundle.id, planner.agent_id,
                )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _find_empty_bundles(self) -> list["Bundle"]:
        """Return open bundles with no graph_code and no tasks AND
        autoplan_enabled=1.

        The autoplan filter is the new opt-in gate: a "+ NEW" tab from
        cookrew-beta creates an empty bundle with autoplan_enabled=0
        on purpose — the operator wants a blank board to drop tasks
        onto, not an LLM-generated graph. Orchestrator-mode flows
        flip the flag at create time (or via a dedicated dispatch
        endpoint) to opt back in.
        """
        cursor = await self._db.execute(
            """SELECT b.* FROM bundles b
               WHERE b.status = 'open'
                 AND b.graph_code IS NULL
                 AND b.autoplan_enabled = 1
                 AND NOT EXISTS (
                   SELECT 1 FROM tasks t WHERE t.bundle_id = b.id
                 )
               ORDER BY b.created_at"""
        )
        rows = await cursor.fetchall()
        if not rows:
            return []

        bundles_repo = BundleRepo(self._db)
        result = []
        for row in rows:
            b = await bundles_repo.get(row["id"])
            if b is not None:
                result.append(b)
        return result

    async def _dispatch(
        self,
        bundle: "Bundle",
        recipe: "Recipe",
        planner: "AgentPresence",
    ) -> bool:
        """POST an A2A message/send to the planner via the A2A hub gateway.

        Instead of hitting the agent's local endpoint_url (which is behind NAT),
        route through the hub gateway at /a2a/{owner}/{agent_short_name}.
        """
        owner = planner.owner_username or planner.agent_id.split("@")[-1]
        agent_short = planner.agent_id.split("@")[0]
        # Route through the local hub gateway (same krewhub process)
        hub_url = f"http://127.0.0.1:8420/a2a/{owner}/{agent_short}"

        payload = {
            "jsonrpc": "2.0",
            "id": f"plan:{bundle.id}",
            "method": "message/send",
            "params": {
                "message": {
                    "messageId": uuid.uuid4().hex,
                    "role": "user",
                    "parts": [{"kind": "text", "text": bundle.prompt}],
                    "metadata": {
                        "bundle_id": bundle.id,
                        "cookbook_id": recipe.cookbook_id or "",
                        "recipe_id": recipe.id,
                        "recipe_name": recipe.name,
                        "repo_url": recipe.repo_url,
                        "branch": recipe.default_branch,
                    },
                },
                "configuration": {"returnImmediately": True},
            },
        }

        try:
            resp = await self._http.post(hub_url, json=payload)
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            # Planner is unreachable — the POST never landed, so it's
            # safe (and correct) to free the reservation and retry on
            # the next reconcile.
            logger.info(
                "PlannerDispatch: planner %s unreachable for bundle %s: %s",
                planner.agent_id, bundle.id, exc,
            )
            self._dispatched.discard(bundle.id)
            return False
        except (httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as exc:
            # The POST landed but the server is still processing
            # (codegen can take minutes). Do NOT free the reservation:
            # the gateway is almost certainly going to finish and POST
            # attach_graph; a second dispatch here would race it and
            # lose with HTTP 409.
            logger.info(
                "PlannerDispatch: planner %s timed out reading bundle %s "
                "(codegen still running server-side): %s",
                planner.agent_id, bundle.id, exc,
            )
            return False
        except httpx.RequestError as exc:
            # Unknown transport failure — treat as unreachable and free
            # the slot. Worst case we retry; the server-side dedup
            # (attach_graph 409 on double-attach) still protects us.
            logger.info(
                "PlannerDispatch: planner %s request failed for bundle %s: %s",
                planner.agent_id, bundle.id, exc,
            )
            self._dispatched.discard(bundle.id)
            return False

        if resp.status_code != 200:
            logger.info(
                "PlannerDispatch: planner %s returned %d for bundle %s: %s",
                planner.agent_id, resp.status_code, bundle.id, resp.text[:300],
            )
            # A non-200 means the planner saw the request and rejected
            # it at the A2A layer — nothing is running server-side.
            # Free the slot so we can try another cycle (or another
            # planner) on the next reconcile.
            self._dispatched.discard(bundle.id)
            return False

        # We don't parse the body — the planner will POST back to
        # /api/v1/bundles/{id}/graph asynchronously when it's done.
        # All we need from this call is "accepted, work started".
        return True
