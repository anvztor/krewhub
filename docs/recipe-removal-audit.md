# Recipe-removal audit — Step (c) inventory

Status: written 2026-05-12 as part of step (b.5) so step (c) is mechanical, not exploratory.

This catalogs every read site of `bundle.recipe_id`, `recipe.repo_url`, and `recipe.default_branch` and tags it with what Step (c) needs to do.

Counts are after the deprecation + Phase 12 commits. Total: **331 `recipe`-coupled lines** across **30 files**.

## Tag legend

- **SWAP** — `recipe_id` parameter/column should swap to `cookbook_id`. Mechanical rename.
- **SPEC** — reads `recipe.repo_url` / `recipe.default_branch`. Must move to `bundle.repo_spec`, with JIT fallback when spec is null.
- **DUAL** — write both `recipe_id` and `cookbook_id` for the dual-write window, then drop `recipe_id` after callers migrate.
- **DELETE** — code lives only because recipe exists; goes away with recipe.
- **TEST** — test fixture setup. Either dual-write or migrate to creating cookbooks directly.

## Hot files (>10 recipe lines)

| File | Lines | Primary action |
|---|---:|---|
| `services/aggregate_service.py` | 26 | SWAP — queries that join recipes can drop the join |
| `routes/recipes.py` | 22 | DELETE — entire file goes |
| `services/task_service.py` | 20 | SWAP — every task event carries `recipe_id`, becomes `cookbook_id` |
| `db/migrations.py` | 20 | DUAL — already correct; will gain a "drop recipe_id" step at end |
| `services/bundle_service.py` | 15 | SWAP — every event emit carries `recipe_id=bundle.recipe_id`. Replace with `cookbook_id=bundle.cookbook_id` |
| `services/digest_service.py` | 11 | DELETE — entire service goes with the digest layer |
| `routes/tasks.py` | 11 | SWAP — event-emit sites |
| `routes/bundles.py` | 11 | SWAP — including the load-bearing `POST /recipes/{id}/bundles` → `POST /cookbooks/{id}/bundles` |
| `db/schema.py` | 11 | DUAL — `recipe_id` columns stay until callers flip, then drop |
| `controllers/task_dispatch.py` | 11 | SWAP — event-emit + repo lookup paths |
| `controllers/graph_runner.py` | 11 | SPEC + SWAP — reads `recipe.repo_url` for dispatch; replace with `bundle.repo_spec` |

## SPEC sites (reads recipe.repo_url / recipe.default_branch)

These are the 4 places that consume the actual repo coordinates. Every one needs to switch to `bundle.repo_spec` with JIT-grant resolution.

| File | Line | Context | Step (c) target |
|---|---:|---|---|
| `controllers/graph_runner.py` | 242–243 | Builds A2A dispatch payload; passes `repo_url` + `branch` to graph node | Read `bundle.repo_spec`; if null, no repo dispatch — bundle is talk-only |
| `controllers/planner_dispatch.py` | 239–240 | Planner agent task payload | Same — read `bundle.repo_spec` |
| `services/digest_service.py` | 168–169 | Merge code_refs into the recipe's git repo on digest approval | DELETE — digest layer is going away entirely |
| `repositories/recipe_repo.py` | 18 | Insert into recipes table | DELETE — recipe persistence goes |

## SWAP sites (bundle.recipe_id, Event.recipe_id)

The event model carries `recipe_id` on every emit. ~75 sites total — all of them need the same rewrite:

```python
# Before
recipe_id=bundle.recipe_id,
# After
cookbook_id=bundle.cookbook_id,
```

Concentrations:
- `services/bundle_service.py`: lines 78, 107, 128, 140, 145, 271, 289, 294, 327, 333, 374, 423, 444, 456 (14 sites)
- `services/task_service.py`: lines 91, 105, 137, 141, 154, 162, 179, 211, 216, 228, 243 (11+ sites)
- `services/aggregate_service.py`: 26 lines, mostly SQL with `WHERE recipe_id = ?`
- `controllers/task_dispatch.py`: emits TASK_CLAIMED/WORKING events
- `routes/tasks.py`, `routes/bundles.py`: pull `recipe_id` from URL path; replace path with `cookbook_id`

## Watch / event scope (5 files)

`watch_log.recipe_id` is the SSE channel key — already has a parallel `cookbook_id` column from Phase 12.

| File | Action |
|---|---|
| `watch/store.py` | SWAP — `record_resource(recipe_id=…)` → `cookbook_id=…` |
| `watch/service.py` | SWAP — same |
| `services/sse_service.py` | SWAP — SSE channel filter key |
| `services/watch_channels.py` | SWAP — broadcast routing |
| `routes/stream.py` | SWAP — `?recipe_id=…` query param → `?cookbook_id=…` |

These can ride in the same commit as the bundle/task swap since they're all reading the same `recipe_id` on event rows.

## DELETE candidates (whole files / functions)

After Step (c) read-flip, these are pure deletes:

- `routes/recipes.py` — every endpoint
- `repositories/recipe_repo.py` — entire file
- `services/digest_service.py` + `digest_helpers.py` + `repositories/digest_repo.py` — digest layer, already deprecated
- `models.RecipeMember` — replaced by `CookbookShare`
- `db.schema` recipes/`recipe_members`/`digests` tables — last step after column drops
- `db.schema` `bundle_status` middle values (CLAIMED/COOKED/BLOCKED/CANCELLED/DIGESTED/REJECTED) — CHECK constraint becomes `IN ('open','closed')`
- `controllers/bundle_controller.py` — already deprecated; phase derivation goes
- `services/bundle_service.cancel_bundle`, `recompute_bundle_status`, `rerun_blocked_tasks` — replaced by simple OPEN↔CLOSED toggle

## TEST sites needing fixture changes

Tests that create recipes as fixture setup: every file that does `RecipeRepo(db).create(...)` or hits `POST /recipes`. Surveyed via the file counts above; the recipe-creating ones are:

- `tests/test_bundles.py`, `tests/test_tasks.py`, `tests/test_graph_runner.py`, `tests/test_graph_runtime.py`, `tests/test_graph_runner.py`, `tests/test_task_dispatch.py`, `tests/test_planner_dispatch.py`, `tests/test_event_visibility.py`, `tests/test_tape.py`, `tests/test_progress.py`, `tests/test_session_isolation.py` (and probably more)

Strategy: introduce a helper `seed_cookbook_with_bundle()` in `tests/conftest.py` that replaces every `seed_recipe_with_bundle()` pattern. Migrate test-by-test; recipe-creating tests keep working through the dual-write window because the underlying tables still exist.

## Risks worth re-flagging before Step (c)

1. **Aggregate queries** in `aggregate_service.py` join `recipes`. After the column flip we can drop the join entirely (bundle.cookbook_id is now direct). Verify performance — the indexes are in place but EXPLAIN should confirm.

2. **`watch_channels`** routes SSE by `recipe_id`. Subscribers expect this key. Step (c) needs to maintain backwards compat at the SSE level for a deploy window (emit both `recipe_id` and `cookbook_id` in the SSE envelope) OR cut subscribers over atomically.

3. **`graph_runner` `recipe.repo_url`** — this is the load-bearing read. If `bundle.repo_spec` is null after Step (c) lands, the runner currently won't have a repo to clone. Need to decide: (a) error explicitly with a "this bundle has no repo" event, or (b) succeed with no working tree (talk-only bundle, no file ops).

4. **`hooks.py`** and **`a2a_gateway.py`** carry `recipe_id` in payloads — agents may consume this. Keep emitting it during transition, ignore inbound.

## Recommended Step (c) order

1. Add `BundleRepo.list_by_cookbook(...)` and `EventRepo.list_by_cookbook(...)` (parallel to existing `_by_recipe`) — additive.
2. Flip `bundle_service` + `task_service` event-emit sites to pass `cookbook_id` (still emit `recipe_id` for back-compat). Tests stay green.
3. Add new routes: `POST /cookbooks/{id}/bundles`, `GET /cookbooks/{id}/bundles`. Old `POST /recipes/{id}/bundles` keeps working.
4. Flip frontend / daemon callers (separate repos) to the new routes.
5. Drop the recipe-scoped routes (already marked deprecated).
6. Drop `recipe_id` columns and the recipes/digests/recipe_members tables.

Step (b.5) leaves the foundation ready for steps 1–3 to land in a single PR.
