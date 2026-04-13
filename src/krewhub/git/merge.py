"""Git merge service — merge task branches into a recipe's default branch.

Called by DigestService.decide() when a digest is approved. Each CodeRef
carries a branch name; we merge each unique branch into the recipe's
default branch inside the krewhub bare repo.

Merge failures (conflicts, missing branches) are logged but do not block
the approval — the human already decided to accept the work. Conflicts
surface as MergeResult entries so the caller can record them.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from urllib.parse import urlparse

from krewhub.git.transport import resolve_repo_path
from krewhub.models import CodeRef

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MergeResult:
    branch: str
    success: bool
    message: str


def _extract_owner_repo(repo_url: str) -> tuple[str, str] | None:
    """Extract (owner, repo) from a repo URL or path-like string.

    Handles:
        - http(s)://host/owner/repo.git
        - owner/repo
        - /owner/repo.git
    """
    parsed = urlparse(repo_url)
    path = parsed.path.strip("/")
    if not path:
        return None
    parts = path.split("/")
    if len(parts) < 2:
        return None
    owner = parts[-2]
    repo = parts[-1].removesuffix(".git")
    return owner, repo


async def merge_code_refs(
    recipe_repo_url: str,
    default_branch: str,
    code_refs: list[CodeRef],
) -> list[MergeResult]:
    """Merge unique branches from code_refs into the default branch.

    Operates on the krewhub bare repo corresponding to recipe_repo_url.
    Each unique branch is merged with --no-ff to preserve history.

    Returns a MergeResult per unique branch attempted.
    """
    owner_repo = _extract_owner_repo(recipe_repo_url)
    if owner_repo is None:
        logger.warning("merge_code_refs: cannot parse repo_url %r", recipe_repo_url)
        return []

    owner, repo = owner_repo
    repo_path = resolve_repo_path(owner, repo)
    if not repo_path.exists():
        logger.warning(
            "merge_code_refs: bare repo not found at %s", repo_path,
        )
        return []

    # Collect unique branches (skip default branch itself)
    branches = list(dict.fromkeys(
        cr.branch
        for cr in code_refs
        if cr.branch and cr.branch != default_branch
    ))

    if not branches:
        logger.info("merge_code_refs: no task branches to merge")
        return []

    results: list[MergeResult] = []
    for branch in branches:
        result = await _merge_branch(
            repo_path=str(repo_path),
            default_branch=default_branch,
            source_branch=branch,
        )
        results.append(result)
        if result.success:
            logger.info(
                "merge_code_refs: merged %s into %s", branch, default_branch,
            )
        else:
            logger.warning(
                "merge_code_refs: failed to merge %s — %s",
                branch, result.message,
            )

    return results


async def _merge_branch(
    repo_path: str,
    default_branch: str,
    source_branch: str,
) -> MergeResult:
    """Merge a single branch into default_branch in a bare repo.

    Uses a temporary worktree to perform the merge, then cleans up.
    Bare repos cannot merge directly, so we:
    1. Create a temporary worktree checked out at default_branch
    2. Merge the source branch
    3. Remove the worktree
    """
    worktree_path = f"{repo_path}/_merge_{source_branch.replace('/', '_')}"

    try:
        # Check that the source branch exists
        check = await asyncio.create_subprocess_exec(
            "git", "-C", repo_path, "rev-parse", "--verify",
            f"refs/heads/{source_branch}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await check.communicate()
        if check.returncode != 0:
            return MergeResult(
                branch=source_branch,
                success=False,
                message=f"branch not found: {stderr.decode().strip()}",
            )

        # Create worktree at default branch
        wt_add = await asyncio.create_subprocess_exec(
            "git", "-C", repo_path, "worktree", "add",
            worktree_path, default_branch,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await wt_add.communicate()
        if wt_add.returncode != 0:
            return MergeResult(
                branch=source_branch,
                success=False,
                message=f"worktree creation failed: {stderr.decode().strip()}",
            )

        # Merge source branch
        merge = await asyncio.create_subprocess_exec(
            "git", "-C", worktree_path, "merge", "--no-ff",
            "-m", f"Merge task branch '{source_branch}' (digest approved)",
            source_branch,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await merge.communicate()
        if merge.returncode != 0:
            # Abort the failed merge to leave worktree clean
            abort = await asyncio.create_subprocess_exec(
                "git", "-C", worktree_path, "merge", "--abort",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await abort.communicate()
            return MergeResult(
                branch=source_branch,
                success=False,
                message=f"merge conflict: {stderr.decode().strip()}",
            )

        return MergeResult(
            branch=source_branch,
            success=True,
            message=stdout.decode().strip(),
        )

    finally:
        # Clean up worktree
        cleanup = await asyncio.create_subprocess_exec(
            "git", "-C", repo_path, "worktree", "remove", "--force",
            worktree_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await cleanup.communicate()
