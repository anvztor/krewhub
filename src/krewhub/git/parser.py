"""Parse .gitmodules and git tree objects from bare repos."""

from __future__ import annotations

import asyncio
import configparser
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


async def _resolve_head(repo_path: Path) -> str:
    """Resolve the HEAD ref. Falls back to first branch if HEAD is unborn."""
    proc = await asyncio.create_subprocess_exec(
        "git", "-C", str(repo_path), "rev-parse", "HEAD",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode == 0:
        return "HEAD"

    # HEAD is unborn — find first branch
    proc = await asyncio.create_subprocess_exec(
        "git", "-C", str(repo_path), "branch", "--format=%(refname:short)",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    branches = stdout.decode().strip().splitlines()
    if branches:
        return branches[0]

    return "HEAD"


@dataclass(frozen=True)
class SubmoduleEntry:
    name: str
    path: str
    url: str
    branch: str = "main"


@dataclass(frozen=True)
class GitlinkEntry:
    path: str
    commit_sha: str


@dataclass(frozen=True)
class ResolvedRecipe:
    name: str
    path: str
    url: str
    branch: str
    commit_sha: str


async def parse_gitmodules(repo_path: Path) -> list[SubmoduleEntry]:
    """Read .gitmodules from HEAD of a bare repo."""
    ref = await _resolve_head(repo_path)
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", str(repo_path), "show", f"{ref}:.gitmodules",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.debug("No .gitmodules in %s: %s", repo_path, stderr.decode().strip())
            return []
    except FileNotFoundError:
        logger.warning("git binary not found")
        return []

    config = configparser.ConfigParser()
    config.read_string(stdout.decode())

    entries: list[SubmoduleEntry] = []
    for section in config.sections():
        if not section.startswith('submodule "'):
            continue
        name = section[len('submodule "'):-1]
        path = config.get(section, "path", fallback=name)
        url = config.get(section, "url", fallback="")
        branch = config.get(section, "branch", fallback="main")
        if url:
            entries.append(SubmoduleEntry(name=name, path=path, url=url, branch=branch))

    return entries


async def parse_tree_gitlinks(repo_path: Path) -> list[GitlinkEntry]:
    """Read gitlink entries (mode 160000) from HEAD tree."""
    ref = await _resolve_head(repo_path)
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", str(repo_path), "ls-tree", ref,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.debug("ls-tree failed for %s: %s", repo_path, stderr.decode().strip())
            return []
    except FileNotFoundError:
        return []

    entries: list[GitlinkEntry] = []
    for line in stdout.decode().splitlines():
        # Format: <mode> <type> <sha>\t<path>
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        mode, _type, sha, path = parts[0], parts[1], parts[2], parts[3]
        if mode == "160000":
            entries.append(GitlinkEntry(path=path, commit_sha=sha))

    return entries


def resolve_recipes(
    modules: list[SubmoduleEntry],
    gitlinks: list[GitlinkEntry],
) -> list[ResolvedRecipe]:
    """Join submodule entries with gitlink SHAs by path."""
    gitlink_map = {g.path: g.commit_sha for g in gitlinks}

    resolved: list[ResolvedRecipe] = []
    for mod in modules:
        commit_sha = gitlink_map.get(mod.path, "")
        resolved.append(ResolvedRecipe(
            name=mod.name,
            path=mod.path,
            url=mod.url,
            branch=mod.branch,
            commit_sha=commit_sha,
        ))

    return resolved
