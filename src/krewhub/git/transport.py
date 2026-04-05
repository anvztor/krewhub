"""Git smart HTTP transport — run git binaries for push/pull."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from krewhub.config import get_settings

logger = logging.getLogger(__name__)

VALID_SERVICES = {"git-receive-pack", "git-upload-pack"}


def repos_root() -> Path:
    """Root directory for bare repos: /data/repos/ in Docker."""
    settings = get_settings()
    db_path = Path(settings.database_path)
    return db_path.parent / "repos"


def resolve_repo_path(owner: str, repo: str) -> Path:
    """Map owner/repo to /data/repos/{owner}/{repo}.git"""
    name = repo if repo.endswith(".git") else f"{repo}.git"
    return repos_root() / owner / name


async def ensure_bare_repo(repo_path: Path) -> None:
    """Create a bare repo if it doesn't exist."""
    if repo_path.exists():
        return

    repo_path.mkdir(parents=True, exist_ok=True)
    proc = await asyncio.create_subprocess_exec(
        "git", "init", "--bare", str(repo_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"git init --bare failed: {stderr.decode()}")

    # Set default branch to main
    proc = await asyncio.create_subprocess_exec(
        "git", "-C", str(repo_path), "symbolic-ref", "HEAD", "refs/heads/main",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()

    logger.info("Created bare repo at %s", repo_path)


async def run_git_service(
    service: str,
    repo_path: Path,
    input_data: bytes | None = None,
    advertise: bool = False,
) -> bytes:
    """Run a git service binary and return output.

    Args:
        service: "git-receive-pack" or "git-upload-pack"
        repo_path: Path to bare repo
        input_data: Request body to pipe to stdin
        advertise: If True, run --advertise-refs (ref discovery)
    """
    if service not in VALID_SERVICES:
        raise ValueError(f"Invalid git service: {service}")

    cmd = [service, "--stateless-rpc"]
    if advertise:
        cmd.append("--advertise-refs")
    cmd.append(str(repo_path))

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE if input_data else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate(input=input_data)

    if proc.returncode != 0:
        logger.warning("git %s failed (rc=%d): %s", service, proc.returncode, stderr.decode().strip())

    return stdout
