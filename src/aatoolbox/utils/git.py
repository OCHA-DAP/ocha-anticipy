"""Utils for obtaining git repository information."""
import subprocess


def get_short_hash():
    """Get short hash of last git commit."""
    return (
        subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
        .strip()
        .decode()
    )


def get_branch_name():
    """Get name of current git branch."""
    return (
        subprocess.check_output(["git", "branch", "--show"]).strip().decode()
    )
