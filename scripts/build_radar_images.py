"""Build review images from a committed Git tree. Never starts/replaces services.

The dependency image must already exist by content ID. To reproduce elsewhere,
docker save/load that immutable image; no floating runtime tag is accepted.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
DEPENDENCY_ID = "sha256:8e95486558630441c661ed066367c261151b80850e2bc9562a9703899dca48f9"
DEPENDENCY_TAG = "quantia-radar-dependencies:8e95486558630441"


def command(*args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, text=True, **kwargs).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    sha = command("git", "rev-parse", args.revision + "^{commit}")
    actual = command("docker", "image", "inspect", DEPENDENCY_ID, "--format", "{{.Id}}")
    if actual != DEPENDENCY_ID:
        raise ValueError("Dependency image identity mismatch")
    command("docker", "tag", DEPENDENCY_ID, DEPENDENCY_TAG)
    with tempfile.TemporaryDirectory(prefix="quantia-radar-build-") as temporary:
        folder = Path(temporary)
        archive = folder / "source.tar"
        command("git", "archive", "--format=tar", "--output", str(archive), sha)
        context = folder / "source"
        context.mkdir()
        with tarfile.open(archive) as tar:
            tar.extractall(context, filter="data")
        runtime = f"quantia-radar-review:{sha[:12]}"
        frontend = f"quantia-radar-frontend-review:{sha[:12]}"
        subprocess.run(["docker", "build", "-f", str(context/"Dockerfile.radar"),
            "--build-arg", "CODE_VERSION="+sha, "-t", runtime, str(context)], check=True)
        subprocess.run(["docker", "build", "-t", frontend, str(context/"frontend")], check=True)
        result = dict(commit=sha, dependency_image=DEPENDENCY_ID, images={
            name: command("docker", "image", "inspect", name, "--format", "{{.Id}}")
            for name in (runtime, frontend)}, started_services=False)
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    with args.receipt.open("x", encoding="utf8") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
