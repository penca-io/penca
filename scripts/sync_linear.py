#!/usr/bin/env python3
"""Sync labels and projects from linear/ config to Linear.

Creates Linear labels from linear/labels.toml and Linear projects from
linear/projects.toml. By default syncs both; use --labels or --projects
to sync only one. Optionally classifies issues with scope labels and
assigns them to projects using Claude for intelligent categorization.

Requires LINEAR_API_KEY environment variable (personal API key).

Usage:
    just sync-linear                          # sync labels + projects
    just sync-linear --labels                 # sync labels only
    just sync-linear --projects               # sync projects only
    just sync-linear --retag                  # classify + tag/assign open issues via Claude
    just sync-linear --archive-old            # archive old scope-based projects
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

LINEAR_API_URL = "https://api.linear.app/graphql"
TEAM_KEY = "CHA"
LINEAR_DIR = Path(__file__).resolve().parent.parent / "linear"
LABELS_PATH = LINEAR_DIR / "labels.toml"
PROJECTS_PATH = LINEAR_DIR / "projects.toml"

# Scope-based projects that were created in error and should be archived.
OLD_SCOPE_PROJECT_NAMES = {
    "lifecycle",
    "query",
    "write",
    "branch",
    "meta",
    "cold",
    "hot",
    "proto",
    "ci",
    "deps",
    "infra",
    "hosted",
}


# TOML parsing (lightweight, no toml dependency)


def parse_toml(path: Path) -> list[dict]:
    """Parse a simple TOML file with sections, string fields, and arrays."""
    entries: list[dict] = []
    current: dict | None = None
    in_array = False
    array_key = ""
    array_buf: list[str] = []

    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        header = re.match(r"^\[([a-z][a-z0-9-]*)\]$", stripped)
        if header:
            if current is not None and array_buf:
                current[array_key] = array_buf

            current = {"_key": header.group(1)}
            entries.append(current)
            in_array = False
            array_buf = []
            continue

        if current is None:
            continue

        if stripped == "]":
            in_array = False
            current[array_key] = array_buf
            array_buf = []
            continue

        if in_array:
            item = re.match(r'^"(.*)"[,]?$', stripped)
            if item:
                array_buf.append(item.group(1))

            continue

        array_start = re.match(r"^(\w+)\s*=\s*\[$", stripped)
        if array_start:
            in_array = True
            array_key = array_start.group(1)
            array_buf = []
            continue

        kv = re.match(r'^(\w+)\s*=\s*"(.*)"$', stripped)
        if kv:
            current[kv.group(1)] = kv.group(2)

    if current is not None and array_buf:
        current[array_key] = array_buf

    return entries


def write_toml(
    path: Path, header_lines: list[str], entries: list[dict], fields: list[str]
) -> None:
    """Write entries back to a TOML file with the given header and field order."""
    lines = list(header_lines) + [""]
    for entry in entries:
        lines.append(f"[{entry['_key']}]")
        for field in fields:
            if field not in entry:
                continue

            value = entry[field]
            if isinstance(value, list):
                lines.append(f"{field} = [")
                for item in value:
                    lines.append(f'    "{item}",')

                lines.append("]")
            else:
                lines.append(f'{field} = "{value}"')

        lines.append("")

    path.write_text("\n".join(lines) + "\n")


def load_scopes() -> list[dict]:
    """Parse linear/labels.toml."""
    scopes = parse_toml(LABELS_PATH)
    for scope in scopes:
        scope["name"] = scope.pop("_key")

    return scopes


def write_scopes(scopes: list[dict]) -> None:
    """Write scopes back to linear/labels.toml."""
    entries = [
        {"_key": s["name"], **{k: v for k, v in s.items() if k != "name"}}
        for s in scopes
    ]
    write_toml(
        LABELS_PATH,
        [
            "# Conventional commit scopes for Penca.",
            "# Each scope maps 1:1 to a Linear label.",
            "# This file is the single source of truth — the commit-msg hook,",
            "# sync-linear script, and documentation all read from here.",
        ],
        entries,
        ["description", "linear_label_id", "keywords"],
    )


def load_projects() -> list[dict]:
    """Parse linear/projects.toml."""
    return parse_toml(PROJECTS_PATH)


def write_projects(projects: list[dict]) -> None:
    """Write projects back to linear/projects.toml."""
    write_toml(
        PROJECTS_PATH,
        [
            "# Linear projects for Penca.",
            "# Each project is a time-bound initiative with its own backlog.",
            "# The sync-linear script creates missing projects and assigns issues",
            "# to them using the keywords defined here.",
        ],
        projects,
        ["name", "description", "linear_project_id", "keywords"],
    )


def graphql(api_key: str, query: str, variables: dict | None = None) -> dict:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    request = urllib.request.Request(
        LINEAR_API_URL,
        data=payload,
        headers={
            "Authorization": api_key,
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request) as response:
        data = json.loads(response.read())

    if "errors" in data:
        print(f"Linear API error: {data['errors']}", file=sys.stderr)
        sys.exit(1)

    return data["data"]


def get_team_id(api_key: str) -> str:
    data = graphql(
        api_key,
        """
        query($key: String!) {
            teams(filter: { key: { eq: $key } }) {
                nodes { id }
            }
        }
    """,
        {"key": TEAM_KEY},
    )
    return data["teams"]["nodes"][0]["id"]


def list_labels(api_key: str, team_id: str) -> list[dict]:
    data = graphql(
        api_key,
        """
        query($teamId: ID) {
            issueLabels(
                filter: { team: { id: { eq: $teamId } } }
                first: 250
            ) {
                nodes { id name }
            }
        }
    """,
        {"teamId": team_id},
    )
    return data["issueLabels"]["nodes"]


def create_label(api_key: str, team_id: str, name: str, description: str) -> str:
    data = graphql(
        api_key,
        """
        mutation($input: IssueLabelCreateInput!) {
            issueLabelCreate(input: $input) {
                issueLabel { id }
            }
        }
    """,
        {"input": {"name": name, "description": description, "teamId": team_id}},
    )
    return data["issueLabelCreate"]["issueLabel"]["id"]


def list_linear_projects(api_key: str) -> list[dict]:
    data = graphql(
        api_key,
        """
        query {
            projects(first: 100) {
                nodes { id name }
            }
        }
    """,
    )
    return data["projects"]["nodes"]


def create_linear_project(
    api_key: str, team_id: str, name: str, description: str
) -> str:
    data = graphql(
        api_key,
        """
        mutation($input: ProjectCreateInput!) {
            projectCreate(input: $input) {
                project { id }
            }
        }
    """,
        {"input": {"name": name, "description": description, "teamIds": [team_id]}},
    )
    return data["projectCreate"]["project"]["id"]


def archive_project(api_key: str, project_id: str) -> None:
    graphql(
        api_key,
        """
        mutation($id: String!) {
            projectArchive(id: $id) {
                success
            }
        }
    """,
        {"id": project_id},
    )


def list_open_issues(api_key: str) -> list[dict]:
    data = graphql(
        api_key,
        """
        query($teamKey: String!) {
            issues(
                filter: {
                    team: { key: { eq: $teamKey } }
                    completedAt: { null: true }
                    canceledAt: { null: true }
                }
                first: 250
            ) {
                nodes {
                    id
                    identifier
                    title
                    description
                    url
                    labels { nodes { id name } }
                    project { id name }
                }
            }
        }
    """,
        {"teamKey": TEAM_KEY},
    )
    return data["issues"]["nodes"]


def update_issue_labels(api_key: str, issue_id: str, label_ids: list[str]) -> None:
    graphql(
        api_key,
        """
        mutation($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                issue { id }
            }
        }
    """,
        {"id": issue_id, "input": {"labelIds": label_ids}},
    )


def update_issue_project(api_key: str, issue_id: str, project_id: str | None) -> None:
    graphql(
        api_key,
        """
        mutation($id: String!, $input: IssueUpdateInput!) {
            issueUpdate(id: $id, input: $input) {
                issue { id }
            }
        }
    """,
        {"id": issue_id, "input": {"projectId": project_id}},
    )


def classify_issues(
    issues: list[dict],
    scopes: list[dict],
    projects: list[dict],
) -> dict[str, dict]:
    """Classify issues into labels and projects using Claude.

    Returns a dict mapping issue identifier to
    {"labels": ["scope", ...], "project": "name" | None}.
    """
    claude_path = shutil.which("claude")
    if not claude_path:
        print(
            "Error: claude CLI not found on PATH.\n"
            "Install it from: https://claude.ai/download",
            file=sys.stderr,
        )
        sys.exit(1)

    scope_descriptions = "\n".join(f"- {s['name']}: {s['description']}" for s in scopes)
    project_descriptions = "\n".join(
        f"- {p['name']}: {p['description']}" for p in projects
    )

    issue_lines = []
    for issue in issues:
        desc = (issue.get("description") or "")[:300]
        issue_lines.append(
            f"- {issue['identifier']}: {issue['title']}"
            + (f" — {desc}" if desc else "")
        )

    issues_text = "\n".join(issue_lines)
    identifiers = [issue["identifier"] for issue in issues]

    prompt = f"""\
Classify each Linear issue into the best-matching scope labels and project.

## Scope labels (pick 1-2 that best fit, or empty list if none fit):
{scope_descriptions}

## Projects (pick exactly 1, or null if none fit):
{project_descriptions}

## Issues:
{issues_text}

Return a JSON object mapping each issue identifier to its classification.
Every issue identifier listed above MUST appear as a key in the response.
Use the exact scope/project names from the lists above."""

    schema = {
        "type": "object",
        "properties": {
            identifier: {
                "type": "object",
                "properties": {
                    "labels": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "project": {"type": ["string", "null"]},
                },
                "required": ["labels", "project"],
            }
            for identifier in identifiers
        },
        "required": identifiers,
    }

    result = subprocess.run(
        [
            claude_path,
            "-p",
            "--model",
            "haiku",
            "--output-format",
            "json",
            "--json-schema",
            json.dumps(schema),
        ],
        input=prompt,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(
            f"Error: claude CLI failed (exit code {result.returncode})", file=sys.stderr
        )
        sys.exit(1)

    data = json.loads(result.stdout)

    # --output-format json returns a list of message events.
    # The structured JSON is in a StructuredOutput tool_use block within
    # an assistant message. Usage stats are in assistant message events.
    if isinstance(data, list):
        structured_json = None
        total_input = 0
        total_output = 0

        for msg in data:
            if "usage" in msg:
                total_input += msg["usage"].get("input_tokens", 0)
                total_output += msg["usage"].get("output_tokens", 0)

            message = msg.get("message", {})
            for block in message.get("content", []):
                if (
                    block.get("type") == "tool_use"
                    and block.get("name") == "StructuredOutput"
                ):
                    structured_json = block.get("input", {})

        if total_input or total_output:
            print(
                f"\n  Claude usage: {total_input:,} input + {total_output:,} output"
                f" = {total_input + total_output:,} total tokens"
            )

        if structured_json is None:
            print("Error: no structured output in Claude response", file=sys.stderr)
            sys.exit(1)

        return structured_json

    # Single-object envelope (future-proofing)
    classifications = data.get("result", data)
    if isinstance(classifications, str):
        classifications = json.loads(classifications)

    return classifications


def sync(
    api_key: str,
    *,
    sync_labels: bool,
    sync_projects: bool,
    retag: bool,
    archive_old: bool,
) -> None:
    team_id = get_team_id(api_key)
    scopes: list[dict] = []
    label_by_name: dict[str, str] = {}
    projects: list[dict] = []
    lp_by_name: dict[str, str] = {}

    if sync_labels:
        scopes = load_scopes()
        labels = list_labels(api_key, team_id)
        label_by_name = {la["name"]: la["id"] for la in labels}

        labels_created = 0
        for scope in scopes:
            if scope["name"] in label_by_name:
                scope["linear_label_id"] = label_by_name[scope["name"]]
                print(f"  label exists: {scope['name']}")
            else:
                label_id = create_label(
                    api_key, team_id, scope["name"], scope["description"]
                )
                scope["linear_label_id"] = label_id
                label_by_name[scope["name"]] = label_id
                labels_created += 1
                print(f"  label created: {scope['name']}")

        write_scopes(scopes)
        print(f"\n{labels_created} labels created. labels.toml updated.")

    if sync_projects:
        projects = load_projects()
        linear_projects = list_linear_projects(api_key)
        lp_by_name = {p["name"]: p["id"] for p in linear_projects}

        projects_created = 0
        for proj in projects:
            if proj["name"] in lp_by_name:
                proj["linear_project_id"] = lp_by_name[proj["name"]]
                print(f"  project exists: {proj['name']}")
            else:
                project_id = create_linear_project(
                    api_key, team_id, proj["name"], proj["description"]
                )
                proj["linear_project_id"] = project_id
                lp_by_name[proj["name"]] = project_id
                projects_created += 1
                print(f"  project created: {proj['name']}")

        write_projects(projects)
        print(f"{projects_created} projects created. projects.toml updated.\n")

    if retag:
        # Ensure we have data loaded even if --labels/--projects wasn't passed
        if not scopes:
            scopes = load_scopes()
            labels = list_labels(api_key, team_id)
            label_by_name = {la["name"]: la["id"] for la in labels}
            for scope in scopes:
                if scope["name"] in label_by_name:
                    scope["linear_label_id"] = label_by_name[scope["name"]]

        if not projects:
            projects = load_projects()
            linear_projects = list_linear_projects(api_key)
            lp_by_name = {p["name"]: p["id"] for p in linear_projects}

        issues = list_open_issues(api_key)
        if not issues:
            print("No open issues to retag.")
        else:
            print(f"Classifying {len(issues)} issues with Claude...")
            classifications = classify_issues(issues, scopes, projects)

            label_tagged = 0
            project_tagged = 0
            issue_by_id = {i["identifier"]: i for i in issues}

            for identifier, classification in classifications.items():
                issue = issue_by_id.get(identifier)
                if not issue:
                    continue

                existing_label_ids = {
                    la["id"] for la in issue.get("labels", {}).get("nodes", [])
                }

                matched_labels = classification.get("labels", [])
                new_label_ids = set()
                for label_name in matched_labels:
                    label_id = label_by_name.get(label_name)
                    if label_id and label_id not in existing_label_ids:
                        new_label_ids.add(label_id)

                if new_label_ids:
                    all_label_ids = list(existing_label_ids | new_label_ids)
                    update_issue_labels(api_key, issue["id"], all_label_ids)
                    label_tagged += 1
                    added = ", ".join(matched_labels)
                    print(f"  {identifier} +labels {added}")

                matched_proj = classification.get("project")
                current_project = (
                    issue["project"]["name"] if issue.get("project") else None
                )

                if matched_proj is None:
                    if current_project in OLD_SCOPE_PROJECT_NAMES:
                        update_issue_project(api_key, issue["id"], None)
                        print(
                            f"  {identifier} cleared stale project '{current_project}'"
                        )
                elif matched_proj in lp_by_name:
                    target_project_id = lp_by_name[matched_proj]
                    current_project_id = (
                        issue["project"]["id"] if issue.get("project") else None
                    )
                    if current_project_id != target_project_id:
                        update_issue_project(api_key, issue["id"], target_project_id)
                        project_tagged += 1
                        print(f"  {identifier} -> project {matched_proj}")

            print(
                f"\n{label_tagged} issues labeled,"
                f" {project_tagged} issues assigned to projects."
            )

    if archive_old:
        print("\nArchiving old scope-based projects...")
        linear_projects = list_linear_projects(api_key)
        lp_by_name = {p["name"]: p["id"] for p in linear_projects}
        for name in OLD_SCOPE_PROJECT_NAMES:
            if name in lp_by_name:
                archive_project(api_key, lp_by_name[name])
                print(f"  archived: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Penca labels and projects to Linear"
    )
    parser.add_argument(
        "--labels",
        action="store_true",
        help="Sync labels only (from linear/labels.toml)",
    )
    parser.add_argument(
        "--projects",
        action="store_true",
        help="Sync projects only (from linear/projects.toml)",
    )
    parser.add_argument(
        "--retag",
        action="store_true",
        help="Classify open issues via Claude and assign labels + projects",
    )
    parser.add_argument(
        "--archive-old",
        action="store_true",
        help="Archive the old scope-based projects",
    )
    args = parser.parse_args()

    api_key = os.environ.get("LINEAR_API_KEY", "")
    if not api_key:
        print(
            "Error: LINEAR_API_KEY environment variable is not set.\n"
            "Create a personal API key at: https://linear.app/settings/api",
            file=sys.stderr,
        )
        sys.exit(1)

    # If neither --labels nor --projects specified, sync both
    do_labels = args.labels or not args.projects
    do_projects = args.projects or not args.labels

    sync(
        api_key,
        sync_labels=do_labels,
        sync_projects=do_projects,
        retag=args.retag,
        archive_old=args.archive_old,
    )


if __name__ == "__main__":
    main()
