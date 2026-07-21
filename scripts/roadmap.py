#!/usr/bin/env python3
"""Fetch open issues from Linear and print a formatted roadmap.

Requires LINEAR_API_KEY environment variable (personal API key).
Create one at: https://linear.app/settings/api

Usage:
    just roadmap
    just roadmap --project "Query Engine"
    just roadmap --priority 2
    just roadmap --label lifecycle
    just roadmap --query "purge data cleanup"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

LINEAR_API_URL = "https://api.linear.app/graphql"
TEAM_KEY = "CHA"

PRIORITY_LABELS = {
    0: "None",
    1: "Urgent",
    2: "High",
    3: "Medium",
    4: "Low",
}

ISSUES_QUERY = """
query($teamKey: String!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
      completedAt: { null: true }
      canceledAt: { null: true }
    }
    orderBy: updatedAt
    first: 100
  ) {
    nodes {
      identifier
      title
      priority
      url
      state { name }
      labels { nodes { name } }
      assignee { name }
      project { name }
      parent { identifier }
      children { nodes { identifier } }
      createdAt
    }
  }
}
"""

ISSUES_QUERY_ALL = """
query($teamKey: String!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
    }
    orderBy: updatedAt
    first: 100
  ) {
    nodes {
      identifier
      title
      priority
      url
      state { name }
      labels { nodes { name } }
      assignee { name }
      project { name }
      parent { identifier }
      children { nodes { identifier } }
      createdAt
    }
  }
}
"""

SEARCH_QUERY = """
query($teamKey: String!, $term: String!) {
  searchIssues(term: $term, filter: {
    team: { key: { eq: $teamKey } }
  }, first: 100, includeComments: true) {
    nodes {
      identifier
      title
      priority
      url
      state { name }
      labels { nodes { name } }
      assignee { name }
      project { name }
      parent { identifier }
      children { nodes { identifier } }
      createdAt
    }
  }
}
"""

CLOSED_STATES = frozenset({"Done", "Canceled", "Cancelled"})


def fetch_issues(
    api_key: str,
    query: str | None = None,
    include_closed: bool = False,
) -> list[dict]:
    if query:
        gql = SEARCH_QUERY
        variables: dict = {"teamKey": TEAM_KEY, "term": query}
        result_key = "searchIssues"
    else:
        gql = ISSUES_QUERY_ALL if include_closed else ISSUES_QUERY
        variables = {"teamKey": TEAM_KEY}
        result_key = "issues"

    payload = json.dumps({"query": gql, "variables": variables}).encode()
    request = urllib.request.Request(
        LINEAR_API_URL,
        data=payload,
        headers={
            "Authorization": api_key,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request) as response:
            data = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        print(f"Linear API HTTP {exc.code}: {body}", file=sys.stderr)
        sys.exit(1)

    if "errors" in data:
        print(f"Linear API error: {data['errors']}", file=sys.stderr)
        sys.exit(1)

    return data["data"][result_key]["nodes"]


def format_summary(issues: list[dict]) -> list[str]:
    """Top-level summary: counts by status and assignee."""
    by_status: dict[str, int] = {}
    by_assignee: dict[str, int] = {}
    for issue in issues:
        status = issue["state"]["name"]
        by_status[status] = by_status.get(status, 0) + 1
        assignee = issue.get("assignee")
        if assignee:
            name = assignee["name"]
            by_assignee[name] = by_assignee.get(name, 0) + 1

    lines = [f"**{len(issues)} open issues**"]
    status_parts = [f"{name}: {count}" for name, count in sorted(by_status.items())]
    lines.append(f"Status: {' · '.join(status_parts)}")
    if by_assignee:
        assignee_parts = [
            f"{name}: {count}" for name, count in sorted(by_assignee.items())
        ]
        lines.append(f"Assigned: {' · '.join(assignee_parts)}")

    lines.append("")
    return lines


def filter_issues(
    issues: list[dict],
    *,
    project: str | None = None,
    priority: int | None = None,
    label: str | None = None,
    include_closed: bool = False,
) -> list[dict]:
    """Filter issues by project name, priority, and/or label (all case-insensitive)."""
    filtered = issues
    if not include_closed:
        filtered = [i for i in filtered if i["state"]["name"] not in CLOSED_STATES]

    if project is not None:
        project_lower = project.lower()
        filtered = [
            i
            for i in filtered
            if i.get("project") and i["project"]["name"].lower() == project_lower
        ]

    if priority is not None:
        filtered = [i for i in filtered if i["priority"] == priority]

    if label is not None:
        label_lower = label.lower()
        filtered = [
            i
            for i in filtered
            if any(
                la["name"].lower() == label_lower
                for la in i.get("labels", {}).get("nodes", [])
            )
        ]

    return filtered


def format_issue(issue: dict, indent: str = "") -> str:
    """Format a single issue line with metadata tags."""
    identifier = issue["identifier"]
    title = issue["title"]
    url = issue["url"]
    status = issue["state"]["name"]
    assignee = issue.get("assignee")
    assignee_name = assignee["name"] if assignee else ""
    project = issue.get("project")
    project_name = project["name"] if project else ""
    labels = [label["name"] for label in issue["labels"]["nodes"]]
    children = issue.get("children", {}).get("nodes", [])

    tags = [f"`{status}`"]
    if project_name:
        tags.append(project_name)

    if assignee_name:
        tags.append(assignee_name)

    if labels:
        tags.append(", ".join(labels))

    if children:
        child_ids = [child["identifier"] for child in children]
        tags.append(f"sub: {', '.join(child_ids)}")

    return f"{indent}- **[{identifier}]({url})** {title}  {' · '.join(tags)}"


def format_roadmap(issues: list[dict]) -> str:
    lines: list[str] = []
    lines.append("# Penca Roadmap")
    lines.append("")

    issue_by_id = {issue["identifier"]: issue for issue in issues}

    # A sub-issue is one whose parent is also in the open issue list.
    sub_issue_ids = {
        issue["identifier"]
        for issue in issues
        if issue.get("parent") and issue["parent"]["identifier"] in issue_by_id
    }

    lines.extend(format_summary(issues))

    # Group top-level issues by priority.
    by_priority: dict[int, list[dict]] = {}
    for issue in issues:
        if issue["identifier"] in sub_issue_ids:
            continue

        priority = issue["priority"]
        by_priority.setdefault(priority, []).append(issue)

    for priority in sorted(by_priority):
        label = PRIORITY_LABELS.get(priority, f"P{priority}")
        group = by_priority[priority]
        lines.append(f"## {label} ({len(group)})")
        lines.append("")
        for issue in group:
            lines.append(format_issue(issue))
            for child_ref in issue.get("children", {}).get("nodes", []):
                child = issue_by_id.get(child_ref["identifier"])
                if child:
                    lines.append(format_issue(child, indent="  "))

        lines.append("")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Penca Linear roadmap")
    parser.add_argument("--project", default=None, help="Filter by project name")
    parser.add_argument(
        "--priority",
        type=int,
        default=None,
        help="Filter by priority (1=Urgent, 2=High, 3=Medium, 4=Low)",
    )
    parser.add_argument("--label", default=None, help="Filter by label name (scope)")
    parser.add_argument(
        "--query", default=None, help="Search issue titles, descriptions, and comments"
    )
    parser.add_argument(
        "--include-closed",
        action="store_true",
        default=False,
        help="Include completed and canceled issues",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    api_key = os.environ.get("LINEAR_API_KEY", "")
    if not api_key:
        print(
            "Error: LINEAR_API_KEY environment variable is not set.\n"
            "Create a personal API key at: https://linear.app/settings/api",
            file=sys.stderr,
        )
        sys.exit(1)

    issues = fetch_issues(api_key, query=args.query, include_closed=args.include_closed)
    issues = filter_issues(
        issues,
        project=args.project,
        priority=args.priority,
        label=args.label,
        include_closed=args.include_closed,
    )
    print(format_roadmap(issues))


if __name__ == "__main__":
    main()
