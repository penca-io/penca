---
name: lint
description: Run ruff linting and report issues
disable-model-invocation: true
allowed-tools: Bash Read
---

Run the linter on the Penca codebase.

```bash
just lint
```

Report any issues. If there are auto-fixable issues, ask before applying fixes.
