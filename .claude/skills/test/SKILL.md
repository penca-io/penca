---
name: test
description: Run the test suite with pytest
disable-model-invocation: true
allowed-tools: Bash Read
---

Run the Penca test suite.

If arguments are provided, pass them through to pytest:

```bash
just test $ARGUMENTS
```

Otherwise run the full suite:

```bash
just test
```

Report results and any failures.
