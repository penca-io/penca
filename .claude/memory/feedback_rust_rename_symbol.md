---
name: feedback_rust_rename_symbol
description: For typed Rust symbol renames prefer mcp__language-server__rename_symbol, BUT it applies edits at stale coordinates in files Edit-ed earlier in the session (verify those sites after) and the user wants the language server killed after use (rust-analyzer holds ~3GB)
type: feedback
originSessionId: 1e2a19a2-2bb2-44f0-b8a5-624c302e21f7
---
For typed-symbol renames in Rust, prefer `mcp__language-server__rename_symbol` over `LSP findReferences` + `Edit`. The MCP tool drives `textDocument/rename` and applies the resulting `WorkspaceEdit` atomically — correct for trait-method renames (every `impl` body gets rewritten), aliased imports (`use foo::Foo as Bar` is preserved), and cross-crate references.

**Why:** `LSP findReferences` returns the call/use sites only; you then have to Edit each one yourself, and you'll miss trait impl bodies, macro-generated tokens that the symbol search doesn't return, and aliased re-exports. `rename_symbol` produces a complete `WorkspaceEdit` set the harness applies atomically.

**How to apply:** Use `mcp__language-server__rename_symbol` for any typed Rust identifier rename (function, method, trait, struct, enum variant). Fall back to manual Grep+Edit only when the LSP returns an error (typically macro-generated tokens that rust-analyzer can't trace).

**Stale-coordinate hazard (2026-07-04, CHA-484):** rust-analyzer's view can lag files modified with the Edit tool earlier in the same session — the rename then lands edits at stale line/col positions in exactly those files (observed: replaced the wrong line in a struct Default impl; spliced the new name into an unrelated comment word). Unedited files rename cleanly. After any rename, grep every file the session previously edited for mangled tokens and run `cargo check --workspace --all-targets` before trusting it.

**Shut the language server down after the rename (user request, 2026-07-04):** rust-analyzer holds ~3GB RSS and keeps burning CPU. When done with the language-server tool for the task, kill it — but scope to THIS session's instance on this shared VM: list candidates with `pgrep -af "mcp-language-server|rust-analyzer"`, then verify a candidate PID's ancestor chain reaches this session's own claude process (walk `ps -o ppid= -p <pid>` upward, or check `pgrep -P <this-session's-claude-pid>` output) before killing it and its rust-analyzer / proc-macro-srv children. Identical command lines are NOT a discriminator — every session in this repo spawns `mcp-language-server --workspace .`. If ancestry is ambiguous, leave it running. Never blanket `pkill -f rust-analyzer`. It restarts on next tool use.
