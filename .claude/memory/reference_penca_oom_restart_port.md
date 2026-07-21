---
name: penca-oom-restart-reassigns-port
description: A large perf load can OOM-kill a penca container; docker start reassigns its host port — restore with penca-down/up
metadata: 
  node_type: memory
  type: reference
  originSessionId: 156cf9b3-02aa-4acd-854c-0928ebfb20e0
---

Running a very large `just perf-test`-style load on this VM (≈7 GB RAM) can **OOM-kill a penca service container** mid-run — e.g. `penca-lifecycle-1` exited `137` (SIGKILL/OOM) while snapshotting 10M rows to Lance, surfacing client-side as `failed to connect to all addresses … Connection refused`. Check `docker ps -a | grep penca` for an `Exited (137)`.

**Do not** recover with `docker start <container>`: Docker assigns it a **new random host port** (the original ephemeral binding was released on stop), but `docker/.client.env` still points at the old port → still "connection refused". Confirm via `docker ps --format '{{.Names}} {{.Ports}}'` vs the `*_URL` ports in `.client.env`.

**Correct recovery:** `just penca-down` then `just penca-up` — recreates all containers, regenerates `.client.env` with the actual ports, and (with `down -v`) frees the partial-load leftover data. The cached backend image means `penca-up` doesn't recompile, so this is ~2 min, not a from-scratch build.

Empirically the wall on this VM is between pgbench `scale 10` (1M/500k, fine) and `scale 100` (10M/5M, OOMs the lifecycle snapshot).
