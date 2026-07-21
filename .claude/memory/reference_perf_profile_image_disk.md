---
name: reference_perf_profile_image_disk
description: perf-test --profile image builds blow the disk via 5 parallel identical unpacks; pre-build one service then PENCA_SKIP_BUILD=1
metadata: 
  node_type: memory
  type: reference
  originSessionId: 5e2dc0f8-5196-4877-8a1a-eb23cd5b2a6c
---

`just perf-test --profile` builds the DWARF `profiling` image (~12 GB unpacked vs ~1 GB release). Plain `penca-up --build` lets compose build the SAME `penca-rust-server:latest` once per service → five parallel layer unpacks race into separate containerd snapshots and transiently multiply disk several-fold (`no space left on device` twice on a 74 GB VM, 2026-06-10, CHA-417).

**How to apply:** pre-build once, then skip build at bring-up:
```
CARGO_PROFILE=profiling docker compose -f docker/compose.yml --env-file docker/test.env --profile infra --profile penca-backend build query
PENCA_SKIP_BUILD=1 just perf-test --profile <paths>
```
Also: host `target/debug` (30 GB of regenerable check artifacts) is the first thing to drop for headroom, and the big profiling build can GC-evict the *release* chef layer → the next integration-test pays a full dep rebuild. Related: [[reference_docker_builder_prune_hangs]].
