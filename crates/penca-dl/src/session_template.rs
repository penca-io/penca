//! Process-wide cold-tier DataFusion session template (CHA-421).
//!
//! Each cold read builds a DataFusion `SessionContext`. In a *warm* process
//! that is ~128 µs/call (release): DataFusion's default functions are
//! process-global `OnceLock` singletons, so `SessionContext::new()` does not
//! rebuild the UDFs — it collects the already-initialised `Arc`s into a fresh
//! HashMap and assembles the analyzer/optimizer rule lists. (The often-quoted
//! ~1.4 ms is the *cold* first-call cost — initialising those singletons —
//! paid once per process either way; a debug build also inflates the warm cost
//! to ~2 ms. CHA-353's bench reported the debug/cold figure.)
//!
//! This module trims the warm per-call cost ~45% (~128 µs → ~71 µs): build ONE
//! template at startup ([`build_cold_session_template`]) and derive every
//! per-unit context from it ([`derive_cold_session`]) — a clone of the
//! template's `scalar_functions` + rules (Arc/HashMap clones) with a FRESH
//! `catalog_list`. ~71 µs is the floor: a fresh `SessionState` must clone the
//! ~800-entry registry HashMap, so this can't reach "a few µs" without sharing
//! one context across queries — which the fresh `catalog_list` deliberately
//! prevents.
//!
//! The fresh catalog list is load-bearing: a shared `Arc<dyn CatalogProviderList>`
//! would let one merge's table registrations (`l`, `exclusion`, `upsert_log`, …)
//! collide with a concurrent merge's. `as_of`/schema live on the per-query
//! providers + SQL, not on the `SessionState`, so sharing the registry is
//! correctness-safe as long as the catalog stays per-unit (the CHA-421 design
//! note).

use std::sync::Arc;

use datafusion::catalog::MemoryCatalogProviderList;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::execution::session_state::SessionStateBuilder;

/// Build the process-wide cold-session template: the full default function
/// registry + analyzer/optimizer rule sets. Identical to the `SessionState`
/// behind `SessionContext::new()`, but built ONCE per service and reused by
/// [`derive_cold_session`]. Each service binary builds this at startup and
/// injects the resulting `Arc<SessionState>` into its cold
/// [`crate::driver::DatafusionDlDriver`].
pub fn build_cold_session_template() -> SessionState {
    SessionStateBuilder::new().with_default_features().build()
}

/// Derive a per-unit cold [`SessionContext`] from `template`: a ~71 µs clone
/// (release) of the template's `scalar_functions` + analyzer/optimizer rules +
/// config (Arc/HashMap clones) with a FRESH, empty `catalog_list`, so concurrent
/// cold reads never collide on the fixed table names (`l`, `exclusion`,
/// `upsert_log`, `delete_log`).
///
/// The fresh `MemoryCatalogProviderList` is the load-bearing part: cloning a
/// `SessionState` keeps its `catalog_list: Arc<dyn CatalogProviderList>`
/// Arc-shared, so without this swap two derived contexts would register their
/// tables into the SAME catalog (the CHA-421 design-note trap). `build()`
/// recreates the default catalog/schema in the fresh list
/// (`create_default_catalog_and_schema`), so unqualified `register_table` / SQL
/// resolves exactly as `SessionContext::new()` does today.
///
/// CHA-368: also the residual-filter session for the all-hot read path
/// (`penca_api::query::stream_all_hot`) — DataFusion is the single user-filter
/// engine, so the hot tier evaluates its residual through the same
/// template-derived context the cold tier uses, rather than a fresh
/// `SessionContext::new()`.
pub fn derive_cold_session(template: &SessionState) -> SessionContext {
    derive_cold_session_inner(template, None)
}

/// [`derive_cold_session`] pinned to `target_partitions = 1` — for the
/// ordered (`ByPlan`) snapshot scan (CHA-404): with one target
/// partition the physical optimizer never inserts a `RepartitionExec`
/// above the single-partition snapshot provider, so plan order survives
/// to the output.
pub(crate) fn derive_cold_session_single_partition(template: &SessionState) -> SessionContext {
    derive_cold_session_inner(template, Some(1))
}

fn derive_cold_session_inner(
    template: &SessionState,
    target_partitions: Option<usize>,
) -> SessionContext {
    // `new_from_existing` sets `create_default_catalog_and_schema = false`
    // (the template already had a default catalog), but we swap in an EMPTY
    // catalog_list — so re-enable creation on the config to seed
    // `datafusion`/`public` into the fresh list, else unqualified
    // `register_table` / SQL fails with "failed to resolve catalog: datafusion".
    let mut config = template
        .config()
        .clone()
        .with_create_default_catalog_and_schema(true);
    if let Some(tp) = target_partitions {
        config = config.with_target_partitions(tp);
    }
    let state = SessionStateBuilder::new_from_existing(template.clone())
        .with_config(config)
        .with_catalog_list(Arc::new(MemoryCatalogProviderList::new()))
        .build();
    SessionContext::new_with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    /// RT1 (CHA-421): `derive_cold_session` produces a context cloned from the
    /// *given* template (its registry / rules / config), but with a FRESH,
    /// independent catalog.
    ///
    /// The discriminator is a sentinel `config` value the default
    /// `SessionContext::new()` would never carry: a clone of the template
    /// preserves it, a fresh build does not. (Probing a default UDF `Arc`
    /// can't discriminate — DataFusion's default functions are process-global
    /// `OnceLock` singletons shared even across independent `new()` contexts.)
    ///
    /// Red on the I1 stub (derive ignores the template → fresh `new()` →
    /// default batch size, sentinel lost); green once I1 clones the template.
    #[tokio::test]
    async fn derive_cold_session_clones_template_and_isolates_catalog() {
        const SENTINEL_BATCH_SIZE: usize = 4242;

        let mut template = build_cold_session_template();
        // A value the default registry would never carry — proves the derived
        // context is cloned from THIS template, not rebuilt from `new()`.
        template.config_mut().options_mut().execution.batch_size = SENTINEL_BATCH_SIZE;

        let a = derive_cold_session(&template);
        let b = derive_cold_session(&template);

        // (1) Cloned from the template: both derives carry the sentinel config
        // (registry + analyzer/optimizer rules ride along in the same clone),
        // rather than the defaults a fresh `SessionContext::new()` would have.
        for (label, ctx) in [("a", &a), ("b", &b)] {
            assert_eq!(
                ctx.state().config().options().execution.batch_size,
                SENTINEL_BATCH_SIZE,
                "derive `{label}` must clone the template (sentinel config \
                 preserved), not rebuild via SessionContext::new()",
            );
        }

        // (2) Isolated catalog: a table registered in `a` is invisible in `b`
        // — each derive gets a FRESH catalog_list (the `Arc<dyn
        // CatalogProviderList>` trap the design note warns about).
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
        a.register_table("isolation_probe", Arc::new(mem)).unwrap();
        assert!(
            a.table_exist("isolation_probe").unwrap(),
            "table registered into `a` must exist in `a`",
        );
        assert!(
            !b.table_exist("isolation_probe").unwrap(),
            "each derived session must have an independent catalog_list — a \
             registration in `a` must NOT be visible in `b`",
        );
    }

    // CHA-421 bench: per-unit cold-session cost. In release a *warm*
    // `SessionContext::new()` is ~128 µs/call (the default registry is global
    // OnceLock singletons, so warm `new()` only collects the Arcs into a
    // HashMap; the ~1.4 ms everyone quotes is the one-time cold/debug figure).
    // `derive_cold_session` from a once-built template clones that HashMap
    // instead → ~71 µs/call, so the per-cold-read saving is ~57 µs (~1.8x).
    // ~71 µs is the floor (cloning the ~800-entry registry). Ignored by default:
    //   cargo test -p penca-dl bench_derive_vs_new -- --ignored --nocapture
    #[test]
    #[ignore = "timing microbench"]
    fn bench_derive_vs_new() {
        let n = 500u32;

        // Warmup (page in code, registries, allocator).
        for _ in 0..50 {
            black_box(SessionContext::new());
        }
        let template = build_cold_session_template();
        for _ in 0..50 {
            black_box(derive_cold_session(&template));
        }

        // (a) SessionContext::new() per call — the pre-CHA-421 per-cold-read cost.
        let t = Instant::now();
        for _ in 0..n {
            black_box(SessionContext::new());
        }
        let new_us = t.elapsed().as_micros() as f64 / n as f64;

        // (b) derive_cold_session(&template) — the CHA-421 per-unit cost, with
        // the expensive template built once outside the loop.
        let t = Instant::now();
        for _ in 0..n {
            black_box(derive_cold_session(&template));
        }
        let derive_us = t.elapsed().as_micros() as f64 / n as f64;

        println!("\n=== CHA-421 per-unit cold-session cost (warm, n={n}) ===");
        println!("(a) SessionContext::new()            : {new_us:8.1} µs/call");
        println!("(b) derive_cold_session(&template)   : {derive_us:8.1} µs/call");
        println!(
            "saving per cold session              : {:8.1} µs/call\n",
            new_us - derive_us
        );
    }
}
