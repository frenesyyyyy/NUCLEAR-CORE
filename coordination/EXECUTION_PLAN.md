# EXECUTION PLAN — Hybrid Parallel Upgrade Phase 1
## doublecheck all the logic behind the pipeline files and analyze the required files just to have an idea (exclude all implementation for the executable UI)
## Goal
Add a production-safe orchestration layer around the existing sequential GEO pipeline.

## Current actual runner order
orchestrator -> content_fetcher -> prospector -> business_profile_selector -> content_strategist -> content_engineering -> schema_generation -> crawler_policy -> earned_media -> source_quality -> researcher -> model_analytics -> implementation_blueprint -> agentic_readiness -> validator -> finalizer

## Safe Phase 1 target
Sequential truth spine:
- orchestrator
- content_fetcher
- prospector
- business_profile_selector

Parallel branch A:
- content_strategist
- content_engineering
- schema_generation
- crawler_policy
- earned_media
- source_quality
- agentic_readiness

Parallel branch B:
- researcher
- model_analytics

Sequential synthesis:
- implementation_blueprint
- validator
- finalizer

## Invariants
- Do not change validator scoring semantics.
- Do not change finalizer export semantics.
- Do not parallelize implementation_blueprint in Phase 1.
- Do not parallelize validator in Phase 1.
- Do not parallelize finalizer in Phase 1.
- Do not mutate shared state directly in parallel branches.
- Always preserve legacy runner.