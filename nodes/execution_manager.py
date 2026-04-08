from __future__ import annotations

from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict

from rich.console import Console

from nodes.node_contracts import NodeMeta, NodeResult
from nodes.state_reducer import compute_patch, merge_patch
from nodes.telemetry import TelemetryStore
from nodes.run_modes import RUN_MODES

console = Console()


def _wrap_node(
    logical_name: str,
    func: Callable[[dict], dict],
    state: dict,
    bundle: str,
    telemetry: TelemetryStore,
) -> NodeResult:
    started = telemetry.start()
    before = deepcopy(state)

    try:
        after = func(deepcopy(state))
        patch = compute_patch(before, after, logical_name)
        result = NodeResult(
            node_name=logical_name,
            status="success",
            patch=patch,
            meta=NodeMeta(bundle=bundle),
        )
        telemetry.record(logical_name, bundle, started, "success")
        return result
    except Exception as e:
        telemetry.record(logical_name, bundle, started, "failed", error=str(e))
        console.print(f"[bold red]CRITICAL NODE FAILURE ({logical_name}): {e}[/bold red]")
        return NodeResult(
            node_name=logical_name,
            status="failed",
            patch={"status": "degraded"},
            meta=NodeMeta(bundle=bundle, error=str(e)),
        )


def _load_nodes() -> Dict[str, Callable[[dict], dict]]:
    from nodes.orchestrator_node import process as orchestrator_process
    from nodes.content_fetcher_node import process as content_fetcher_process
    from nodes.prospector_node import process as prospector_process
    from nodes.business_profile_selector_node import process as business_profile_selector_process
    from nodes.content_strategist_node import process as content_strategist_process
    from nodes.content_engineering_node import process as content_engineering_process
    from nodes.schema_generation_node import process as schema_generation_process
    from nodes.crawler_policy_node import process as crawler_policy_process
    from nodes.earned_media_node import process as earned_media_process
    from nodes.source_quality_node import process as source_quality_process
    from nodes.researcher_node import process as researcher_process
    from nodes.model_analytics_node import process as model_analytics_process
    from nodes.implementation_blueprint_node import process as implementation_blueprint_process
    from nodes.agentic_readiness_node import process as agentic_readiness_process
    from nodes.validator_node import process as validator_process
    from nodes.finalizer_node import process as finalizer_process

    return {
        "orchestrator": orchestrator_process,
        "content_fetcher": content_fetcher_process,
        "prospector": prospector_process,
        "business_profile_selector": business_profile_selector_process,
        "content_strategist": content_strategist_process,
        "content_engineering": content_engineering_process,
        "schema_generation": schema_generation_process,
        "crawler_policy": crawler_policy_process,
        "earned_media": earned_media_process,
        "source_quality": source_quality_process,
        "researcher": researcher_process,
        "model_analytics": model_analytics_process,
        "implementation_blueprint": implementation_blueprint_process,
        "agentic_readiness": agentic_readiness_process,
        "validator": validator_process,
        "finalizer": finalizer_process,
    }


def run_hybrid_pipeline(initial_state: dict, run_mode: str = "standard") -> dict:
    nodes = _load_nodes()
    cfg = RUN_MODES.get(run_mode, RUN_MODES["standard"])
    telemetry = TelemetryStore()

    state = deepcopy(initial_state)

    truth_spine = [
        "content_fetcher",
        "orchestrator",
        "prospector",
        "business_profile_selector",
    ]

    for node_name in truth_spine:
        result = _wrap_node(node_name, nodes[node_name], state, "truth_spine", telemetry)
        state = merge_patch(state, result)
        if result.status == "failed":
            state["status"] = "degraded"

    if not cfg["parallel_enabled"]:
        sequential_tail = [
            "content_strategist",
            "content_engineering",
            "schema_generation",
            "crawler_policy",
            "earned_media",
            "source_quality",
            "researcher",
            "model_analytics",
            "implementation_blueprint",
            "agentic_readiness",
            "validator",
            "finalizer",
        ]
        for node_name in sequential_tail:
            result = _wrap_node(node_name, nodes[node_name], state, "sequential_tail", telemetry)
            state = merge_patch(state, result, allow_protected=node_name in {"validator", "finalizer"})
            if result.status == "failed":
                state["status"] = "degraded"

        state["execution_telemetry"] = telemetry.export()
        state["execution_mode"] = "hybrid_manager_sequential_compat"
        return state

    strategy_branch_state = deepcopy(state)
    stress_branch_state = deepcopy(state)

    def run_strategy_branch(branch_state: dict) -> dict:
        branch_nodes = [
            "content_strategist",
            "content_engineering",
            "schema_generation",
            "crawler_policy",
            "earned_media",
            "source_quality",
            "agentic_readiness",
        ]
        local = deepcopy(branch_state)
        for name in branch_nodes:
            result = _wrap_node(name, nodes[name], local, "strategy_branch", telemetry)
            local = merge_patch(local, result)
            if result.status == "failed":
                local["status"] = "degraded"
        return local

    def run_stress_branch(branch_state: dict) -> dict:
        branch_nodes = [
            "researcher",
            "model_analytics",
        ]
        local = deepcopy(branch_state)
        for name in branch_nodes:
            result = _wrap_node(name, nodes[name], local, "stress_branch", telemetry)
            local = merge_patch(local, result)
            if result.status == "failed":
                local["status"] = "degraded"
        return local

    with ThreadPoolExecutor(max_workers=cfg["max_workers"]) as ex:
        fut_strategy = ex.submit(run_strategy_branch, strategy_branch_state)
        fut_stress = ex.submit(run_stress_branch, stress_branch_state)

        strategy_out = fut_strategy.result()
        stress_out = fut_stress.result()

    strategy_patch = compute_patch(state, strategy_out, "finalizer")
    stress_patch = compute_patch(state, stress_out, "finalizer")

    state = merge_patch(
        state,
        NodeResult("strategy_branch_merge", "success", strategy_patch, NodeMeta(bundle="merge")),
    )
    state = merge_patch(
        state,
        NodeResult("stress_branch_merge", "success", stress_patch, NodeMeta(bundle="merge")),
    )

    for node_name in ["implementation_blueprint", "validator", "finalizer"]:
        result = _wrap_node(node_name, nodes[node_name], state, "final_synthesis", telemetry)
        state = merge_patch(state, result, allow_protected=node_name in {"validator", "finalizer"})
        if result.status == "failed":
            state["status"] = "degraded"

    state["execution_telemetry"] = telemetry.export()
    state["execution_mode"] = "hybrid_manager_parallel_phase1"
    return state