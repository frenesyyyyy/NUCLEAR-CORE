from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


NodeStatus = str  # success | failed | degraded | skipped


@dataclass
class NodeMeta:
    duration_ms: int = 0
    error: Optional[str] = None
    retries: int = 0
    cost_estimate: float = 0.0
    bundle: str = "default"


@dataclass
class NodeResult:
    node_name: str
    status: NodeStatus
    patch: Dict[str, Any] = field(default_factory=dict)
    meta: NodeMeta = field(default_factory=NodeMeta)


PROTECTED_KEYS = {
    "agency_verdict",
    "agency_verdict_reason",
    "confidence_score",
    "audit_integrity_score",
    "audit_validity_summary",
    "overall_pipeline_readiness",
    "validation",
    "roi_verified",
    "validator_notes",
    "decision_summary",
    "decision_risks",
    "decision_next_step",
}

ALLOWED_WRITE_KEYS = {
    "orchestrator": None,
    "content_fetcher": {
        "client_content_raw",
        "client_content_clean",
        "client_content_depth",
        "schema_type_counts",
        "page_title",
        "og_tags",
        "robots_txt_status",
        "json_ld_blocks",
        "js_fallback_used",
        "audit_integrity_status",
        "audit_integrity_reasons",
        "source_of_truth_mode",
        "extracted_on_site_address",
        "fetch_notes",
        "fetch_debug",
        "js_heavy_suspect",
        "render_fallback_used",
        "render_source",
        "anti_bot_detected",
        "render_success",
        "render_notes",
        "fetched_page_urls",
        "fetched_page_types",
        "site_fingerprint",
        "acquisition_mode",
        "page_selection_notes",
        "status",
    },
    "prospector": {
        "raw_data_complete",
        "external_sources",
        "prospector_notes",
        "external_data_quality",
        "classification_notes",
        "validated_location",
        "location_confidence",
        "location_inference_mode",
        "discovered_location",
        "brand_name",
        "target_industry",
        "target_audience_summary",
        "persona_matrix",
        "status",
    },
    "business_profile_selector": {
        "business_profile_key",
        "business_profile",
        "classification_reliability",
        "classification_evidence",
        "status",
    },
    "content_strategist": None,
    "content_engineering": None,
    "schema_generation": {
        "schema_recommendations",
        "status",
    },
    "crawler_policy": {
        "crawler_policy",
        "status",
    },
    "earned_media": {
        "earned_media",
        "external_sources_raw",
        "status",
    },
    "source_quality": {
        "source_taxonomy",
        # v4.5 Authority Upgrade — additive fields (safe, optional for downstream)
        "source_pack_used",
        "source_family_breakdown",
        "penalized_relevant_gaps",
        "ignored_irrelevant_gaps",
        "relevant_gap_count",
        "trust_anchors_found",
        "source_detection_notes",
        "status",
    },
    "agentic_readiness": {
        "agentic_readiness",
        "status",
    },
    "researcher": None,
    "model_analytics": {
        "model_analytics",
        "raw_visibility_score",
        "authority_adjusted_visibility_score",
        "authority_composite",
        "profile_weight_pack_used",
        "position_adjusted_word_count",
        "status",
    },
    "implementation_blueprint": {
        "implementation_blueprint",
        "status",
    },
    "validator": None,
    "finalizer": None,
}