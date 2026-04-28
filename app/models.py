from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RawComplianceRecord:
    source_record_id: str
    workspace_id: str
    user_id: str
    conversation_id: str
    message_id: str
    prompt_text: str
    prompt_timestamp: str
    raw_payload: dict[str, Any]


@dataclass(slots=True)
class PolicyRule:
    rule_id: str
    rule_name: str
    policy_family: str
    description: str
    match_logic: dict[str, Any]
    severity: str
    report_category: str


@dataclass(slots=True)
class PolicyMatch:
    rule_id: str
    rule_name: str
    policy_family: str
    severity: str
    report_category: str
    reason: str


@dataclass(slots=True)
class EvaluatedComplianceRecord:
    source_record_id: str
    report_period: str
    prompt_timestamp: str
    user_id: str
    conversation_id: str
    prompt_text: str
    openai_policy_matches: list[PolicyMatch] = field(default_factory=list)
    company_policy_matches: list[PolicyMatch] = field(default_factory=list)
    severity: str = "none"
    disposition: str = "no_match"
    review_status: str = "pending_review"
    review_notes: str = ""
    report_included: bool = True


@dataclass(slots=True)
class PipelineRunSummary:
    run_id: str
    period: str
    start_date: str
    end_date: str
    source: str
    records_pulled: int
    records_inserted: int
    duplicates_skipped: int
    evaluated_records: int
    no_match_count: int
    openai_flag_count: int
    company_flag_count: int
    dual_flag_count: int
    failures: int
    detail_report_path: str
    summary_report_path: str
