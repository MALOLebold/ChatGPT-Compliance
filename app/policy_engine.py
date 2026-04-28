from __future__ import annotations

import json
import re
from pathlib import Path

from app.models import EvaluatedComplianceRecord, PolicyMatch, PolicyRule, RawComplianceRecord


class PolicyLibrary:
    """Loads and evaluates a deterministic rule library."""

    def __init__(self, path: Path, label: str) -> None:
        self.path = path
        self.label = label
        self.rules = self._load_rules()

    def _load_rules(self) -> list[PolicyRule]:
        if not self.path.exists():
            raise FileNotFoundError(f"Policy file not found: {self.path}")

        payload = json.loads(self.path.read_text(encoding="utf-8"))
        rules = payload.get("rules", payload)
        loaded: list[PolicyRule] = []
        for item in rules:
            loaded.append(
                PolicyRule(
                    rule_id=item["rule_id"],
                    rule_name=item["rule_name"],
                    policy_family=item["policy_family"],
                    description=item["description"],
                    match_logic=item["match_logic"],
                    severity=item["severity"],
                    report_category=item["report_category"],
                )
            )
        return loaded

    def evaluate(self, record: RawComplianceRecord) -> list[PolicyMatch]:
        matches: list[PolicyMatch] = []
        normalized_prompt = record.prompt_text.lower()
        for rule in self.rules:
            keyword_hits = [
                phrase
                for phrase in rule.match_logic.get("keywords", [])
                if phrase.lower() in normalized_prompt
            ]
            regex_hits = [
                pattern
                for pattern in rule.match_logic.get("regex", [])
                if re.search(pattern, record.prompt_text, flags=re.IGNORECASE)
            ]

            if not keyword_hits and not regex_hits:
                continue

            reasons: list[str] = []
            if keyword_hits:
                reasons.append(f"keywords matched: {', '.join(keyword_hits)}")
            if regex_hits:
                reasons.append(f"regex matched: {', '.join(regex_hits)}")

            matches.append(
                PolicyMatch(
                    rule_id=rule.rule_id,
                    rule_name=rule.rule_name,
                    policy_family=rule.policy_family,
                    severity=rule.severity,
                    report_category=rule.report_category,
                    reason="; ".join(reasons),
                )
            )

        return matches


class PolicyEvaluator:
    """Combines the OpenAI and company rule libraries into one result."""

    def __init__(self, openai_library: PolicyLibrary, company_library: PolicyLibrary) -> None:
        self.openai_library = openai_library
        self.company_library = company_library

    def evaluate(
        self,
        record: RawComplianceRecord,
        report_period: str,
    ) -> EvaluatedComplianceRecord:
        openai_matches = self.openai_library.evaluate(record)
        company_matches = self.company_library.evaluate(record)

        return EvaluatedComplianceRecord(
            source_record_id=record.source_record_id,
            report_period=report_period,
            prompt_timestamp=record.prompt_timestamp,
            user_id=record.user_id,
            conversation_id=record.conversation_id,
            prompt_text=record.prompt_text,
            openai_policy_matches=openai_matches,
            company_policy_matches=company_matches,
            severity=self._derive_severity(openai_matches, company_matches),
            disposition=self._derive_disposition(openai_matches, company_matches),
            review_status="pending_review",
            review_notes=self._build_reason_summary(openai_matches, company_matches),
            report_included=True,
        )

    def _derive_severity(
        self,
        openai_matches: list[PolicyMatch],
        company_matches: list[PolicyMatch],
    ) -> str:
        severity_order = {"none": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
        current = "none"
        for match in [*openai_matches, *company_matches]:
            if severity_order.get(match.severity, 0) > severity_order.get(current, 0):
                current = match.severity
        return current

    @staticmethod
    def _derive_disposition(
        openai_matches: list[PolicyMatch],
        company_matches: list[PolicyMatch],
    ) -> str:
        if openai_matches and company_matches:
            return "potential_both"
        if openai_matches:
            return "potential_openai_policy_issue"
        if company_matches:
            return "potential_company_policy_issue"
        return "no_match"

    @staticmethod
    def _build_reason_summary(
        openai_matches: list[PolicyMatch],
        company_matches: list[PolicyMatch],
    ) -> str:
        reasons = [match.reason for match in [*openai_matches, *company_matches]]
        return " | ".join(reasons)
