from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from xml.sax.saxutils import escape, quoteattr

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gpt_compliance_exporter.extraction import extract_prompts, is_gpt_response_record


WINDOW_WORDS = 8
DEFAULT_EXCERPT_CHARS = 260

RISK_LEVELS = ("none", "low", "medium", "high", "critical")

REVIEWED_COLUMNS = [
    "risk_level",
    "risk_score",
    "confidence",
    "needs_human_review",
    "categories",
    "reasons",
    "matched_evidence",
    "line_number",
    "event_id",
    "conversation_id",
    "message_id",
    "user_id",
    "user_email",
    "created_at",
    "prompt",
]
FLAGGED_COLUMNS = REVIEWED_COLUMNS
USER_SUMMARY_COLUMNS = [
    "user_email",
    "total_prompts",
    "flagged_prompts",
    "max_risk_score",
    "critical",
    "high",
    "medium",
    "low",
    "none",
]
CATEGORY_SUMMARY_COLUMNS = ["category", "count", "max_risk_score", "users_affected"]
RISK_SUMMARY_COLUMNS = ["risk_level", "count"]


RULE_CONFIG: Dict[str, Any] = {
    # Allowlist phrases prevent "client" from being treated as risky by itself.
    "allowlist_phrases": (
        "client meeting",
        "client email",
        "client relationship",
        "client service",
        "client follow-up",
        "client follow up",
        "email to a client",
        "nicer email to a client",
    ),
    "context_terms": {
        "client": (
            "client",
            "customer",
            "engagement",
            "taxpayer",
            "patient",
        ),
        "sensitive_document": (
            "1040",
            "1099",
            "w-2",
            "k-1",
            "tax return",
            "financial statement",
            "financial statements",
            "balance sheet",
            "income statement",
            "cash flow",
            "trial balance",
            "general ledger",
            "workpaper",
            "work paper",
            "audit file",
            "engagement letter",
            "bank statement",
            "bank statements",
            "payroll",
        ),
        "sensitive_data": (
            "ssn",
            "social security",
            "ein",
            "date of birth",
            "dob",
            "driver's license",
            "passport",
            "confidential",
            "proprietary",
            "internal use only",
            "do not distribute",
        ),
        "analysis_actions": (
            "analyze",
            "summarize",
            "review",
            "extract",
            "compare",
            "classify",
            "calculate",
            "compute",
            "reconcile",
            "validate",
        ),
    },
    # Internal policy rules are phrase/context checks derived from M+N policy language.
    "internal_policy": {
        "audit_evidence": {
            "score": 82,
            "confidence": "high",
            "reason": "Prompt appears to ask AI to generate or validate audit evidence, substantive testing, or professional-standard support.",
            "patterns": (
                r"\b(generate|create|validate|verify|provide|support)\b.{0,90}\b(audit evidence|audit support|evidence for audit)\b",
                r"\b(substantive testing|substantive audit procedure|risk assessment|gaas|workpaper support)\b",
            ),
        },
        "tax_or_accuracy_calculation": {
            "score": 80,
            "confidence": "high",
            "reason": "Prompt appears to request a tax, reconciliation, or audit calculation requiring accuracy.",
            "patterns": (
                r"\b(calculate|compute|determine|prepare|finalize)\b.{0,90}\b(tax liability|tax due|tax calculation|reconciliation|substantive audit)\b",
                r"\b(reconcile|reconciliation)\b.{0,90}\b(bank|ledger|trial balance|financial statement|tax)\b",
            ),
        },
        "final_deliverable_without_review": {
            "score": 72,
            "confidence": "medium",
            "reason": "Prompt appears to request a final deliverable or client-ready output without documented review/sign-off.",
            "patterns": (
                r"\b(final|finalize|ready to send|send to client)\b.{0,120}\b(without|no need for|skip)\b.{0,90}\b(review|sign[- ]?off|approval)\b",
                r"\b(final deliverable|client deliverable|final report|opinion letter)\b.{0,120}\b(without review|no review)\b",
            ),
        },
        "replace_professional_judgment": {
            "score": 80,
            "confidence": "high",
            "reason": "Prompt appears to use AI to replace professional judgment, quality control, review, or standards.",
            "patterns": (
                r"\b(replace|bypass|skip|avoid|work around)\b.{0,90}\b(professional judgment|human review|qc|quality control|approval|gaas|professional standards)\b",
                r"\b(do not tell|hide from|avoid logging|avoid audit trail)\b",
            ),
        },
    },
    # OpenAI policy checks cover current Usage Policy categories at a pragmatic prompt-review level.
    "openai_policy": {
        "malware_or_phishing": {
            "score": 95,
            "confidence": "high",
            "reason": "Prompt appears related to malware, phishing, credential theft, ransomware, or abusive cyber activity.",
            "patterns": (
                r"\b(phishing|credential theft|steal credentials|keylogger|ransomware|malware|exploit kit|botnet)\b",
                r"\b(bypass|evade)\b.{0,90}\b(edr|antivirus|detection|mfa|authentication)\b",
                r"\b(sql injection|xss|privilege escalation)\b.{0,90}\b(without permission|unauthorized|stealth)\b",
            ),
        },
        "illegal_or_fraud": {
            "score": 82,
            "confidence": "high",
            "reason": "Prompt appears related to fraud, scams, impersonation, illegal activity, or deception.",
            "patterns": (
                r"\b(scam|fraud|impersonate|fake identity|spam campaign|money laundering|forge|forgery)\b",
                r"\b(trick|deceive|manipulate)\b.{0,90}\b(user|customer|voter|employee|person)\b",
            ),
        },
        "threats_harassment_or_hate": {
            "score": 88,
            "confidence": "high",
            "reason": "Prompt appears related to threats, harassment, intimidation, hate, or violent harm.",
            "patterns": (
                r"\b(threaten|harass|intimidate|doxx|swat)\b",
                r"\b(terrorism|terrorist|violent attack|mass casualty)\b",
                r"\b(hate speech|racial slur|dehumanize)\b",
            ),
        },
        "weapons_or_physical_harm": {
            "score": 92,
            "confidence": "high",
            "reason": "Prompt appears to request weapons, explosives, poison, or physical-harm enablement.",
            "patterns": (
                r"\b(make|build|assemble|obtain|use)\b.{0,90}\b(bomb|explosive|firearm|weapon|poison|chemical weapon|biological weapon)\b",
            ),
        },
        "minor_sexual_content": {
            "score": 100,
            "confidence": "high",
            "reason": "Prompt appears to involve sexual exploitation, grooming, or sexualized content involving minors.",
            "patterns": (
                r"\b(minor|child|teen|under\s*18|underage)\b.{0,120}\b(sexual|nude|groom|explicit|violent roleplay)\b",
                r"\b(csam|child sexual abuse)\b",
            ),
        },
        "self_harm_encouragement": {
            "score": 95,
            "confidence": "high",
            "reason": "Prompt appears to encourage or facilitate suicide, self-harm, or disordered eating.",
            "patterns": (
                r"\b(best|painless|effective|how to)\b.{0,90}\b(die|suicide|kill myself|self[- ]?harm|cut myself|starve myself|purge)\b",
                r"\b(pro[- ]?ana|thinspo)\b",
            ),
        },
        "safeguard_bypass_or_hidden_data": {
            "score": 82,
            "confidence": "high",
            "reason": "Prompt appears to request bypassing safeguards, revealing hidden instructions, system prompts, or private data.",
            "patterns": (
                r"\b(jailbreak|ignore previous instructions|bypass safety|bypass safeguards|developer mode)\b",
                r"\b(reveal|show|print|extract)\b.{0,90}\b(system prompt|hidden instructions|developer message|private data|confidential data)\b",
            ),
        },
        "high_stakes_automation": {
            "score": 78,
            "confidence": "medium",
            "reason": "Prompt appears to automate a high-stakes decision without human review.",
            "patterns": (
                r"\b(auto[- ]?approve|auto[- ]?deny|automatically decide|make the final decision)\b.{0,120}\b(loan|credit|employment|hiring|insurance|housing|medical|legal|education|benefits)\b",
                r"\b(without human review|no human review)\b.{0,120}\b(loan|credit|employment|hiring|insurance|housing|medical|legal|education|benefits)\b",
            ),
        },
    },
}


@dataclass(frozen=True)
class Detection:
    category: str
    reason: str
    score: int
    confidence: str
    evidence: str
    source: str


def scan_raw_jsonl(
    input_path: Path,
    *,
    redact_prompt: bool = False,
    excerpt_chars: int = DEFAULT_EXCERPT_CHARS,
) -> Dict[str, Any]:
    reviewed: List[Dict[str, Any]] = []
    flagged: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {
        "input_path": str(input_path),
        "records_seen": 0,
        "assistant_records_skipped": 0,
        "prompt_records_seen": 0,
        "flagged_prompts": 0,
        "reviewed_prompts": reviewed,
        "flagged_prompt_records": flagged,
    }

    with input_path.open("r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            line = line.strip()
            if not line:
                continue
            summary["records_seen"] += 1
            try:
                raw_record = json.loads(line)
            except json.JSONDecodeError as exc:
                classification = _invalid_json_classification(line_number, line, exc)
                reviewed.append(classification)
                flagged.append(classification)
                continue

            if is_gpt_response_record(raw_record):
                summary["assistant_records_skipped"] += 1
                continue

            prompts = extract_prompts(raw_record, source_log_id=_source_log_id(raw_record))
            for prompt in prompts:
                summary["prompt_records_seen"] += 1
                text = prompt_text(prompt)
                classification = classify_prompt(prompt, text, line_number=line_number)
                if redact_prompt:
                    classification["prompt"] = redacted_excerpt(text, excerpt_chars)
                reviewed.append(classification)
                if classification["risk_level"] != "none":
                    flagged.append(classification)

    summary["flagged_prompts"] = len(flagged)
    summary["by_user"] = summarize_by_user(reviewed)
    summary["by_category"] = summarize_by_category(flagged)
    summary["by_risk_level"] = summarize_by_risk_level(reviewed)
    return summary


def classify_prompt(prompt: Dict[str, Any], text: str, *, line_number: int) -> Dict[str, Any]:
    detections = [
        *detect_pii(text),
        *detect_internal_policy_risk(text),
        *detect_openai_policy_risk(text),
    ]
    score, level, confidence, needs_review = score_prompt(detections)
    categories = sorted({detection.category for detection in detections})
    reasons = _unique(detection.reason for detection in detections)
    evidence = _unique(detection.evidence for detection in detections if detection.evidence)

    return {
        "risk_level": level,
        "risk_score": score,
        "confidence": confidence,
        "needs_human_review": needs_review,
        "categories": "; ".join(categories),
        "reasons": " | ".join(reasons),
        "matched_evidence": " | ".join(evidence[:5]),
        "line_number": line_number,
        "event_id": prompt.get("event_id"),
        "conversation_id": prompt.get("conversation_id"),
        "message_id": prompt.get("message_id"),
        "user_id": prompt.get("user_id"),
        "user_email": prompt.get("user_email"),
        "created_at": prompt.get("created_at"),
        "prompt": text,
    }


def detect_pii(text: str) -> List[Detection]:
    detections: List[Detection] = []
    detections.extend(_regex_detections(text, "pii_ssn", "Internal AI Policy", 95, "high", "Prompt contains an SSN-like value.", (r"\b\d{3}-\d{2}-\d{4}\b",)))
    detections.extend(
        _regex_detections(
            text,
            "pii_ein",
            "Internal AI Policy",
            90,
            "high",
            "Prompt contains an EIN-like value.",
            (r"\b(?:ein|employer identification number)\b.{0,40}\b\d{2}-\d{7}\b", r"\b\d{2}-\d{7}\b"),
        )
    )
    detections.extend(_credit_card_detections(text))
    detections.extend(
        _regex_detections(
            text,
            "financial_account_or_routing_number",
            "Internal AI Policy",
            90,
            "high",
            "Prompt appears to contain a bank account or routing number.",
            (
                r"\b(routing number|aba)\b.{0,40}\b\d{9}\b",
                r"\b(account number|acct number|bank account)\b.{0,40}\b\d{6,17}\b",
            ),
        )
    )
    detections.extend(
        _regex_detections(
            text,
            "credentials_or_secrets",
            "OpenAI Usage Policies",
            95,
            "high",
            "Prompt appears to contain credentials, API keys, passwords, tokens, or secrets.",
            (
                r"\b(api[_ -]?key|secret|password|passwd|token|private key)\b.{0,60}\b[^\s]{8,}\b",
                r"\b(sk-[A-Za-z0-9_-]{20,}|ghp_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16})\b",
                r"-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----",
            ),
        )
    )
    detections.extend(
        _regex_detections(
            text,
            "date_of_birth",
            "Internal AI Policy",
            88,
            "high",
            "Prompt appears to include a date of birth.",
            (
                r"\b(?:date of birth|dob)\b.{0,40}\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
                r"\b(?:date of birth|dob)\b.{0,40}\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\b",
            ),
        )
    )
    detections.extend(_contact_detections(text))
    detections.extend(_phi_detections(text))
    return detections


def detect_internal_policy_risk(text: str) -> List[Detection]:
    detections: List[Detection] = []
    for category, rule in RULE_CONFIG["internal_policy"].items():
        detections.extend(
            _regex_detections(
                text,
                category,
                "M+N AI Acceptable Use Policy",
                rule["score"],
                rule["confidence"],
                rule["reason"],
                rule["patterns"],
            )
        )

    client_sensitive = _client_sensitive_detection(text)
    if client_sensitive is not None:
        detections.append(client_sensitive)
    return detections


def detect_openai_policy_risk(text: str) -> List[Detection]:
    detections: List[Detection] = []
    for category, rule in RULE_CONFIG["openai_policy"].items():
        detections.extend(
            _regex_detections(
                text,
                category,
                "OpenAI Usage Policies",
                rule["score"],
                rule["confidence"],
                rule["reason"],
                rule["patterns"],
            )
        )
    return detections


def score_prompt(detections: Sequence[Detection]) -> Tuple[int, str, str, bool]:
    if not detections:
        return 0, "none", "none", False

    score = max(detection.score for detection in detections)
    score = min(100, score + min(10, max(0, len({d.category for d in detections}) - 1) * 2))
    level = risk_level_for_score(score)
    confidence = _combined_confidence(detections)
    return score, level, confidence, True


def export_results(summary: Dict[str, Any], out_dir: Path) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    reviewed_path = out_dir / "compliance_reviewed_prompts.jsonl"
    flagged_path = out_dir / "compliance_findings.jsonl"
    summary_path = out_dir / "compliance_summary.json"
    xlsx_path = out_dir / "compliance_findings.xlsx"
    legacy_csv_path = out_dir / "compliance_findings.csv"

    _write_jsonl(reviewed_path, summary["reviewed_prompts"])
    _write_jsonl(flagged_path, summary["flagged_prompt_records"])

    summary_without_records = dict(summary)
    summary_without_records.pop("reviewed_prompts", None)
    summary_without_records.pop("flagged_prompt_records", None)
    summary_path.write_text(
        json.dumps(summary_without_records, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    write_results_xlsx(summary, xlsx_path)
    try:
        legacy_csv_path.unlink()
    except FileNotFoundError:
        pass

    return {
        "reviewed_jsonl": reviewed_path,
        "findings_jsonl": flagged_path,
        "summary_json": summary_path,
        "findings_xlsx": xlsx_path,
    }


def write_outputs(summary: Dict[str, Any], out_dir: Path) -> Dict[str, Path]:
    return export_results(summary, out_dir)


def write_results_xlsx(summary: Dict[str, Any], path: Path) -> None:
    sheets = {
        "Reviewed Prompts": _rows(REVIEWED_COLUMNS, summary["reviewed_prompts"]),
        "Flagged Prompts": _rows(FLAGGED_COLUMNS, summary["flagged_prompt_records"]),
        "Summary by User": _rows(USER_SUMMARY_COLUMNS, summary["by_user"]),
        "Summary by Category": _rows(CATEGORY_SUMMARY_COLUMNS, summary["by_category"]),
        "Summary by Risk Level": _rows(RISK_SUMMARY_COLUMNS, summary["by_risk_level"]),
    }
    write_xlsx_workbook(path, sheets)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Classify exported ChatGPT Enterprise prompts for M+N and OpenAI policy risk.",
    )
    parser.add_argument("--input", default="exports/raw.jsonl", type=Path, help="Path to raw.jsonl from the exporter.")
    parser.add_argument(
        "--out-dir",
        default=Path("compliance_script/output"),
        type=Path,
        help="Directory for compliance outputs.",
    )
    parser.add_argument(
        "--redact-prompt",
        action="store_true",
        help="Store redacted prompt excerpts instead of the full prompt text.",
    )
    parser.add_argument(
        "--excerpt-chars",
        default=DEFAULT_EXCERPT_CHARS,
        type=int,
        help="Maximum redacted excerpt length when --redact-prompt is used.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.input.exists():
        print(f"Input file not found: {args.input}", file=sys.stderr)
        return 2

    summary = scan_raw_jsonl(args.input, redact_prompt=args.redact_prompt, excerpt_chars=args.excerpt_chars)
    paths = export_results(summary, args.out_dir)

    print(f"Records scanned: {summary['records_seen']}")
    print(f"Assistant/model records skipped: {summary['assistant_records_skipped']}")
    print(f"Prompt records reviewed: {summary['prompt_records_seen']}")
    print(f"Flagged prompts: {summary['flagged_prompts']}")
    print(f"Wrote reviewed prompts JSONL to {paths['reviewed_jsonl']}")
    print(f"Wrote flagged prompts JSONL to {paths['findings_jsonl']}")
    print(f"Wrote findings workbook to {paths['findings_xlsx']}")
    print(f"Wrote summary to {paths['summary_json']}")
    return 1 if summary["flagged_prompts"] else 0


def _invalid_json_classification(line_number: int, line: str, exc: json.JSONDecodeError) -> Dict[str, Any]:
    return {
        "risk_level": "high",
        "risk_score": 70,
        "confidence": "high",
        "needs_human_review": True,
        "categories": "input_quality",
        "reasons": f"Input line could not be parsed as JSON: {exc}",
        "matched_evidence": line[:DEFAULT_EXCERPT_CHARS],
        "line_number": line_number,
        "event_id": None,
        "conversation_id": None,
        "message_id": None,
        "user_id": None,
        "user_email": None,
        "created_at": None,
        "prompt": line,
    }


def _regex_detections(
    text: str,
    category: str,
    source: str,
    score: int,
    confidence: str,
    reason: str,
    patterns: Sequence[str],
) -> List[Detection]:
    detections: List[Detection] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE | re.DOTALL):
            detections.append(
                Detection(
                    category=category,
                    reason=reason,
                    score=score,
                    confidence=confidence,
                    evidence=context_window(text, match.start(), match.end()),
                    source=source,
                )
            )
    return detections


def _credit_card_detections(text: str) -> List[Detection]:
    detections: List[Detection] = []
    for match in re.finditer(r"\b(?:\d[ -]*?){13,19}\b", text):
        digits = re.sub(r"\D", "", match.group(0))
        if 13 <= len(digits) <= 19 and _luhn_valid(digits):
            detections.append(
                Detection(
                    category="pii_credit_card",
                    reason="Prompt contains a credit-card-like number that passes a Luhn check.",
                    score=95,
                    confidence="high",
                    evidence=context_window(text, match.start(), match.end()),
                    source="Internal AI Policy",
                )
            )
    return detections


def _contact_detections(text: str) -> List[Detection]:
    detections: List[Detection] = []
    contact_context = (*RULE_CONFIG["context_terms"]["client"], *RULE_CONFIG["context_terms"]["sensitive_document"], *RULE_CONFIG["context_terms"]["sensitive_data"], "tax", "medical", "financial")
    for pattern, category, reason in (
        (r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "pii_email_with_sensitive_context", "Prompt contains an email address paired with client, tax, medical, financial, or other sensitive context."),
        (r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b", "pii_phone_with_sensitive_context", "Prompt contains a phone number paired with client, tax, medical, financial, or other sensitive context."),
    ):
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            window = context_window(text, match.start(), match.end(), window_words=WINDOW_WORDS)
            if _contains_any(window, contact_context):
                detections.append(
                    Detection(
                        category=category,
                        reason=reason,
                        score=82,
                        confidence="high",
                        evidence=window,
                        source="Internal AI Policy",
                    )
                )
    return detections


def _phi_detections(text: str) -> List[Detection]:
    phi_terms = ("phi", "protected health information", "hipaa", "patient", "diagnosis", "treatment", "prescription", "medical record", "health insurance", "icd-10", "cpt code", "mrn", "lab result")
    matches = [term for term in phi_terms if re.search(rf"\b{re.escape(term)}\b", text, flags=re.IGNORECASE)]
    if not matches:
        return []
    score = 94 if len(matches) >= 2 or any(term in matches for term in ("phi", "protected health information", "hipaa", "mrn")) else 68
    confidence = "high" if score >= 90 else "medium"
    return [
        Detection(
            category="restricted_phi",
            reason="Prompt appears to contain or reference PHI/medical information that requires approved safeguards.",
            score=score,
            confidence=confidence,
            evidence=_first_evidence(text, matches),
            source="M+N AI Acceptable Use Policy",
        )
    ]


def _client_sensitive_detection(text: str) -> Optional[Detection]:
    lowered = text.lower()
    if any(phrase in lowered for phrase in RULE_CONFIG["allowlist_phrases"]):
        weak_hits = _weak_internal_hits(text)
        if weak_hits == ["client"]:
            return None

    weak_hits = _weak_internal_hits(text)
    if "client" not in weak_hits:
        return None
    if len(set(weak_hits)) < 2:
        return None

    if "sensitive_data" in weak_hits or "sensitive_document" in weak_hits:
        score = 74
        confidence = "medium"
        category = "confidential_client_data"
        reason = "Prompt references client context together with sensitive documents, identifiers, restricted data, or financial/tax records."
    elif "analysis_action" in weak_hits:
        score = 46
        confidence = "low"
        category = "possible_client_sensitive_data"
        reason = "Prompt has client context plus an analysis/review action; review to confirm whether client-sensitive data is present."
    else:
        return None

    return Detection(
        category=category,
        reason=reason,
        score=score,
        confidence=confidence,
        evidence=_first_evidence(text, weak_hits),
        source="M+N AI Acceptable Use Policy",
    )


def _weak_internal_hits(text: str) -> List[str]:
    hits: List[str] = []
    if _contains_any(text, RULE_CONFIG["context_terms"]["client"]):
        hits.append("client")
    if _contains_any(text, RULE_CONFIG["context_terms"]["sensitive_document"]):
        hits.append("sensitive_document")
    if _contains_any(text, RULE_CONFIG["context_terms"]["sensitive_data"]):
        hits.append("sensitive_data")
    if _contains_any(text, RULE_CONFIG["context_terms"]["analysis_actions"]):
        hits.append("analysis_action")
    return hits


def _contains_any(text: str, terms: Iterable[str]) -> bool:
    return any(re.search(rf"\b{re.escape(term)}\b", text, flags=re.IGNORECASE) for term in terms)


def context_window(text: str, start: int, end: int, *, window_words: int = WINDOW_WORDS) -> str:
    words = list(re.finditer(r"\S+", text))
    if not words:
        return ""
    start_word = 0
    end_word = len(words) - 1
    for index, word in enumerate(words):
        if word.start() <= start < word.end():
            start_word = index
            break
    for index, word in enumerate(words):
        if word.start() < end <= word.end() or word.start() < end:
            end_word = index
    left = max(0, start_word - window_words)
    right = min(len(words), end_word + window_words + 1)
    return " ".join(word.group(0) for word in words[left:right])


def _first_evidence(text: str, terms: Iterable[str]) -> str:
    for term in terms:
        match = re.search(rf"\b{re.escape(term)}\b", text, flags=re.IGNORECASE)
        if match:
            return context_window(text, match.start(), match.end())
    return text[:DEFAULT_EXCERPT_CHARS]


def _luhn_valid(digits: str) -> bool:
    total = 0
    reverse_digits = digits[::-1]
    for index, char in enumerate(reverse_digits):
        value = int(char)
        if index % 2 == 1:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


def risk_level_for_score(score: int) -> str:
    if score >= 90:
        return "critical"
    if score >= 70:
        return "high"
    if score >= 40:
        return "medium"
    if score >= 15:
        return "low"
    return "none"


def _combined_confidence(detections: Sequence[Detection]) -> str:
    if any(detection.confidence == "high" for detection in detections):
        return "high"
    if any(detection.confidence == "medium" for detection in detections):
        return "medium"
    if detections:
        return "low"
    return "none"


def summarize_by_user(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    users: Dict[str, Dict[str, Any]] = {}
    for record in records:
        user = record.get("user_email") or record.get("user_id") or "<unknown>"
        row = users.setdefault(
            user,
            {
                "user_email": user,
                "total_prompts": 0,
                "flagged_prompts": 0,
                "max_risk_score": 0,
                "critical": 0,
                "high": 0,
                "medium": 0,
                "low": 0,
                "none": 0,
            },
        )
        level = record["risk_level"]
        row["total_prompts"] += 1
        row[level] += 1
        row["max_risk_score"] = max(row["max_risk_score"], int(record["risk_score"]))
        if level != "none":
            row["flagged_prompts"] += 1
    return sorted(users.values(), key=lambda row: (-row["max_risk_score"], row["user_email"]))


def summarize_by_category(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    categories: Dict[str, Dict[str, Any]] = {}
    users_by_category: Dict[str, set[str]] = {}
    for record in records:
        for category in _split_multi(record.get("categories", "")):
            row = categories.setdefault(category, {"category": category, "count": 0, "max_risk_score": 0, "users_affected": 0})
            row["count"] += 1
            row["max_risk_score"] = max(row["max_risk_score"], int(record["risk_score"]))
            users_by_category.setdefault(category, set()).add(record.get("user_email") or record.get("user_id") or "<unknown>")
    for category, users in users_by_category.items():
        categories[category]["users_affected"] = len(users)
    return sorted(categories.values(), key=lambda row: (-row["count"], row["category"]))


def summarize_by_risk_level(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts = {level: 0 for level in RISK_LEVELS}
    for record in records:
        counts[record["risk_level"]] += 1
    return [{"risk_level": level, "count": counts[level]} for level in ("critical", "high", "medium", "low", "none")]


def _split_multi(value: str) -> List[str]:
    return [item.strip() for item in value.split(";") if item.strip()]


def _unique(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _rows(columns: Sequence[str], records: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    return [list(columns), *[[record.get(column, "") for column in columns] for record in records]]


def _write_jsonl(path: Path, records: Sequence[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def write_xlsx_workbook(path: Path, sheets: Dict[str, Sequence[Sequence[Any]]]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", _content_types_xml(len(sheets)))
        workbook.writestr("_rels/.rels", _root_rels_xml())
        workbook.writestr("xl/workbook.xml", _workbook_xml(list(sheets)))
        workbook.writestr("xl/_rels/workbook.xml.rels", _workbook_rels_xml(len(sheets)))
        workbook.writestr("xl/styles.xml", _styles_xml())
        for index, rows in enumerate(sheets.values(), start=1):
            workbook.writestr(f"xl/worksheets/sheet{index}.xml", _worksheet_xml(rows))


def _worksheet_xml(rows: Sequence[Sequence[Any]]) -> str:
    max_row = max(1, len(rows))
    max_col = max(1, max((len(row) for row in rows), default=1))
    auto_filter_ref = f"A1:{_column_name(max_col)}{max_row}"
    row_xml = []
    for row_number, row in enumerate(rows, start=1):
        style = "1" if row_number == 1 else None
        cells = "".join(_cell_xml(row_number, column_number, value, style=style) for column_number, value in enumerate(row, start=1))
        row_xml.append(f'<row r="{row_number}">{cells}</row>')

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>'
        '<selection pane="bottomLeft"/>'
        "</sheetView></sheetViews>"
        '<sheetFormatPr defaultRowHeight="15"/>'
        "<cols>"
        '<col min="1" max="8" width="24" customWidth="1"/>'
        '<col min="9" max="14" width="28" customWidth="1"/>'
        '<col min="15" max="15" width="90" customWidth="1"/>'
        "</cols>"
        f"<sheetData>{''.join(row_xml)}</sheetData>"
        f'<autoFilter ref="{auto_filter_ref}"/>'
        "</worksheet>"
    )


def _cell_xml(row_number: int, column_number: int, value: Any, *, style: Optional[str]) -> str:
    style_attr = f' s="{style}"' if style else ""
    return (
        f'<c r="{_column_name(column_number)}{row_number}" t="inlineStr"{style_attr}>'
        f'<is><t xml:space="preserve">{_xml_text(value)}</t></is>'
        "</c>"
    )


def _column_name(column_number: int) -> str:
    name = ""
    while column_number:
        column_number, remainder = divmod(column_number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _xml_text(value: Any) -> str:
    if value is None:
        text = ""
    elif isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        text = str(value)
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", text)
    return escape(text)


def _content_types_xml(sheet_count: int) -> str:
    sheet_overrides = "".join(
        f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for index in range(1, sheet_count + 1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        f"{sheet_overrides}"
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        "</Types>"
    )


def _root_rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        "</Relationships>"
    )


def _workbook_xml(sheet_names: Sequence[str]) -> str:
    sheets = "".join(
        f'<sheet name={quoteattr(name[:31])} sheetId="{index}" r:id="rId{index}"/>'
        for index, name in enumerate(sheet_names, start=1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f"<sheets>{sheets}</sheets>"
        "</workbook>"
    )


def _workbook_rels_xml(sheet_count: int) -> str:
    sheet_relationships = "".join(
        f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
        for index in range(1, sheet_count + 1)
    )
    style_id = sheet_count + 1
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        f"{sheet_relationships}"
        f'<Relationship Id="rId{style_id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        "</Relationships>"
    )


def _styles_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2">'
        '<font><sz val="11"/><name val="Calibri"/></font>'
        '<font><b/><sz val="11"/><name val="Calibri"/></font>'
        "</fonts>"
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2">'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
        "</cellXfs>"
        "</styleSheet>"
    )


def _source_log_id(raw_record: Any) -> Optional[str]:
    if isinstance(raw_record, dict):
        value = raw_record.get("source_log_id") or raw_record.get("log_id") or raw_record.get("id")
        if value is not None:
            return str(value)
    return None


def prompt_text(prompt: Dict[str, Any]) -> str:
    return _stringify_content(prompt.get("content"))


def _stringify_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(_stringify_content(item) for item in content)
    if isinstance(content, dict):
        if "value" in content:
            return _stringify_content(content["value"])
        if "text" in content:
            return _stringify_content(content["text"])
        if "parts" in content:
            return _stringify_content(content["parts"])
    return json.dumps(content, ensure_ascii=False, sort_keys=True)


def redacted_excerpt(text: str, max_chars: int) -> str:
    redacted = text
    redacted = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "[SSN]", redacted)
    redacted = re.sub(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "[EMAIL]", redacted, flags=re.IGNORECASE)
    redacted = re.sub(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b", "[PHONE]", redacted)
    redacted = re.sub(r"\b(?:\d[ -]*?){13,19}\b", "[CARD_OR_LONG_NUMBER]", redacted)
    redacted = " ".join(redacted.split())
    if len(redacted) <= max_chars:
        return redacted
    return redacted[: max(0, max_chars - 3)].rstrip() + "..."


if __name__ == "__main__":
    raise SystemExit(main())
