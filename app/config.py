from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    database_path: Path
    log_file_path: Path
    report_output_dir: Path
    openai_rules_path: Path
    company_rules_path: Path
    enterprise_base_url: str | None
    enterprise_api_key: str | None
    enterprise_events_path: str
    enterprise_timeout_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_path=Path(os.getenv("REPORTING_DB_PATH", "data/compliance_reporting.db")),
            log_file_path=Path(os.getenv("REPORTING_LOG_PATH", "logs/app.log")),
            report_output_dir=Path(os.getenv("REPORT_OUTPUT_DIR", "reports")),
            openai_rules_path=Path(
                os.getenv("OPENAI_RULES_PATH", "data/policies/openai_policy_rules.json")
            ),
            company_rules_path=Path(
                os.getenv("COMPANY_RULES_PATH", "data/policies/company_policy_rules.json")
            ),
            enterprise_base_url=os.getenv("OPENAI_COMPLIANCE_BASE_URL"),
            enterprise_api_key=os.getenv("OPENAI_COMPLIANCE_API_KEY"),
            enterprise_events_path=os.getenv(
                "OPENAI_COMPLIANCE_EVENTS_PATH", "/v1/compliance/logs/conversations"
            ),
            enterprise_timeout_seconds=int(
                os.getenv("OPENAI_COMPLIANCE_TIMEOUT_SECONDS", "30")
            ),
        )
