from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

from app.models import RawComplianceRecord


class ComplianceSource(ABC):
    """Abstract source for ChatGPT Enterprise compliance records."""

    @abstractmethod
    def fetch_records(
        self,
        start_date: date,
        end_date: date,
        input_file: Path | None = None,
    ) -> list[RawComplianceRecord]:
        raise NotImplementedError
