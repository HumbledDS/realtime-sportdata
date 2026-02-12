"""
Schema Validator — Validates events against JSON Schema before pipeline entry.

Rejects malformed events at the edge to prevent pollution downstream.
Uses JSON Schema Draft 2020-12.
"""

import json
import structlog
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import jsonschema
from jsonschema import Draft202012Validator

logger = structlog.get_logger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    event_type: Optional[str] = None


class SchemaValidator:
    """
    Validates events against their corresponding JSON schema.
    Schemas are loaded from disk and cached in memory.
    """

    def __init__(self, schema_dir: str = "config/schemas"):
        self.schema_dir = Path(schema_dir)
        self._schemas: dict[str, dict] = {}
        self._validators: dict[str, Draft202012Validator] = {}
        self._load_schemas()

    def _load_schemas(self):
        """Load all event schemas from the schema directory."""
        if not self.schema_dir.exists():
            logger.warning("schema_dir_not_found", path=str(self.schema_dir))
            return

        for schema_file in self.schema_dir.glob("*.json"):
            try:
                with open(schema_file) as f:
                    schema = json.load(f)

                # Map event type to schema
                title = schema.get("title", "")
                event_type = self._title_to_event_type(title)
                if event_type:
                    self._schemas[event_type] = schema
                    self._validators[event_type] = Draft202012Validator(schema)
                    logger.info(
                        "schema_loaded",
                        event_type=event_type,
                        file=schema_file.name,
                    )
            except Exception as e:
                logger.error(
                    "schema_load_error",
                    file=schema_file.name,
                    error=str(e),
                )

    def validate(self, event: dict) -> ValidationResult:
        """
        Validate an event against its corresponding schema.
        Returns ValidationResult with is_valid and any errors.
        """
        event_type = event.get("event_type")
        if not event_type:
            return ValidationResult(
                is_valid=False,
                errors=["Missing required field: event_type"],
                event_type=None,
            )

        validator = self._validators.get(event_type)
        if not validator:
            # No specific schema found — validate against base schema
            validator = self._validators.get("BASE")
            if not validator:
                # No schemas loaded — pass through with warning
                logger.warning(
                    "no_schema_for_event_type",
                    event_type=event_type,
                )
                return ValidationResult(is_valid=True, event_type=event_type)

        errors = []
        for error in validator.iter_errors(event):
            errors.append(f"{error.json_path}: {error.message}")

        if errors:
            logger.warning(
                "validation_failed",
                event_type=event_type,
                error_count=len(errors),
                first_error=errors[0],
            )

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            event_type=event_type,
        )

    def _title_to_event_type(self, title: str) -> Optional[str]:
        """Map schema title to event type constant."""
        mapping = {
            "GoalScored": "GOAL_SCORED",
            "GoalDisallowed": "GOAL_DISALLOWED",
            "PenaltyAwarded": "PENALTY_AWARDED",
            "YellowCard": "YELLOW_CARD",
            "RedCard": "RED_CARD",
            "Substitution": "SUBSTITUTION",
            "VARReview": "VAR_REVIEW_STARTED",
            "MatchState": "KICK_OFF",
            "BaseMatchEvent": "BASE",
        }
        return mapping.get(title)
