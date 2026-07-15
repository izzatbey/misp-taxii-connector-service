"""
pulse_decay.py — Recency / decay filter for OTX pulses (the "decaying system").

A pulse is considered ELIGIBLE for processing (fetching its indicators and
pushing to TAXII) when it is still "fresh" according to a sliding window:

    A pulse is fresh if EITHER its `created` OR its `modified` timestamp
    falls within the last `PULSE_MAX_AGE_DAYS` days (default 90).

Rationale (a decaying feed):
    - A brand-new pulse is always processed.
    - An old pulse that is still being updated (modified recently) is still
      relevant and is processed.
    - A pulse that was created long ago AND hasn't been touched recently is
      stale threat intel — its indicators are likely already covered by
      newer pulses, so we stop re-fetching and re-pushing it. This keeps the
      OTX -> TAXII volume bounded and the TAXII collection from growing
      forever.

Examples (today = 2026-07-13, window = 90d, cutoff = 2026-04-14):
    Pulse A: created 2026-07-10 | modified 1h ago        -> ELIGIBLE (created fresh)
    Pulse B: created 2026-01-10 | modified 3 days ago    -> ELIGIBLE (modified fresh)
    Pulse C: created 2024-09-09 | modified 2024-09-09    -> SKIP     (both stale)

The decision is intentionally permissive: if a pulse is missing BOTH
timestamps (or they cannot be parsed), it is treated as ELIGIBLE so we
never silently drop data due to malformed metadata. Set
PULSE_DECAY_STRICT_MISSING=true to instead skip pulses with no usable dates.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def _parse_otx_timestamp(value) -> Optional[datetime]:
    """
    Parse an OTX pulse timestamp string into a timezone-aware UTC datetime.

    OTX timestamps can arrive as:
        - "2026-07-10T13:45:23.000"    (naive -> treat as UTC)
        - "2026-07-10T13:45:23.000Z"   (Zulu)
        - "2026-07-10T13:45:23+00:00"  (explicit offset)
        - None / empty / non-string

    Returns None if the value cannot be parsed.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        # datetime.fromisoformat in 3.11+ accepts 'Z', but 3.10 and earlier
        # do not — normalise it to an explicit UTC offset first.
        normalised = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalised)
    except (ValueError, TypeError):
        logger.debug(f"Could not parse OTX timestamp '{value}'; ignoring.")
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_pulse_eligible(
    pulse: dict,
    max_age_days: int = 90,
    now: Optional[datetime] = None,
    strict_missing: bool = False,
) -> tuple[bool, str]:
    """
    Decide whether a pulse is fresh enough to process.

    Args:
        pulse: OTX pulse dict (must contain 'created' and/or 'modified').
        max_age_days: Sliding window length in days. A pulse is fresh if its
            created OR modified timestamp is within this window of `now`.
        now: Reference "current" time (UTC). Defaults to datetime.now(UTC).
        strict_missing: If True, a pulse with no parseable timestamps is
            considered STALE (skipped). If False (default), it is considered
            fresh (processed) to avoid silent data loss.

    Returns:
        (eligible, reason) where reason is a short human-readable string
        suitable for logging, e.g. "modified within 90d window (2026-07-10)".
    """
    if now is None:
        now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max_age_days)

    created_dt = _parse_otx_timestamp(pulse.get("created"))
    modified_dt = _parse_otx_timestamp(pulse.get("modified"))

    # Rule 1: created recently -> fresh.
    if created_dt is not None and created_dt >= cutoff:
        return True, f"created within {max_age_days}d window ({created_dt.date()})"

    # Rule 2: modified recently -> fresh (even if created long ago).
    if modified_dt is not None and modified_dt >= cutoff:
        return True, f"modified within {max_age_days}d window ({modified_dt.date()})"

    # No fresh timestamp found.
    if created_dt is None and modified_dt is None:
        if strict_missing:
            return False, "no parseable created/modified timestamps (strict mode)"
        return True, "no timestamps available; allowed (non-strict default)"

    # We have timestamps but BOTH are stale -> decay.
    newest = max(d for d in (created_dt, modified_dt) if d is not None)
    return (
        False,
        f"stale - newest of created/modified is {newest.date()} "
        f"(cutoff {cutoff.date()})",
    )


class PulseDecayFilter:
    """
    Stateful decay filter bound to config, with counters for stats logging.

    Usage:
        decay = PulseDecayFilter(max_age_days=config.PULSE_MAX_AGE_DAYS,
                                 enabled=config.PULSE_DECAY_ENABLED)
        for pulse in pulses:
            if decay.should_process(pulse):
                ...  # fetch indicators, build bundle, push
        logging.info(decay.summary())

    The counters (seen/passed/skipped) are NOT thread-safe; each cycle should
    own its own instance and call should_process() from the submitter thread
    (NOT from inside worker threads).
    """

    def __init__(
        self,
        max_age_days: int = 90,
        enabled: bool = True,
        strict_missing: bool = False,
        now: Optional[datetime] = None,
    ):
        self.max_age_days = max(0, int(max_age_days))
        self.enabled = enabled
        self.strict_missing = strict_missing
        self._now = now  # fixed reference for deterministic tests; None = live
        self.seen = 0
        self.passed = 0
        self.skipped = 0

    @classmethod
    def from_config(cls, config) -> "PulseDecayFilter":
        """
        Build a filter from the app Config object.

        Reads PULSE_MAX_AGE_DAYS, PULSE_DECAY_ENABLED, and
        PULSE_DECAY_STRICT_MISSING from the Config instance (which itself
        loads from env).
        """
        return cls(
            max_age_days=getattr(config, "PULSE_MAX_AGE_DAYS", 90),
            enabled=getattr(config, "PULSE_DECAY_ENABLED", True),
            strict_missing=getattr(config, "PULSE_DECAY_STRICT_MISSING", False),
        )

    def should_process(self, pulse: dict) -> bool:
        """
        Return True if the pulse should be processed (fresh / disabled).

        When the filter is disabled or max_age_days <= 0, every pulse passes
        through unchanged (the decaying system is a no-op). This makes it
        safe to ship the filter always-on and toggle purely via env.
        """
        self.seen += 1
        if not self.enabled or self.max_age_days <= 0:
            self.passed += 1
            return True

        eligible, reason = is_pulse_eligible(
            pulse,
            max_age_days=self.max_age_days,
            now=self._now,
            strict_missing=self.strict_missing,
        )
        if eligible:
            self.passed += 1
            return True

        self.skipped += 1
        pulse_id = pulse.get("id", "?")
        pulse_name = pulse.get("name", "?")
        logger.info(
            f"[decay] Skipping stale pulse '{pulse_name}' (ID: {pulse_id}): {reason}"
        )
        return False

    def summary(self) -> str:
        """One-line stats string for end-of-cycle logging."""
        return (
            f"decay filter: seen={self.seen}, passed={self.passed}, "
            f"skipped={self.skipped} (window={self.max_age_days}d, "
            f"enabled={self.enabled})"
        )
