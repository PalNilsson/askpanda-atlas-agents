# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""
askpanda_atlas_agents/agents/base.py

A minimal, consistent lifecycle interface for AskPanDA-ATLAS Agents.

Design goals:
- Simple: start/tick/health/stop
- Testable: deterministic tick() units of work
- Supervisor-friendly: uniform health reporting and clean shutdown
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional


class AgentState(str, Enum):
    """Lifecycle state of an agent."""

    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True)
class HealthReport:
    """Structured health information returned by Agent.health()."""

    name: str
    state: AgentState
    ok: bool
    last_tick_utc: Optional[datetime] = None
    last_success_utc: Optional[datetime] = None
    last_error_utc: Optional[datetime] = None
    error: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert the health report to a JSON-serializable dictionary."""
        def _dt(v: Optional[datetime]) -> Optional[str]:
            return v.astimezone(timezone.utc).isoformat() if v else None

        return {
            "name": self.name,
            "state": self.state.value,
            "ok": self.ok,
            "last_tick_utc": _dt(self.last_tick_utc),
            "last_success_utc": _dt(self.last_success_utc),
            "last_error_utc": _dt(self.last_error_utc),
            "error": self.error,
            "details": dict(self.details),
        }


class Agent(ABC):
    """Base class for all AskPanDA-ATLAS agents.

    Agents implement a minimal lifecycle:
      - start(): initialize resources
      - tick(): perform one unit of scheduled work
      - health(): report current health status
      - stop(): release resources

    Notes:
    - `tick()` should be idempotent where practical, and safe to retry.
    - Exceptions from `tick()` should generally be allowed to propagate to
      the supervisor, which can apply restart/backoff policies. This base
      class provides helpers to record success/failure timestamps.
    - `health()` MUST be safe to call at any time, even if the agent is failed.
    """

    def __init__(self, name: str) -> None:
        """Initialize an Agent.

        Args:
            name: Human-friendly unique agent name (e.g., "metadata-agent").
        """
        self._name = name
        self._state: AgentState = AgentState.NEW
        self._last_tick_utc: Optional[datetime] = None
        self._last_success_utc: Optional[datetime] = None
        self._last_error_utc: Optional[datetime] = None
        self._last_error: Optional[str] = None

    @property
    def name(self) -> str:
        """Return the agent name."""
        return self._name

    @property
    def state(self) -> AgentState:
        """Return the current agent state."""
        return self._state

    def start(self) -> None:
        """Initialize resources and enter running state.

        Subclasses should implement `_start_impl()` to do real work.
        """
        if self._state in (AgentState.RUNNING, AgentState.STARTING):
            return

        self._state = AgentState.STARTING
        try:
            self._start_impl()
            self._state = AgentState.RUNNING
            self._last_error = None
        except Exception as exc:
            self._mark_failed(exc)
            raise

    def tick(self) -> None:
        """Execute one scheduled unit of work.

        Subclasses should implement `_tick_impl()` to do real work.
        """
        if self._state != AgentState.RUNNING:
            raise RuntimeError(
                f"Agent '{self._name}' is not running (state={self._state.value}); "
                "cannot tick()."
            )

        self._last_tick_utc = datetime.now(timezone.utc)
        try:
            self._tick_impl()
            self._last_success_utc = datetime.now(timezone.utc)
            self._last_error = None
        except Exception as exc:
            self._mark_failed(exc)
            raise

    def stop(self) -> None:
        """Gracefully release resources and shut down.

        Subclasses should implement `_stop_impl()` to do real work.
        """
        if self._state in (AgentState.STOPPING, AgentState.STOPPED):
            return

        self._state = AgentState.STOPPING
        try:
            self._stop_impl()
            self._state = AgentState.STOPPED
        except Exception as exc:
            # Stopping errors should be visible but shouldn't prevent a stop state.
            self._last_error_utc = datetime.now(timezone.utc)
            self._last_error = f"stop() failed: {exc!r}"
            self._state = AgentState.STOPPED
            raise

    def health(self) -> HealthReport:
        """Return a structured health report for the supervisor/UI."""
        ok = self._state in (AgentState.RUNNING, AgentState.STOPPED)
        if self._state == AgentState.FAILED:
            ok = False

        return HealthReport(
            name=self._name,
            state=self._state,
            ok=ok,
            last_tick_utc=self._last_tick_utc,
            last_success_utc=self._last_success_utc,
            last_error_utc=self._last_error_utc,
            error=self._last_error,
            details=self._health_details(),
        )

    # ---- Hooks for subclasses -------------------------------------------------

    @abstractmethod
    def _start_impl(self) -> None:
        """Subclass hook: initialize resources (connections, files, etc.)."""

    @abstractmethod
    def _tick_impl(self) -> None:
        """Subclass hook: do one unit of work."""

    @abstractmethod
    def _stop_impl(self) -> None:
        """Subclass hook: release resources cleanly."""

    def _health_details(self) -> Mapping[str, Any]:
        """Subclass hook: return additional health details (optional)."""
        return {}

    # ---- Internal helpers -----------------------------------------------------

    def _mark_failed(self, exc: Exception) -> None:
        """Record failure metadata and set FAILED state."""
        self._state = AgentState.FAILED
        self._last_error_utc = datetime.now(timezone.utc)
        self._last_error = f"{type(exc).__name__}: {exc}"
