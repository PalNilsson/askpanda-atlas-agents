from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional

class AgentState(str, Enum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"

@dataclass(frozen=True)
class HealthReport:
    name: str
    state: AgentState
    ok: bool
    last_tick_utc: Optional[datetime] = None
    last_success_utc: Optional[datetime] = None
    last_error_utc: Optional[datetime] = None
    error: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
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
    def __init__(self, name: str) -> None:
        self._name = name
        self._state: AgentState = AgentState.NEW
        self._last_tick_utc: Optional[datetime] = None
        self._last_success_utc: Optional[datetime] = None
        self._last_error_utc: Optional[datetime] = None
        self._last_error: Optional[str] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> AgentState:
        return self._state

    def start(self) -> None:
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
        if self._state != AgentState.RUNNING:
            raise RuntimeError(f"Agent '{self._name}' is not running (state={self._state.value}); cannot tick().")
        self._last_tick_utc = datetime.now(timezone.utc)
        try:
            self._tick_impl()
            self._last_success_utc = datetime.now(timezone.utc)
            self._last_error = None
        except Exception as exc:
            self._mark_failed(exc)
            raise

    def stop(self) -> None:
        if self._state in (AgentState.STOPPING, AgentState.STOPPED):
            return
        self._state = AgentState.STOPPING
        try:
            self._stop_impl()
            self._state = AgentState.STOPPED
        except Exception as exc:
            self._last_error_utc = datetime.now(timezone.utc)
            self._last_error = f"stop() failed: {exc!r}"
            self._state = AgentState.STOPPED
            raise

    def health(self) -> HealthReport:
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

    @abstractmethod
    def _start_impl(self) -> None:
        pass

    @abstractmethod
    def _tick_impl(self) -> None:
        pass

    @abstractmethod
    def _stop_impl(self) -> None:
        pass

    def _health_details(self) -> Mapping[str, Any]:
        return {}

    def _mark_failed(self, exc: Exception) -> None:
        self._state = AgentState.FAILED
        self._last_error_utc = datetime.now(timezone.utc)
        self._last_error = f"{type(exc).__name__}: {exc}"
