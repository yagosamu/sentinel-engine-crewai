"""A scripted CrewAI LLM stub for offline (no-API-key) wiring tests.

WHY A SUBCLASS OF ``crewai.LLM`` (not a duck-typed fake): in CrewAI 0.100.0,
``crewai.utilities.llm_utils.create_llm`` returns the value as-is ONLY when it is
``isinstance(x, LLM)``; any other object is coerced into a real, token-calling
``LLM``. So the stub MUST subclass ``LLM`` and override ``call`` to return scripted
text. The crew then runs its real delegation / task / output_pydantic machinery
while the token-producing call is replaced — verifying WIRING, never claiming the
LLM diagnosed anything (HONESTY RULE / R5).
"""

from __future__ import annotations

from typing import Any

from crewai import LLM


class StubLLM(LLM):
    """An ``LLM`` whose ``call`` returns scripted replies instead of tokens.

    Pass either a single ``default`` string (returned for every call) or a list of
    ``replies`` consumed in order (the last is repeated once exhausted). The stub
    records every prompt it saw in :attr:`calls` for assertions.
    """

    def __init__(self, replies: list[str] | None = None, default: str = "ACK") -> None:
        super().__init__(model="stub/stub")
        self._replies = list(replies or [])
        self._default = default
        self.calls: list[Any] = []

    def call(
        self,
        messages: Any,
        tools: Any = None,
        callbacks: Any = None,
        available_functions: Any = None,
    ) -> str:
        self.calls.append(messages)
        if self._replies:
            return self._replies.pop(0)
        return self._default

    # The hierarchical executor consults these; keep them cheap and deterministic.
    def supports_function_calling(self) -> bool:
        return False

    def supports_stop_words(self) -> bool:
        return False

    def get_context_window_size(self) -> int:
        return 8192
