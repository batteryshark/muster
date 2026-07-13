"""Optional Pydantic AI durability helpers.

This module is intentionally outside muster's dependency-free core. Applications that
install ``muster[model]`` can use it to preserve provider-native message history, normalize
stream activity, and split a forced evidence-tool turn from a structured-output turn.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

try:
    from pydantic_ai import DeferredToolRequests, ModelMessagesTypeAdapter
    from pydantic_ai.capabilities import AbstractCapability
except ImportError as exc:  # pragma: no cover - exercised by installations without the extra
    raise ImportError(
        "muster.pydantic_runtime requires the optional model dependencies; "
        "install 'muster[model]'"
    ) from exc


@dataclass(frozen=True)
class ModelStreamEvent:
    """Provider-neutral, bounded activity suitable for a durable application ledger."""

    kind: str
    message: str
    payload: dict[str, Any] = field(default_factory=dict)


ModelEventSink = Callable[[ModelStreamEvent], None]


def dump_message_history(messages: list[Any]) -> str:
    """Serialize Pydantic model messages without losing thinking signatures/tool ids."""

    return ModelMessagesTypeAdapter.dump_json(messages).decode("utf-8")


def load_message_history(value: str) -> list[Any]:
    """Restore message history produced by :func:`dump_message_history`."""

    return ModelMessagesTypeAdapter.validate_json(value)


def anthropic_cache_settings() -> dict[str, Any]:
    """Settings for explicit caching on Anthropic-compatible continuation turns."""

    return {
        "anthropic_cache_messages": True,
        "anthropic_cache_instructions": True,
        "anthropic_cache_tool_definitions": True,
    }


def acquisition_output_types() -> list[Any]:
    """Output contract for a tool-acquisition turn, kept free of report-schema prompts."""

    return [str, DeferredToolRequests]


@dataclass
class RequiredToolTurn(AbstractCapability):
    """Require a function tool for one agent run, optionally suppressing cache breakpoints.

    Attach this capability only to the acquisition run. Start the structured-output run
    separately with the preserved message history and deferred tool results.

    ``disable_anthropic_cache`` exists for Anthropic-compatible gateways where forced tool
    choice and explicit ``cache_control`` blocks interact badly. Normal/continuation turns
    can retain :func:`anthropic_cache_settings`.
    """

    disable_anthropic_cache: bool = False
    event_sink: ModelEventSink | None = None

    def get_model_settings(self):
        def settings(_ctx) -> dict[str, Any]:
            value: dict[str, Any] = {"tool_choice": "required"}
            if self.disable_anthropic_cache:
                value.update({
                    "anthropic_cache_messages": False,
                    "anthropic_cache_instructions": False,
                    "anthropic_cache_tool_definitions": False,
                })
            return value

        # A callable is required: a static required-tool choice would prevent a later
        # agent graph step from producing final output.
        return settings

    async def before_model_request(self, _ctx, request_context):
        if self.event_sink:
            definitions = request_context.model_request_parameters.tool_defs
            tool_names = (list(definitions) if isinstance(definitions, dict)
                          else [getattr(item, "name", "unknown") for item in definitions])
            self.event_sink(ModelStreamEvent(
                kind="model.request.prepared",
                message=f"Prepared model request with {len(tool_names)} tool(s)",
                payload={
                    "toolChoice": request_context.model_settings.get("tool_choice"),
                    "tools": tool_names,
                },
            ))
        return request_context


def coalesced_event_handler(sink: ModelEventSink, *, checkpoint_chars: int = 512):
    """Return a Pydantic ``event_stream_handler`` with bounded durable delta volume.

    Semantic part/tool boundaries are retained. Text, thinking, and tool-argument deltas
    become character-count checkpoints rather than one database write per token. Raw parts
    belong in the serialized message history, not in broadly visible status events.
    """

    if checkpoint_chars < 1:
        raise ValueError("checkpoint_chars must be positive")

    async def handle(_ctx, stream) -> None:
        totals: dict[str, int] = {}
        emitted: dict[str, int] = {}
        async for event in stream:
            event_kind = getattr(event, "event_kind", type(event).__name__)
            if event_kind == "part_delta":
                delta = event.delta
                part_kind = getattr(delta, "part_delta_kind", "unknown").replace("_", "-")
                content = getattr(delta, "content_delta", None)
                if content is None:
                    content = getattr(delta, "args_delta", None)
                size = len(content) if isinstance(content, (str, dict)) else 0
                totals[part_kind] = totals.get(part_kind, 0) + size
                if totals[part_kind] - emitted.get(part_kind, 0) >= checkpoint_chars:
                    emitted[part_kind] = totals[part_kind]
                    sink(ModelStreamEvent(
                        kind=f"model.{part_kind}.streaming",
                        message=f"Streaming {part_kind}",
                        payload={"characters": totals[part_kind]},
                    ))
                continue
            if event_kind in {"part_start", "part_end"}:
                part = event.part
                part_kind = getattr(part, "part_kind", "unknown")
                payload = {"part": part_kind, "index": event.index}
                if hasattr(part, "tool_name"):
                    payload.update(toolName=part.tool_name,
                                   toolCallId=getattr(part, "tool_call_id", None))
                sink(ModelStreamEvent(
                    kind=f"model.part.{event_kind.removeprefix('part_')}",
                    message=f"Model {part_kind} part {event_kind.removeprefix('part_')}",
                    payload=payload,
                ))
                continue
            if event_kind == "function_tool_call":
                sink(ModelStreamEvent(
                    kind="model.tool.requested",
                    message=f"Model requested {event.part.tool_name}",
                    payload={"toolName": event.part.tool_name,
                             "toolCallId": event.part.tool_call_id},
                ))
                continue
            if event_kind == "function_tool_result":
                sink(ModelStreamEvent(
                    kind="model.tool.returned",
                    message=f"Tool result returned for {event.part.tool_name}",
                    payload={"toolName": event.part.tool_name,
                             "toolCallId": event.part.tool_call_id,
                             "outcome": getattr(event.part, "outcome", "success")},
                ))
                continue
            if event_kind == "final_result":
                sink(ModelStreamEvent(
                    kind="model.finalizing",
                    message="Model produced a final result",
                    payload={"toolName": event.tool_name,
                             "toolCallId": event.tool_call_id},
                ))

        for part_kind, total in totals.items():
            if total and emitted.get(part_kind, 0) != total:
                sink(ModelStreamEvent(
                    kind=f"model.{part_kind}.streamed",
                    message=f"Finished streaming {part_kind}",
                    payload={"characters": total},
                ))

    return handle


__all__ = [
    "ModelStreamEvent",
    "ModelEventSink",
    "RequiredToolTurn",
    "acquisition_output_types",
    "anthropic_cache_settings",
    "coalesced_event_handler",
    "dump_message_history",
    "load_message_history",
]
