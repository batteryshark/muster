from __future__ import annotations

import asyncio

from pydantic_ai import DeferredToolRequests
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    PartDeltaEvent,
    PartEndEvent,
    PartStartEvent,
    ThinkingPart,
    ThinkingPartDelta,
    ToolCallPart,
    UserPromptPart,
)

from muster.pydantic_runtime import (
    RequiredToolTurn,
    acquisition_output_types,
    anthropic_cache_settings,
    coalesced_event_handler,
    dump_message_history,
    load_message_history,
)


def test_message_history_round_trip_preserves_thinking_and_tool_identity():
    messages = [
        ModelRequest(parts=[UserPromptPart("inspect")]),
        ModelResponse(parts=[
            ThinkingPart("consider evidence", signature="provider-signature"),
            ToolCallPart("probe", {}, "call-1"),
        ]),
    ]
    restored = load_message_history(dump_message_history(messages))
    thinking, call = restored[1].parts
    assert thinking.signature == "provider-signature"
    assert call.tool_name == "probe" and call.tool_call_id == "call-1"


def test_required_tool_turn_is_dynamic_and_can_disable_gateway_cache_points():
    resolver = RequiredToolTurn(disable_anthropic_cache=True).get_model_settings()
    settings = resolver(None)
    assert settings["tool_choice"] == "required"
    assert settings["anthropic_cache_messages"] is False
    assert settings["anthropic_cache_instructions"] is False
    assert settings["anthropic_cache_tool_definitions"] is False


def test_acquisition_contract_and_continuation_cache_defaults():
    assert acquisition_output_types() == [str, DeferredToolRequests]
    assert anthropic_cache_settings() == {
        "anthropic_cache_messages": True,
        "anthropic_cache_instructions": True,
        "anthropic_cache_tool_definitions": True,
    }


def test_stream_handler_preserves_boundaries_and_coalesces_thinking_deltas():
    events = []

    async def stream():
        yield PartStartEvent(index=0, part=ThinkingPart(""))
        yield PartDeltaEvent(
            index=0, delta=ThinkingPartDelta(content_delta="x" * 600)
        )
        yield PartEndEvent(index=0, part=ThinkingPart("x" * 600))

    asyncio.run(coalesced_event_handler(events.append)(None, stream()))
    kinds = [event.kind for event in events]
    assert kinds == [
        "model.part.start",
        "model.thinking.streaming",
        "model.part.end",
    ]
    assert events[1].payload == {"characters": 600}
