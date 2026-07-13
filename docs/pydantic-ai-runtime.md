# Durable Pydantic AI turns

Install `muster[model]` to use `muster.pydantic_runtime`. The module provides the
provider-neutral pieces that an application needs to make a Pydantic AI worker durable:

- lossless message-history serialization, including thinking signatures and tool ids;
- coalesced model/thinking/tool stream events for an application ledger;
- a dynamic required-tool capability for a bounded acquisition turn;
- a schema-free acquisition output contract (`str | DeferredToolRequests`); and
- explicit Anthropic-compatible caching defaults for normal and continuation turns.

Muster deliberately does not store model messages in its core schema. An application owns
the retention, encryption, redaction, and access policy for prompts and provider-native
thinking. Store `dump_message_history(...)` in an application table or artifact, then use
`load_message_history(...)` when resuming with `DeferredToolResults`.

## Recommended deferred-tool shape

1. Start a Pydantic run with `output_type=acquisition_output_types()` and attach
   `RequiredToolTurn()`.
2. If the output is `DeferredToolRequests`, persist `result.all_messages()` and turn each
   request into a normal Muster work item.
3. Use Muster's durable question channel before executing tools that require approval.
4. When all tool work is terminal, start a separate continuation run with the stored
   history, `DeferredToolResults`, and the application's structured report output.
5. Feed `coalesced_event_handler(...)` into both turns. Persist semantic boundaries and
   bounded checkpoints; keep raw model parts in message history.

Separating acquisition from reporting prevents a permanently required tool choice from
blocking final output and keeps tool execution outside the model process.

## Anthropic-compatible gateway note: MiniMax-M3

Verified against `https://api.minimax.io/anthropic` on 2026-07-13:

- MiniMax-M3 supports Anthropic streaming tool calls and returns `tool_use` when
  `tool_choice={"type":"any"}` is used by itself.
- In observed tests, combining forced tool choice with explicit `cache_control` blocks
  caused MiniMax-M3 to return text instead of a tool call.
- Combining the forced acquisition turn with a prompted structured-output schema also made
  tool selection unreliable.

For that combination, attach `RequiredToolTurn(disable_anthropic_cache=True)` and keep the
acquisition turn on `acquisition_output_types()`. Restore `anthropic_cache_settings()` on
the separate continuation/report turn. Treat provider-returned text on a mandatory tool
turn as a failed protocol step rather than as evidence that a tool was requested.

This is a gateway compatibility policy, not a reason to add provider-specific behavior to
Muster's ledger or work queue.
