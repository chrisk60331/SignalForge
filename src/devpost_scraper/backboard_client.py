from __future__ import annotations

import json
import os
from typing import Any, Awaitable, Callable, Mapping

from backboard import BackboardClient
from backboard.exceptions import BackboardAPIError

ToolHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class BackboardClientError(Exception):
    """Raised when a Backboard operation fails."""


def build_client() -> BackboardClient:
    api_key = os.getenv("BACKBOARD_API_KEY", "").strip()
    if not api_key:
        raise BackboardClientError(
            "Missing required environment variable `BACKBOARD_API_KEY`."
        )
    return BackboardClient(api_key=api_key)


async def ensure_assistant(
    client: BackboardClient,
    *,
    assistant_id: str | None,
    name: str,
    system_prompt: str,
    tools: list[dict[str, Any]],
) -> str:
    if assistant_id:
        return assistant_id
    assistant = await client.create_assistant(
        name=name,
        system_prompt=system_prompt,
        tools=tools,
    )
    return str(assistant.assistant_id)


async def _collect_stream(stream: Any) -> dict[str, Any]:
    """Drain a streaming add_message response into a unified result dict."""
    content_parts: list[str] = []
    tool_calls: list[Any] = []
    run_id: str | None = None
    status = "completed"

    async for chunk in stream:
        t = chunk.get("type")
        if t == "content_streaming":
            content_parts.append(chunk.get("content", ""))
        elif t == "tool_submit_required":
            status = "REQUIRES_ACTION"
            run_id = chunk.get("run_id")
            tool_calls = chunk.get("tool_calls", [])
        elif t == "run_ended":
            if chunk.get("status") not in (None, "completed"):
                raise BackboardClientError(
                    f"Run ended with status: {chunk.get('status')}"
                )

    return {
        "content": "".join(content_parts) or None,
        "status": status,
        "tool_calls": tool_calls,
        "run_id": run_id,
    }


async def run_in_thread(
    client: BackboardClient,
    *,
    assistant_id: str,
    user_message: str,
    tool_handlers: Mapping[str, ToolHandler],
    llm_provider: str = "openai",
    model_name: str = "gpt-4o-mini",
    max_tool_rounds: int = 6,
) -> str:
    """Create a thread, send a message via streaming, execute the tool loop."""
    thread = await client.create_thread(assistant_id)

    stream = await client.add_message(
        thread_id=thread.thread_id,
        content=user_message,
        stream=True,
        llm_provider=llm_provider,
        model_name=model_name,
    )
    result = await _collect_stream(stream)

    rounds = 0
    while result["status"] == "REQUIRES_ACTION":
        rounds += 1
        if rounds > max_tool_rounds:
            raise BackboardClientError(
                f"Tool loop exceeded {max_tool_rounds} rounds — aborting."
            )
        if not result["run_id"]:
            raise BackboardClientError("REQUIRES_ACTION without run_id.")
        if not result["tool_calls"]:
            raise BackboardClientError("REQUIRES_ACTION without tool_calls.")

        tool_outputs = []
        for tc in result["tool_calls"]:
            name = tc["function"]["name"] if isinstance(tc, dict) else tc.function.name
            args_raw = (
                tc["function"].get("arguments", "{}")
                if isinstance(tc, dict)
                else (tc.function.arguments or "{}")
            )
            args = args_raw if isinstance(args_raw, dict) else json.loads(args_raw or "{}")
            tc_id = tc["id"] if isinstance(tc, dict) else tc.id

            handler = tool_handlers.get(name)
            if handler is None:
                raise BackboardClientError(f"No handler registered for tool `{name}`.")

            call_result = await handler(args)
            tool_outputs.append({"tool_call_id": tc_id, "output": json.dumps(call_result)})

        stream = await client.submit_tool_outputs(
            thread_id=thread.thread_id,
            run_id=result["run_id"],
            tool_outputs=tool_outputs,
            stream=True,
        )
        result = await _collect_stream(stream)

    if not result["content"]:
        raise BackboardClientError("Run completed without content.")
    return result["content"]
