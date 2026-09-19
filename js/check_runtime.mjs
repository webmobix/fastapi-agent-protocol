// Wiring proof: the Assistant-UI LangGraph runtime entrypoint resolves.
// (In Assistant-UI docs this family is called the "stream runtime"; the
// pinned package's export is named `useLangGraphRuntime`.) The browser wires
// it with a `stream` callback that POSTs to /threads/{id}/stream and parses
// the SSE envelopes this library serves — e2e.mjs exercises that exact
// over-the-wire shape.
import { useLangGraphRuntime } from "@assistant-ui/react-langgraph";

if (typeof useLangGraphRuntime !== "function") {
  throw new Error("useLangGraphRuntime did not resolve to a function");
}
console.log("useLangGraphRuntime wiring OK:", typeof useLangGraphRuntime);
