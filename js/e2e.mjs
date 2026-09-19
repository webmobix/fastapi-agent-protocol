// End-to-end: pinned langgraph-sdk-js + raw protocol stream against the
// fixture app (booted separately, URL in argv[2]).
//
// Covers: SDK init (/info probe), thread create, stream to completed,
// interrupt/resume, cancel, reload hydration (state + history).
import { Client } from "@langchain/langgraph-sdk";

const BASE = process.argv[2] ?? "http://127.0.0.1:8123";
const client = new Client({ apiUrl: BASE });

const results = [];
function check(name, cond) {
  results.push([name, Boolean(cond)]);
  console.log(cond ? "ok  " : "FAIL", "-", name);
  if (!cond) process.exitCode = 1;
}

async function cmd(threadId, id, method, params) {
  const res = await fetch(`${BASE}/threads/${threadId}/commands`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ id, method, params }),
  });
  return res.json();
}

// Minimal SSE parser for text/event-stream bodies.
async function readStream(threadId, body, until) {
  const res = await fetch(`${BASE}/threads/${threadId}/stream`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status !== 200) throw new Error(`stream HTTP ${res.status}`);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  const envelopes = [];
  const deadline = Date.now() + 20000;
  let pendingEvent = null;
  for (;;) {
    if (Date.now() > deadline) throw new Error("stream read timed out");
    const { done, value } = await reader.read();
    if (value) buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf("\n\n")) >= 0) {
      const block = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      for (const line of block.split("\n")) {
        if (line.startsWith(":")) continue; // comment / heartbeat
        if (line.startsWith("event: ")) pendingEvent = line.slice(7);
        else if (line.startsWith("data: ")) {
          try {
            envelopes.push(JSON.parse(line.slice(6)));
          } catch {
            /* partial */
          }
        }
      }
    }
    if (until(envelopes)) {
      await reader.cancel();
      return envelopes;
    }
    if (done) return envelopes;
  }
}

const hasTerminal = (envs) =>
  envs.some(
    (e) =>
      e.method === "lifecycle" &&
      ["completed", "failed", "interrupted"].includes(e.params?.data?.event),
  );

// 1. SDK init probe: client construction must not 404 on /info.
const infoRes = await fetch(`${BASE}/info`);
check("GET /info 200 (SDK init probe)", infoRes.status === 200);

// 2. Thread lifecycle via the pinned SDK.
const thread = await client.threads.create({ metadata: { e2e: "js" } });
const threadId = thread.thread_id;
check("SDK threads.create returns thread_id", typeof threadId === "string");

// 3. Stream a run to completion.
let r = await cmd(threadId, 1, "run.start", {
  assistant_id: "echo",
  input: { messages: [{ role: "user", content: "hello-js" }] },
});
check("run.start success envelope", r.type === "success" && r.result?.run_id);
let envs = await readStream(threadId, { channels: ["lifecycle", "messages", "values"] }, hasTerminal);
check("stream reaches completed", envs.some((e) => e.params?.data?.event === "completed"));
check("stream carries values", envs.some((e) => e.method === "values"));

// 3b. Message frames via the model-backed fixture agent.
const tChat = (await client.threads.create({ metadata: {} })).thread_id;
await cmd(tChat, 1, "run.start", {
  assistant_id: "chat",
  input: { messages: [{ role: "user", content: "hi" }] },
});
envs = await readStream(
  tChat,
  { channels: ["messages"] },
  (e) => e.some((x) => x.method === "messages" && x.params?.data?.event === "message-finish"),
);
check(
  "stream carries message frames",
  envs.some((e) => e.method === "messages" && e.params?.data?.event === "message-start"),
);

// 4. Interrupt + resume.
const t2 = (await client.threads.create({ metadata: {} })).thread_id;
await cmd(t2, 1, "run.start", {
  assistant_id: "ask",
  input: { messages: [{ role: "user", content: "go" }] },
});
let interruptId = null;
for (let i = 0; i < 100 && !interruptId; i++) {
  const st = await client.threads.getState(t2);
  const ints = st.interrupts ?? [];
  if (ints.length > 0) interruptId = ints[0].id;
  else await new Promise((r2) => setTimeout(r2, 100));
}
check("interrupt surfaces in state", Boolean(interruptId));
// Cursor past everything the first run published (its `interrupted` would
// otherwise satisfy the terminal predicate immediately). Capture BEFORE
// responding — the resumed run publishes right after.
const maxSeq = (list) => list.reduce((m, e) => Math.max(m, e.seq ?? 0), 0);
const preSeq = maxSeq(
  await readStream(t2, {}, (e) => e.length >= 5 || hasTerminal(e)),
);
r = await cmd(t2, 2, "input.respond", { interrupt_id: interruptId, response: "yes" });
check("input.respond success envelope", r.type === "success");
// Wait for the resumed run to settle, then assert ITS terminal event.
let settled = false;
for (let i = 0; i < 100 && !settled; i++) {
  const st = await client.threads.getState(t2);
  settled = (st.interrupts ?? []).length === 0 && (st.next ?? []).length === 0;
  if (!settled) await new Promise((r2) => setTimeout(r2, 100));
}
check("resumed run settles", settled);
envs = await readStream(t2, { since: preSeq }, hasTerminal);
check("resumed run completes", envs.some((e) => e.params?.data?.event === "completed"));

// 5. Cancel a slow run via REST.
const t3 = (await client.threads.create({ metadata: {} })).thread_id;
r = await cmd(t3, 1, "run.start", {
  assistant_id: "slow",
  input: { messages: [{ role: "user", content: "zzz" }] },
});
const runId = r.result.run_id;
await new Promise((r2) => setTimeout(r2, 300));
const cancelRes = await fetch(`${BASE}/threads/${t3}/runs/${runId}/cancel`, { method: "POST" });
check("REST cancel 200", cancelRes.status === 200);
envs = await readStream(t3, { channels: ["lifecycle"] }, hasTerminal);
check(
  "cancelled run reports interrupted",
  envs.some((e) => e.params?.data?.event === "interrupted"),
);

// 6. Reload hydration via the SDK.
const state = await client.threads.getState(threadId);
check(
  "state hydrates messages after reload",
  JSON.stringify(state.values ?? {}).includes("hello-js"),
);
const history = await client.threads.getHistory(threadId);
check("history returns entries", Array.isArray(history) && history.length >= 1);

const failed = results.filter(([, ok]) => !ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length ? 1 : 0);
