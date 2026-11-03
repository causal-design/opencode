export * as SessionOperation from "./operation.js"

import { createHash } from "node:crypto"
import { Effect, Schema } from "effect"
import { Event } from "@opencode/schema/event"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { Bus } from "../bus.js"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import { SessionEvent } from "./event.js"
import { SessionStore } from "./store.js"

const locks = KeyedMutex.makeUnsafe<SessionMessage.ID>()

export class Conflict extends Schema.TaggedError<Conflict>()("SessionOperation.Conflict", {
  id: SessionMessage.ID,
  message: Schema.String,
}) {}

export const childID = (id: SessionMessage.ID, label: string) => SessionMessage.ID.make(
  `msg_${/^[0-9a-f]{12}$/.test(id.slice(4, 16)) ? id.slice(4, 16) : "000000000000"}${createHash("sha256").update(`${id}:${label}`).digest("hex").slice(0, 14)}`,
)

export function derivedMetadata(metadata: Record<string, unknown> | undefined, source: string) {
  const workspace = metadata?.workspace
  return {
    ...metadata,
    ...(workspace && typeof workspace === "object" && !Array.isArray(workspace) ? { workspace: {
      ...workspace,
      sender: { id: "opencode", name: "OpenCode", kind: "automation" },
      contributions: [workspace], source,
    } } : {}),
  }
}

/** Command/shell side effects are bracketed by existing durable Session events.
 * An interrupted, unfinished invocation is inspected by the user, never replayed.
 * The mutex is process-local; the server already assumes one writer per database.
 */
export const run = <E, R>(input: {
  id: SessionMessage.ID
  sessionID: Session.ID
  kind: string
  metadata?: Record<string, unknown>
}, effect: Effect.Effect<void, E, R>) => locks.withLock(input.id)(Effect.gen(function* () {
  const bus = yield* Bus.Service
  const store = yield* SessionStore.Service
  const startedID = childID(input.id, "operation-started")
  const completedID = childID(input.id, "operation-completed")
  const started = yield* store.message(startedID)
  if (started) {
    const done = yield* store.message(completedID)
    if (started.sessionID !== input.sessionID || started.message.metadata?.operation !== input.kind)
      return yield* new Conflict({ id: input.id, message: "Operation ID belongs to another invocation" })
    if (done?.sessionID === input.sessionID) return
    return yield* new Conflict({ id: input.id, message: "Previous invocation has an uncertain outcome; inspect it before explicitly starting a new operation" })
  }
  yield* bus.publish(SessionEvent.Synthetic, {
    sessionID: input.sessionID,
    text: `Requested ${input.kind}.`,
    description: `Requested ${input.kind}`,
    metadata: { ...input.metadata, operation: input.kind, operationID: input.id },
  }, { id: Event.ID.make(startedID.replace(/^msg_/, "evt_")) })
  yield* effect
  yield* bus.publish(SessionEvent.Synthetic, {
    sessionID: input.sessionID,
    text: `Completed ${input.kind}.`,
    metadata: { ...derivedMetadata(input.metadata, "operation-completed"), operation: input.kind, operationID: input.id },
  }, { id: Event.ID.make(completedID.replace(/^msg_/, "evt_")) })
}).pipe(Effect.uninterruptible))
