export * as SessionBackground from "./background.js"

import type { Kind, Status } from "@opencode/schema/job"
import { Effect } from "effect"
import type { Job } from "../job.js"
import type { SessionSchema } from "./schema.js"

export type LookupSession = {
  readonly parentID?: SessionSchema.ID
  readonly location: { readonly directory: string }
}

export type Lookup = {
  readonly get: (sessionID: SessionSchema.ID) => Effect.Effect<LookupSession | undefined>
}

export type ListFilter = {
  /** Keep only jobs whose owning session is in this location directory. */
  readonly directory?: string
  readonly sessionID?: SessionSchema.ID
  readonly kind?: Kind
  readonly status?: Status
  readonly descendants?: boolean
}

/**
 * Background jobs still running for a Session or any of its descendants. A child's own
 * background shell counts even after the child reported to its parent: its notice wakes
 * the child again, so the child's work is not finished.
 */
export const outstanding = Effect.fnUntraced(function* (
  jobs: Pick<Job.Interface, "runningBackground">,
  store: Lookup,
  sessionID: SessionSchema.ID,
) {
  const sessions = memoize(store)
  const owned = yield* Effect.filter(yield* jobs.runningBackground, (recovery) =>
    descends(sessions, recovery.kind === "subagent" ? recovery.parentSessionID : recovery.sessionID, sessionID),
  )
  return owned.map((recovery) => {
    if (recovery.kind === "shell")
      return {
        kind: "shell" as const,
        sessionID: recovery.sessionID,
        shellID: recovery.shellID,
        command: recovery.command,
      }
    if (recovery.kind === "image")
      return { kind: "image" as const, sessionID: recovery.sessionID, path: recovery.path, model: recovery.model }
    return {
      kind: "subagent" as const,
      sessionID: recovery.parentSessionID,
      childSessionID: recovery.childSessionID,
      agent: recovery.agent,
      description: recovery.description,
    }
  })
})

/** Active and unconsumed background jobs matching the requested location, Session, kind, status, and descendant scope. */
export const list = Effect.fn("SessionBackground.list")(function* (
  jobs: Pick<Job.Interface, "backgroundJobs">,
  store: Lookup,
  filter: ListFilter = {},
) {
  const sessions = memoize(store)
  const sessionID = filter.sessionID
  return yield* Effect.filter(
    yield* jobs.backgroundJobs,
    Effect.fnUntraced(function* (item) {
      if (filter.kind !== undefined && item.kind !== filter.kind) return false
      if (filter.status !== undefined && item.status !== filter.status) return false
      if (sessionID !== undefined) {
        if (!filter.descendants && item.sessionID !== sessionID) return false
        if (filter.descendants && !(yield* descends(sessions, item.sessionID, sessionID))) return false
      }
      if (filter.directory === undefined) return true
      return (yield* sessions.get(item.sessionID))?.location.directory === filter.directory
    }),
  )
})

/** Caches session lookups for the duration of one walk over the job registry. */
function memoize(store: Lookup): Lookup {
  const cache = new Map<SessionSchema.ID, LookupSession | undefined>()
  return {
    get: (sessionID) =>
      cache.has(sessionID)
        ? Effect.succeed(cache.get(sessionID))
        : store.get(sessionID).pipe(Effect.tap((session) => Effect.sync(() => cache.set(sessionID, session)))),
  }
}

const descends = Effect.fnUntraced(function* (store: Lookup, from: SessionSchema.ID, ancestor: SessionSchema.ID) {
  const seen = new Set<SessionSchema.ID>()
  let current: SessionSchema.ID | undefined = from
  while (current !== undefined && !seen.has(current)) {
    if (current === ancestor) return true
    seen.add(current)
    current = (yield* store.get(current))?.parentID
  }
  return false
})
