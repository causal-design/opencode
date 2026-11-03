import { Job } from "@opencode/core/job"
import { Session } from "@opencode/core/session"
import { SessionBackground } from "@opencode/core/session/background"
import { SessionNotFoundError } from "@opencode/protocol/errors"
import type { ListQuery } from "@opencode/protocol/groups/job"
import { Effect, Option } from "effect"
import type { HttpServerRequest } from "effect/http"
import { explicitRef, requestRef } from "../location"
import { missingSession } from "./session-error"

/** `job.*` in the `server.experimental` group. Jobs are scoped to the location of their owning session. */
export const makeJob = Effect.gen(function* () {
  const jobs = yield* Job.Service
  const sessions = yield* Session.Service
  const lookup = {
    get: (sessionID: Session.ID) => sessions.get(sessionID).pipe(Effect.option, Effect.map(Option.getOrUndefined)),
  }

  return {
    list: Effect.fn("JobHandler.list")(function* (query: ListQuery, request: HttpServerRequest.HttpServerRequest) {
      const session =
        query.sessionID === undefined
          ? undefined
          : yield* sessions.get(query.sessionID).pipe(Effect.catchTag("Session.NotFoundError", missingSession))
      const directory = (explicitRef(request) ?? session?.location ?? requestRef(request)).directory
      if (session && session.location.directory !== directory) {
        return yield* new SessionNotFoundError({
          sessionID: session.id,
          message: `Session not found in location ${directory}: ${session.id}`,
        })
      }
      return {
        location: { directory },
        data: yield* SessionBackground.list(jobs, lookup, { ...query, directory }),
      }
    }),
  }
})
