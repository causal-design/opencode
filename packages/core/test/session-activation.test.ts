import { describe, expect, setDefaultTimeout } from "bun:test"
import { Effect } from "effect"
import path from "path"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Watcher } from "@opencode/core/filesystem/watcher"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionOperation } from "@opencode/core/session/operation"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionProjector } from "@opencode/core/session/projector"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { tempGlobalLayer } from "./fixture/global"
import { offlineModels } from "./fixture/models"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

// Real Location boot with plugin discovery, so the first request races the initial plugin activation.
setDefaultTimeout(30_000)

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionProjector.node, Session.node, LocationServiceMap.node]),
    [
      Global.node.replace(tempGlobalLayer),
      offlineModels,
      Watcher.node.replace(Watcher.configured({ enabled: false })),
      SessionExecution.node.replace(SessionExecution.noopLayer),
    ],
  ),
)

const project = Effect.gen(function* () {
  const tmp = yield* tmpdirScoped()
  yield* Effect.promise(() =>
    Bun.write(
      path.join(tmp.path, ".opencode/plugins/prompt.ts"),
      `export default {
        id: "prompt-readiness",
        async setup(ctx) {
          await ctx.session.hook("prompt", (event) => {
            event.prompt.text = "Prepared by plugin"
          })
          await ctx.command.transform((editor) =>
            editor.add({ name: "ready", description: "Registered by plugin", execute: async () => {} }),
          )
        },
      }`,
    ),
  )
  return tmp
})

describe("Session waits for plugin activation", () => {
  it.live("reconciles concurrent command IDs before callback side effects and forwards attribution", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      const counter = path.join(tmp.path, "command-calls")
      yield* Effect.promise(() => Bun.write(path.join(tmp.path, ".opencode/plugins/once.ts"), `
        import { appendFile } from "node:fs/promises"
        export default { id: "command-once", async setup(ctx) {
          await ctx.command.transform(editor => editor.add({ name: "once", execute: async input => {
            await appendFile(${JSON.stringify(counter)}, "called\\n")
            await ctx.session.prompt({ ...input.prompt, sessionID: input.sessionID, delivery: input.delivery })
          }}))
        }}
      `))
      const sessions = yield* Session.Service
      const saved = yield* sessions.create({ location: { directory: AbsolutePath.make(tmp.path) } })
      const input = { sessionID: saved.id, id: SessionMessage.ID.make("msg_command_once"), command: "once", text: "body", metadata: { workspace: { sender: { id: "bob" } } } }
      yield* Effect.all([sessions.command(input), sessions.command(input)], { concurrency: 2 })
      yield* sessions.command(input)
      expect((yield* Effect.promise(() => Bun.file(counter).text())).trim().split("\n")).toHaveLength(1)
      expect((yield* sessions.inbox(saved.id)).filter(item => item.type === "user")).toMatchObject([
        { id: input.id, payload: { metadata: input.metadata } },
      ])
      expect(yield* sessions.operationOwner(input.id)).toMatchObject({ sessionID: saved.id, type: "user", metadata: input.metadata })
      expect(yield* sessions.operationOwner(SessionOperation.childID(input.id, "operation-started"))).toMatchObject({ sessionID: saved.id, type: "synthetic", metadata: input.metadata })
      expect(yield* sessions.operationOwner(SessionMessage.ID.make("msg_absent_operation"))).toBeUndefined()
    }),
  )
  it.live("runs prompt hooks from a cold Location before admitting a prompt", () =>
    Effect.gen(function* () {
      const tmp = yield* project
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ location: { directory: AbsolutePath.make(tmp.path) } })
      const admitted = yield* sessions.prompt({ sessionID: session.id, text: "Original", resume: false })
      expect(admitted.payload.text).toBe("Prepared by plugin")
    }),
  )

  it.live("resolves plugin commands from a cold Location", () =>
    Effect.gen(function* () {
      const tmp = yield* project
      const sessions = yield* Session.Service
      const session = yield* sessions.create({ location: { directory: AbsolutePath.make(tmp.path) } })
      yield* sessions.command({ sessionID: session.id, command: "ready", text: "now" })
    }),
  )
})
