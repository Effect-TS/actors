/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { ActorSystemTag, LiveActorSystem } from "@effect-ts/actors/ActorSystem"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import type { _A } from "@effect-ts/core/Utils"
import * as Z from "@effect-ts/keeper"
import * as R from "@effect-ts/node/Runtime"
import * as PG from "@effect-ts/pg"
import * as S from "@effect-ts/schema"
import { tag } from "@effect-ts/system/Has"
import { matchTag_ } from "@effect-ts/system/Utils"

import * as Cluster from "../Cluster"
import { LivePersistence } from "../Persistence"
import { RemotingExpress, StaticRemotingExpressConfig } from "../Remote"
import { transactional } from "../Transactional"
import { sharded } from "."

const AppLayer = LiveActorSystem("EffectTsActorsDemo")
  [">+>"](RemotingExpress)
  ["<<<"](
    StaticRemotingExpressConfig({
      host: "127.0.0.1",
      port: parseInt(process.env["PORT"]!)
    })
  )
  [">+>"](Cluster.LiveCluster)
  ["<+<"](
    Z.LiveKeeperClient["<<<"](
      L.succeed(
        Z.KeeperConfig.of(Z.makeKeeperConfig({ connectionString: "127.0.0.1:2181" }))
      )
    )
  )
  ["<+<"](
    LivePersistence["<+<"](
      PG.LivePG["<<<"](
        L.succeed(
          PG.PGConfig.of(
            PG.makePGConfig({
              host: "127.0.0.1",
              port: 5432,
              user: "user",
              password: "pass",
              database: "db"
            })
          )
        )
      )
    )
  )

@S.stable
class User extends S.Model<User>()(
  S.props({ _tag: S.prop(S.literal("User")), id: S.prop(S.string) })
) {}

@S.stable
class UserNotFound extends S.Model<UserNotFound>()(
  S.props({ _tag: S.prop(S.literal("UserNotFound")) })
) {}

@S.stable
class UserAlreadyCreated extends S.Model<UserAlreadyCreated>()(
  S.props({ _tag: S.prop(S.literal("UserAlreadyCreated")) })
) {}

class Get extends AM.Message(
  "Get",
  S.props({ id: S.prop(S.string) }),
  S.union({ User, UserNotFound })
) {}

class Create extends AM.Message(
  "Create",
  S.props({ id: S.prop(S.string) }),
  S.union({ User, UserAlreadyCreated })
) {}

const Message = AM.messages(Get, Create)
type Message = AM.TypeOf<typeof Message>

class Initial extends S.Model<Initial>()(
  S.props({ _tag: S.prop(S.literal("Initial")) })
) {}

const userHandler = transactional(
  Message,
  S.union({ Initial, User }),
  O.some(S.string)
)(({ event, state }) => ({
  Create: ({ id }) =>
    T.gen(function* (_) {
      if ((yield* _(state.get))._tag !== "Initial") {
        return yield* _(T.succeed(new UserAlreadyCreated({})))
      }
      yield* _(event.emit("create-user"))
      yield* _(event.emit("setup-user"))
      const user = new User({ id })
      yield* _(state.set(user))
      return user
    }),
  Get: () =>
    T.gen(function* (_) {
      return matchTag_(yield* _(state.get), {
        Initial: () => new UserNotFound({}),
        User: (_) => _
      })
    })
}))

export const makeUsersService = M.gen(function* (_) {
  const system = yield* _(ActorSystemTag)

  const users = yield* _(
    system.make("users", SUP.none, sharded(userHandler), () => new Initial({}))
  )

  return {
    users
  }
})

export interface UsersService extends _A<typeof makeUsersService> {}
export const UsersService = tag<UsersService>()
export const LiveUsersService = L.fromManaged(UsersService)(makeUsersService)

const program = T.gen(function* (_) {
  const { users } = yield* _(UsersService)
  console.log(yield* _(users.ask(new Get({ id: "mike" }))))
  console.log(yield* _(users.ask(new Create({ id: "mike" }))))
  console.log(yield* _(users.ask(new Get({ id: "mike" }))))
  console.log(yield* _(users.ask(new Get({ id: "mike-2" }))))
  console.log(yield* _(users.ask(new Create({ id: "mike" }))))
  console.log((yield* _(PG.query("SELECT * FROM state_journal"))).rows)
  console.log((yield* _(PG.query("SELECT * FROM event_journal"))).rows)

  yield* _(T.sleep(2_000))
})

pipe(program, T.provideSomeLayer(AppLayer[">+>"](LiveUsersService)), R.runMain)
