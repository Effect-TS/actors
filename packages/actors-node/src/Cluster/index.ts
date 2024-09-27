import { ActorProxy } from "@effect-ts/actors/Actor"
import { ActorRefRemote } from "@effect-ts/actors/ActorRef"
import * as AS from "@effect-ts/actors/ActorSystem"
import { Message, messages } from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import { Tagged } from "@effect-ts/core/Case"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as SortedSet from "@effect-ts/core/Collections/Immutable/SortedSet"
import * as T from "@effect-ts/core/Effect"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as Ref from "@effect-ts/core/Effect/Ref"
import { pipe } from "@effect-ts/core/Function"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import * as OT from "@effect-ts/core/OptionT"
import * as Ord from "@effect-ts/core/Ord"
import { chainF } from "@effect-ts/core/Prelude"
import type { _A } from "@effect-ts/core/Utils"
import * as K from "@effect-ts/keeper"
import * as S from "@effect-ts/schema"
import * as Guard from "@effect-ts/schema/Guard"
import * as Th from "@effect-ts/schema/These"

export const EO = pipe(OT.monad(T.Monad), (M) => ({
  map: M.map,
  chain: chainF(M),
  chainT:
    <R2, E2, A, B>(f: (a: A) => T.Effect<R2, E2, B>) =>
    <R, E>(fa: T.Effect<R, E, O.Option<A>>): T.Effect<R2 & R, E2 | E, O.Option<B>> =>
      chainF(M)((a: A) => T.map_(f(a), O.some))(fa)
}))

export const ClusterSym = Symbol()

export class HostPort extends Tagged("HostPort")<{
  readonly host: string
  readonly port: number
}> {}

export class ClusterException extends Tagged("ClusterException")<{
  readonly message: string
}> {}

export class ActorError extends Tagged("ActorError")<{
  readonly message: string
}> {}

export interface MemberIdBrand {
  readonly MemberIdBrand: unique symbol
}

export type MemberId = string & MemberIdBrand

export class Member extends S.Model<Member>()(
  S.props({
    id: S.prop(S.string["|>"](S.brand<MemberId>()))
  })
) {}

const OrdMember = Ord.contramap_(Ord.string, (m: Member) => m.id)

export class Members extends S.Model<Members>()(
  S.props({
    members: S.prop(sortedSet(Member, OrdMember))
  })
) {}

export class GetMembers extends Message("GetMembers", S.props({}), Members) {}
export class Init extends Message("Init", S.props({}), S.props({})) {}
export class Join extends Message(
  "Join",
  S.props({
    id: S.prop(S.string["|>"](S.brand<MemberId>()))
  }),
  S.props({})
) {}
export class Leave extends Message(
  "Leave",
  S.props({
    id: S.prop(S.string["|>"](S.brand<MemberId>()))
  }),
  S.props({})
) {}

export type Protocol = GetMembers | Join | Init | Leave

export function sortedSet<X extends S.SchemaUPI>(
  child: X,
  ord: Ord.Ord<S.ParsedShapeOf<X>>
) {
  return S.chunk(child)[">>>"](
    pipe(
      S.identity(
        (u): u is SortedSet.SortedSet<S.ParsedShapeOf<X>> =>
          u instanceof SortedSet.SortedSet && SortedSet.every_(u, Guard.for(child))
      ),
      S.parser((u: Chunk.Chunk<S.ParsedShapeOf<X>>) =>
        Th.succeed(
          Chunk.reduce_(u, SortedSet.make<S.ParsedShapeOf<X>>(ord), SortedSet.add_)
        )
      ),
      S.encoder(Chunk.from)
    )
  )
}

export function fromChunk(u: Chunk.Chunk<Member>): SortedSet.SortedSet<Member> {
  return Chunk.reduce_(u, SortedSet.make(OrdMember), SortedSet.add_)
}

export const makeCluster = M.gen(function* (_) {
  const cli = yield* _(K.KeeperClient)
  const system = yield* _(AS.ActorSystemTag)
  const clusterDir = `/cluster/${system.actorSystemName}`
  const membersDir = `${clusterDir}/members`

  if (O.isNone(system.remoteConfig)) {
    return yield* _(
      T.die(`actor system ${system.actorSystemName} doesn't support remoting`)
    )
  }

  yield* _(cli.mkdir(membersDir))

  const prefix = `${membersDir}/member_`

  const nodePath = yield* _(
    cli
      .create(prefix, {
        mode: "EPHEMERAL_SEQUENTIAL",
        data: Buffer.from(
          JSON.stringify({
            host: system.remoteConfig.value.host,
            port: system.remoteConfig.value.port
          })
        )
      })
      ["|>"](M.make((p) => cli.remove(p)["|>"](T.orDie)))
  )

  const nodeId = `member_${nodePath.substr(prefix.length)}` as MemberId

  const membersRef = yield* _(
    Ref.makeRef(
      new Members({
        members: SortedSet.add_(SortedSet.make(OrdMember), new Member({ id: nodeId }))
      })
    )
  )

  const ops = yield* _(Q.makeUnbounded<Join | Leave>())

  const isLeader = yield* _(Ref.makeRef(false))

  const manager = yield* _(
    pipe(
      system.make(
        "cluster-manager",
        SUP.none,
        new ActorProxy(messages(GetMembers, Join, Init, Leave), (queue, context) =>
          T.gen(function* (_) {
            while (1) {
              const [m, p] = yield* _(Q.take(queue))

              switch (m._tag) {
                case "Init": {
                  const members = Chunk.map_(
                    yield* _(cli.getChildren(membersDir)),
                    (s) => new Member({ id: s as MemberId })
                  )
                  yield* _(
                    Ref.update_(membersRef, (x) =>
                      x.copy({ members: fromChunk(members) })
                    )
                  )
                  yield* _(pipe(T.succeed({}), T.to(p)))
                  break
                }
                case "Join": {
                  yield* _(
                    Ref.update_(membersRef, (x) =>
                      x.copy({
                        members: SortedSet.add_(x.members, new Member({ id: m.id }))
                      })
                    )
                  )
                  if (yield* _(Ref.get(isLeader))) {
                    yield* _(Q.offer_(ops, m))
                    yield* _(
                      pipe(
                        cli.waitDelete(`${membersDir}/${m.id}`),
                        T.chain(() =>
                          pipe(
                            context.self,
                            T.chain((self) => self.ask(new Leave({ id: m.id })))
                          )
                        ),
                        T.fork
                      )
                    )
                  }
                  yield* _(pipe(T.succeed({}), T.to(p)))
                  break
                }
                case "Leave": {
                  yield* _(
                    Ref.update_(membersRef, (x) =>
                      x.copy({
                        members: SortedSet.filter_(x.members, (_) => _.id !== m.id)
                      })
                    )
                  )
                  if (yield* _(Ref.get(isLeader))) {
                    yield* _(Q.offer_(ops, m))
                  }
                  yield* _(pipe(T.succeed({}), T.to(p)))
                  break
                }
                case "GetMembers": {
                  yield* _(
                    pipe(
                      Ref.get(membersRef),
                      T.map((_) =>
                        _.copy({
                          members: _.members
                        })
                      ),
                      T.to(p)
                    )
                  )
                  break
                }
              }
            }
            return yield* _(T.never)
          })
        ),
        {}
      ),
      M.make((s) => s.stop["|>"](T.orDie))
    )
  )

  const runOnClusterLeader = <R, E, R2, E2>(
    onLeader: T.Effect<R, E, never>,
    whileFollower: (leader: MemberId) => T.Effect<R2, E2, never>
  ): T.Effect<R & R2 & T.DefaultEnv, K.ZooError | E | E2, never> => {
    return T.gen(function* (_) {
      while (1) {
        const leader = Chunk.head(yield* _(cli.getChildren(membersDir)))
        if (O.isSome(leader)) {
          if (leader.value === nodeId) {
            yield* _(onLeader)
          } else {
            yield* _(
              T.race_(
                whileFollower(leader.value as MemberId),
                watchMember(leader.value as MemberId)
              )
            )
          }
        } else {
          yield* _(T.sleep(5))
        }
      }
      return yield* _(T.never)
    })
  }

  yield* _(
    T.forkManaged(
      runOnClusterLeader(
        T.gen(function* (_) {
          yield* _(Ref.set_(isLeader, true))
          yield* _(manager.ask(new Init()))
          while (1) {
            const j = yield* _(Q.take(ops))
            if (j._tag === "Join") {
              const { host, port } = yield* _(memberHostPort(j.id))
              const recipient = `zio://${system.actorSystemName}@${host}:${port}/cluster-manager`
              const ref = new ActorRefRemote<Protocol>(recipient, system)
              const { members } = yield* _(Ref.get(membersRef))
              yield* _(
                T.forEach_(
                  SortedSet.filter_(members, (m) => m.id !== j.id),
                  (m) => ref.ask(new Join({ id: m.id }))
                )
              )
              yield* _(
                T.forEach_(
                  SortedSet.filter_(members, (m) => m.id !== j.id && m.id !== nodeId),
                  (m) =>
                    T.gen(function* (_) {
                      const { host, port } = yield* _(memberHostPort(m.id))
                      const recipient = `zio://${system.actorSystemName}@${host}:${port}/cluster-manager`
                      const ref = new ActorRefRemote<Protocol>(recipient, system)
                      yield* _(ref.ask(new Join({ id: j.id })))
                    })
                )
              )
            } else {
              const { members } = yield* _(Ref.get(membersRef))
              yield* _(
                T.forEach_(
                  SortedSet.filter_(members, (m) => m.id !== j.id && m.id !== nodeId),
                  (m) =>
                    T.gen(function* (_) {
                      const { host, port } = yield* _(memberHostPort(m.id))
                      const recipient = `zio://${system.actorSystemName}@${host}:${port}/cluster-manager`
                      const ref = new ActorRefRemote<Protocol>(recipient, system)
                      yield* _(ref.ask(new Leave({ id: j.id })))
                    })
                )
              )
            }
          }
          return yield* _(T.never)
        }),
        (l: MemberId) =>
          T.gen(function* (_) {
            yield* _(manager.ask(new Init()))
            const { host, port } = yield* _(memberHostPort(l))
            const recipient = `zio://${system.actorSystemName}@${host}:${port}/cluster-manager`
            const ref = new ActorRefRemote<Protocol>(recipient, system)

            yield* _(ref.ask(new Join({ id: nodeId })))

            return yield* _(T.never)
          })
      )
    )
  )

  const memberHostPort = yield* _(
    T.memoize(
      (member: MemberId): T.Effect<unknown, K.ZooError, HostPort> =>
        pipe(
          cli.getData(`${membersDir}/${member}`),
          T.chain(
            O.fold(
              () =>
                T.die(
                  new ClusterException({
                    message: `cannot find metadata on path: ${membersDir}/${member}`
                  })
                ),
              (b) => T.succeed(b)
            )
          ),
          T.map((b) => new HostPort(JSON.parse(b.toString("utf8"))))
        )
    )
  )

  const members = pipe(
    cli.getChildren(membersDir),
    T.chain(T.forEach((x) => memberHostPort(x as MemberId)))
  )

  const init = (scope: string) => cli.mkdir(`${clusterDir}/elections/${scope}`)

  const join = (scope: string) =>
    cli.create(`${clusterDir}/elections/${scope}/w_`, {
      mode: "EPHEMERAL_SEQUENTIAL",
      data: Buffer.from(nodeId)
    })

  const leave = (nodePath: string) => cli.remove(nodePath)

  const leaderPath = (scope: string) =>
    pipe(cli.getChildren(`${clusterDir}/elections/${scope}`), T.map(Chunk.head))

  const leaderId = (scope: string) =>
    pipe(
      leaderPath(scope),
      EO.chain((s) => cli.getData(`${clusterDir}/elections/${scope}/${s}`)),
      EO.map((b) => b.toString("utf-8") as MemberId)
    )

  const runOnLeader =
    (scope: string) =>
    <R, E, R2, E2>(
      onLeader: T.Effect<R, E, never>,
      whileFollower: (leader: MemberId) => T.Effect<R2, E2, never>
    ): T.Effect<R & R2, K.ZooError | E | E2, never> => {
      return T.gen(function* (_) {
        while (1) {
          const leader = yield* _(leaderId(scope))
          if (O.isNone(leader)) {
            return yield* _(T.die("cannot find a leader"))
          }
          if (leader.value === nodeId) {
            yield* _(onLeader)
          } else {
            yield* _(T.race_(watchLeader(scope), whileFollower(leader.value)))
          }
        }
        return yield* _(T.never)
      })
    }

  const watchMember = (member: MemberId) => cli.waitDelete(`${membersDir}/${member}`)

  const watchLeader = (scope: string) =>
    pipe(
      leaderPath(scope),
      T.chain((o) => {
        return O.isNone(o)
          ? T.die("cannot find a leader")
          : cli.waitDelete(`${clusterDir}/elections/${scope}/${o.value}`)
      })
    )

  return {
    [ClusterSym]: ClusterSym,
    members,
    init,
    join,
    leave,
    nodeId,
    memberHostPort,
    leaderId,
    runOnLeader,
    watchLeader,
    watchMember,
    manager
  } as const
})

export interface Cluster extends _A<typeof makeCluster> {}
export const Cluster = tag<Cluster>()
export const LiveCluster = L.fromManaged(Cluster)(makeCluster)
