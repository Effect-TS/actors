import type { AbstractStateful } from "@effect-ts/actors/Actor"
import * as Act from "@effect-ts/actors/Actor"
import { actorRef } from "@effect-ts/actors/ActorRef"
import * as AM from "@effect-ts/actors/Message"
import * as SUP from "@effect-ts/actors/Supervisor"
import * as Chunk from "@effect-ts/core/Collections/Immutable/Chunk"
import * as HashSet from "@effect-ts/core/Collections/Immutable/HashSet"
import * as T from "@effect-ts/core/Effect"
import * as M from "@effect-ts/core/Effect/Managed"
import { nextIntBetween } from "@effect-ts/core/Effect/Random"
import * as STM from "@effect-ts/core/Effect/Transactional/STM"
import * as TRef from "@effect-ts/core/Effect/Transactional/TRef"
import { pipe } from "@effect-ts/core/Function"
import * as O from "@effect-ts/core/Option"
import * as S from "@effect-ts/schema"

import { Cluster } from "../Cluster"
import { makeSingleton } from "../Singleton"
import { transactional } from "../Transactional"

export class Join extends AM.Message(
  "Join",
  S.props({
    nodeId: S.prop(S.string),
    actor: S.prop(actorRef<AM.TypeOf<typeof Worker>>())
  }),
  S.props({
    workerId: S.prop(S.number)
  })
) {}

@S.stable
export class ShardLocation extends S.Model<ShardLocation>()(
  S.props({
    _tag: S.prop(S.literal("ShardLocation")),
    nodeId: S.prop(S.string),
    actor: S.prop(actorRef<AM.TypeOf<typeof Worker>>())
  })
) {}

@S.stable
export class NoWorker extends S.Model<NoWorker>()(
  S.props({
    _tag: S.prop(S.literal("NoWorker"))
  })
) {}

export class GetShardLocation extends AM.Message(
  "GetShardLocation",
  S.props({
    shard: S.prop(S.number)
  }),
  S.union({ ShardLocation, NoWorker })
) {}

export const Coordinator = AM.messages(Join, GetShardLocation)

export class Assign extends AM.Message(
  "Assign",
  S.props({
    shard: S.prop(S.number)
  }),
  S.props({})
) {}

export const Worker = AM.messages(Assign)

export function sharded<R, S, F1 extends AM.AnyMessage>(
  stateful: AbstractStateful<R, S, F1>
) {
  return new Act.ActorProxy(stateful.messages, (queue, context) =>
    pipe(
      M.gen(function* (_) {
        const cluster = yield* _(Cluster)

        const local = yield* _(TRef.makeCommit(HashSet.make<number>()))

        const worker = yield* _(
          pipe(
            context.make(
              cluster.nodeId,
              SUP.none,
              Act.stateful(
                Worker,
                S.unknown
              )(() => ({
                Assign: ({ shard }) =>
                  T.gen(function* (_) {
                    yield* _(STM.commit(TRef.update_(local, HashSet.add(shard))))

                    return yield* _(T.succeed({}))
                  })
              })),
              {}
            ),
            M.makeInterruptible((_) => _.stop["|>"](T.orDie))
          )
        )

        console.log(worker.address)

        const coordinator = yield* _(
          pipe(
            context.make(
              "coordinator",
              SUP.none,
              makeSingleton(
                transactional(
                  Coordinator,
                  S.props({
                    nodes: S.prop(
                      S.chunk(
                        S.props({
                          workerId: S.prop(S.number),
                          nodeId: S.prop(S.string),
                          actor: S.prop(actorRef<AM.TypeOf<typeof Worker>>())
                        })
                      )
                    ),
                    allocations: S.prop(
                      S.chunk(
                        S.props({
                          shard: S.prop(S.number),
                          workerId: S.prop(S.number)
                        })
                      )
                    ),
                    lastId: S.prop(S.number)
                  }),
                  O.none
                )(({ state }) => ({
                  Join: ({ actor, nodeId }) =>
                    T.gen(function* (_) {
                      const { allocations, lastId, nodes } = yield* _(state.get)
                      const workerId = lastId + 1
                      yield* _(
                        state.set({
                          lastId: workerId,
                          nodes: Chunk.append_(nodes, { nodeId, workerId, actor }),
                          allocations
                        })
                      )
                      return { workerId }
                    }),
                  GetShardLocation: ({ shard }) =>
                    T.gen(function* (_) {
                      const { allocations, lastId, nodes } = yield* _(state.get)

                      if (nodes.length === 0) {
                        return new NoWorker({})
                      }

                      const current = Chunk.find_(allocations, (_) => _.shard === shard)

                      if (O.isSome(current)) {
                        return new ShardLocation(
                          yield* _(
                            T.getOrFail(
                              Chunk.find_(
                                nodes,
                                (_) => _.workerId === current.value.workerId
                              )
                            )["|>"](T.orDie)
                          )
                        )
                      } else {
                        const select = yield* _(nextIntBetween(0, nodes.length - 1))
                        const node = Chunk.unsafeGet_(nodes, select)
                        yield* _(
                          state.set({
                            allocations: Chunk.append_(allocations, {
                              shard,
                              workerId: node.workerId
                            }),
                            lastId,
                            nodes
                          })
                        )
                        return new ShardLocation({
                          actor: node.actor,
                          nodeId: node.nodeId
                        })
                      }
                    })
                }))
              ),
              {
                lastId: 0,
                nodes: Chunk.empty<never>(),
                allocations: Chunk.empty<never>()
              }
            ),
            M.makeInterruptible((_) => _.stop["|>"](T.orDie))
          )
        )

        console.log(coordinator.address)

        const { workerId } = yield* _(
          coordinator.ask(new Join({ actor: worker, nodeId: cluster.nodeId }))
        )

        console.log(yield* _(coordinator.ask(new GetShardLocation({ shard: 10 }))))

        console.log(workerId)

        return yield* _(T.interruptible(T.never))
      }),
      M.useNow
    )
  )
}
