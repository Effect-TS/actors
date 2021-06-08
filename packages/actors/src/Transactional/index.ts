import { Chunk, pipe } from "@effect-ts/core"
import * as T from "@effect-ts/core/Effect"
import { pretty } from "@effect-ts/core/Effect/Cause"
import * as L from "@effect-ts/core/Effect/Layer"
import * as M from "@effect-ts/core/Effect/Managed"
import * as P from "@effect-ts/core/Effect/Promise"
import * as Q from "@effect-ts/core/Effect/Queue"
import * as REF from "@effect-ts/core/Effect/Ref"
import type { Has } from "@effect-ts/core/Has"
import { tag } from "@effect-ts/core/Has"
import * as O from "@effect-ts/core/Option"
import { hash } from "@effect-ts/core/Structural"
import * as PG from "@effect-ts/pg"
import type * as SCH from "@effect-ts/schema"
import * as S from "@effect-ts/schema"
import * as Encoder from "@effect-ts/schema/Encoder"
import * as Parser from "@effect-ts/schema/Parser"
import { identity } from "@effect-ts/system/Function"

import type { PendingMessage } from "../Actor"
import { AbstractStateful, Actor } from "../Actor"
import { withSystem } from "../ActorRef"
import * as AS from "../ActorSystem"
import type { Throwable } from "../common"
import type * as AM from "../Message"
import type * as SUP from "../Supervisor"

export type TransactionalEnvelope<F1 extends AM.AnyMessage> = {
  [Tag in AM.TagsOf<F1>]: {
    _tag: Tag
    payload: AM.RequestOf<AM.ExtractTagged<F1, Tag>>
    handle: <R, E>(
      _: T.Effect<R, E, AM.ResponseOf<AM.ExtractTagged<F1, Tag>>>
    ) => T.Effect<R, E, AM.ResponseOf<AM.ExtractTagged<F1, Tag>>>
  }
}[AM.TagsOf<F1>]

export function transactional<S, F1 extends AM.AnyMessage, Ev = never>(
  messages: AM.MessageRegistry<F1>,
  stateSchema: SCH.Standard<S>,
  eventSchema: O.Option<SCH.Standard<Ev>>
) {
  return <R>(
    receive: (
      dsl: {
        state: {
          get: T.UIO<S>
          set: (s: S) => T.UIO<void>
        }
        event: {
          emit: (e: Ev) => T.UIO<void>
        }
      },
      context: AS.Context<F1>
    ) => (
      msg: TransactionalEnvelope<F1>
    ) => T.Effect<R, Throwable, AM.ResponseOf<AM.ExtractTagged<F1, F1["_tag"]>>>
  ) => new Transactional<R, S, Ev, F1>(messages, stateSchema, eventSchema, receive)
}

export interface StateStorageAdapter {
  readonly transaction: (
    persistenceId: string
  ) => <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R, E, A>

  readonly get: (persistenceId: string) => T.Effect<
    unknown,
    never,
    O.Option<{
      persistenceId: string
      shard: number
      state: unknown
      event_sequence: number
    }>
  >

  readonly set: (
    persistenceId: string,
    value: unknown,
    event_sequence: number
  ) => T.Effect<unknown, never, void>

  readonly emit: (
    persistenceId: string,
    value: unknown,
    event_sequence: number
  ) => T.Effect<unknown, never, void>
}

export const StateStorageAdapter = tag<StateStorageAdapter>()

export interface ShardConfig {
  shards: number
}

export const ShardConfig = tag<ShardConfig>()

export const LiveStateStorageAdapter = L.fromManaged(StateStorageAdapter)(
  M.gen(function* (_) {
    const cli = yield* _(PG.PG)

    yield* _(
      cli.query(`
      CREATE TABLE IF NOT EXISTS "state_journal" (
        persistence_id  text PRIMARY KEY,
        shard           integer,
        event_sequence  integer,
        state           jsonb
      );`)
    )

    yield* _(
      cli.query(`
      CREATE TABLE IF NOT EXISTS "event_journal" (
        persistence_id  text,
        shard           integer,
        sequence        integer,
        event           jsonb,
        PRIMARY KEY(persistence_id, sequence)
      );`)
    )

    const transaction: (
      persistenceId: string
    ) => <R, E, A>(effect: T.Effect<R, E, A>) => T.Effect<R, E, A> = () =>
      cli.transaction

    const get: (persistenceId: string) => T.Effect<
      unknown,
      never,
      O.Option<{
        persistenceId: string
        shard: number
        state: unknown
        event_sequence: number
      }>
    > = (persistenceId) =>
      pipe(
        cli.query(
          `SELECT * FROM "state_journal" WHERE "persistence_id" = '${persistenceId}'`
        ),
        T.map((res) =>
          pipe(
            O.fromNullable(res.rows?.[0]),
            O.map((row) => ({
              persistenceId: row.actor_name,
              shard: row.shard,
              state: row["state"],
              event_sequence: row.event_sequence
            }))
          )
        )
      )

    const set: (
      persistenceId: string,
      value: unknown,
      event_sequence: number
    ) => T.Effect<unknown, never, void> = (persistenceId, value, event_sequence) =>
      pipe(
        calcShard(persistenceId),
        T.chain((shard) =>
          cli.query(
            `INSERT INTO "state_journal" ("persistence_id", "shard", "state", "event_sequence") VALUES('${persistenceId}', $2::integer, $1::jsonb, $3::integer) ON CONFLICT ("persistence_id") DO UPDATE SET "state" = $1::jsonb, "event_sequence" = $3::integer`,
            [JSON.stringify(value), shard, event_sequence]
          )
        ),
        T.asUnit
      )

    const emit: (
      persistenceId: string,
      value: unknown,
      event_sequence: number
    ) => T.Effect<unknown, never, void> = (persistenceId, value, event_sequence) =>
      pipe(
        calcShard(persistenceId),
        T.chain((shard) =>
          cli.query(
            `INSERT INTO "event_journal" ("persistence_id", "shard", "event", "sequence") VALUES('${persistenceId}', $2::integer, $1::jsonb, $3::integer)`,
            [JSON.stringify(value), shard, event_sequence]
          )
        ),
        T.asUnit
      )["|>"](
        T.tapCause((c) =>
          T.succeedWith(() => {
            console.log(pretty(c))
          })
        )
      )

    return identity<StateStorageAdapter>({
      transaction,
      get,
      set,
      emit
    })
  })
)

const mod = (m: number) => (x: number) => x < 0 ? (x % m) + m : x % m

const calcShard = (id: string) =>
  T.access((r: unknown) => {
    const maybe = ShardConfig.readOption(r)
    if (O.isSome(maybe)) {
      return mod(maybe.value.shards)(hash(id))
    } else {
      return mod(16)(hash(id))
    }
  })

export class Transactional<R, S, Ev, F1 extends AM.AnyMessage> extends AbstractStateful<
  R & Has<StateStorageAdapter>,
  S,
  F1
> {
  private readonly dbStateSchema = S.props({
    current: S.prop(this.stateSchema)
  })

  readonly decodeState = (s: unknown, system: AS.ActorSystem) =>
    S.condemnDie((u) => withSystem(system)(() => Parser.for(this.dbStateSchema)(u)))(s)

  readonly encodeState = Encoder.for(this.dbStateSchema)

  readonly encodeEvent = O.map_(this.eventSchema, (s) =>
    Encoder.for(S.props({ event: S.prop(s) }))
  )

  readonly getState = (initial: S, system: AS.ActorSystem, actorName: string) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { get } = yield* _(StateStorageAdapter)

      const state = yield* _(get(actorName))

      if (O.isSome(state)) {
        return [
          (yield* _(self.decodeState(state.value.state, system))).current,
          state.value.event_sequence
        ] as const
      }
      return [initial, 0] as const
    })
  }

  readonly setState = (current: S, actorName: string, sequence: number) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { set } = yield* _(StateStorageAdapter)

      yield* _(set(actorName, self.encodeState({ current }), sequence))
    })
  }

  readonly emitEvent = (event: Ev, actorName: string, sequence: number) => {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    return T.gen(function* (_) {
      const { emit } = yield* _(StateStorageAdapter)
      const encode = yield* _(self.encodeEvent)

      yield* _(emit(actorName, encode({ event }), sequence))
    })
  }

  constructor(
    readonly messages: AM.MessageRegistry<F1>,
    readonly stateSchema: SCH.Standard<S>,
    readonly eventSchema: O.Option<SCH.Standard<Ev>>,
    readonly receive: (
      dsl: {
        state: {
          get: T.UIO<S>
          set: (s: S) => T.UIO<void>
        }
        event: {
          emit: (e: Ev) => T.UIO<void>
        }
      },
      context: AS.Context<F1>
    ) => (
      msg: TransactionalEnvelope<F1>
    ) => T.Effect<R, Throwable, AM.ResponseOf<AM.ExtractTagged<F1, F1["_tag"]>>>
  ) {
    super()
  }

  defaultMailboxSize = 10000

  makeActor(
    supervisor: SUP.Supervisor<R>,
    context: AS.Context<F1>,
    optOutActorSystem: () => T.Effect<T.DefaultEnv, Throwable, void>,
    mailboxSize: number = this.defaultMailboxSize
  ): (initial: S) => T.RIO<R & T.DefaultEnv & Has<StateStorageAdapter>, Actor<F1>> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this

    const process = (msg: PendingMessage<F1>, initial: S) => {
      return T.accessServicesM({ prov: StateStorageAdapter })(({ prov }) =>
        pipe(
          AS.resolvePath(context.address)["|>"](T.orDie),
          T.map(([sysName, __, ___, actorName]) => `${sysName}(${actorName})`),
          T.chain((actorName) =>
            prov.transaction(actorName)(
              pipe(
                T.do,
                T.bind("s", () =>
                  self.getState(initial, context.actorSystem, actorName)
                ),
                T.bind("events", () => REF.makeRef(Chunk.empty<Ev>())),
                T.bind("state", (_) => REF.makeRef(_.s[0])),
                T.let("fa", () => msg[0]),
                T.let("promise", () => msg[1]),
                T.let("receiver", (_) => {
                  return this.receive(
                    {
                      event: {
                        emit: (ev) => REF.update_(_.events, Chunk.append(ev))
                      },
                      state: { get: REF.get(_.state), set: (s) => REF.set_(_.state, s) }
                    },
                    context
                  )({ _tag: _.fa._tag as any, payload: _.fa as any, handle: identity })
                }),
                T.let(
                  "completer",
                  (_) => (a: AM.ResponseOf<F1>) =>
                    pipe(
                      T.zip_(REF.get(_.events), REF.get(_.state)),
                      T.chain(({ tuple: [evs, s] }) =>
                        T.zip_(
                          self.setState(s, actorName, _.s[1] + evs.length),
                          T.forEach_(
                            Chunk.zipWithIndexOffset_(evs, _.s[1] + 1),
                            ({ tuple: [ev, seq] }) => self.emitEvent(ev, actorName, seq)
                          )
                        )
                      ),
                      T.zipRight(P.succeed_(_.promise, a)),
                      T.as(T.unit)
                    )
                ),
                T.chain((_) =>
                  T.foldM_(
                    _.receiver,
                    (e) =>
                      pipe(
                        supervisor.supervise(_.receiver, e),
                        T.foldM((__) => P.fail_(_.promise, e), _.completer)
                      ),
                    _.completer
                  )
                )
              )["|>"](
                T.tapCause((c) =>
                  T.succeedWith(() => {
                    console.error(pretty(c))
                  })
                )
              )
            )
          )
        )
      )
    }

    return (initial) =>
      pipe(
        T.do,
        T.bind("state", () => REF.makeRef(initial)),
        T.bind("queue", () => Q.makeBounded<PendingMessage<F1>>(mailboxSize)),
        T.bind("ref", () => REF.makeRef(O.emptyOf<S>())),
        T.tap((_) =>
          pipe(
            Q.take(_.queue),
            T.chain((t) => process(t, initial)),
            T.forever,
            T.fork
          )
        ),
        T.map((_) => new Actor(this.messages, _.queue, optOutActorSystem))
      )
  }
}
