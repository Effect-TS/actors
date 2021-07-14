import type { IsEqualTo } from "@effect-ts/core/Utils"
import type * as S from "@effect-ts/schema"

export const RequestSchemaSymbol = Symbol("@effect-ts/actors/RequestSchema")
export const ResponseSchemaSymbol = Symbol("@effect-ts/actors/ResponseSchema")
export const _Response = Symbol("@effect-ts/actors/PhantomResponse")

export interface Message<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
> {
  readonly _tag: Tag
  readonly [_Response]: () => S.ParsedShapeOf<Res>

  readonly [ResponseSchemaSymbol]: Res
  readonly [RequestSchemaSymbol]: Req
}

export type TypedMessage<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
> = S.ParsedShapeOf<Req> & Message<Tag, Req, Res>

export type AnyMessage = Message<any, S.SchemaAny, S.SchemaAny>

export type ResponseOf<A extends AnyMessage> = [A] extends [Message<any, any, infer B>]
  ? S.ParsedShapeOf<B>
  : void

export type RequestOf<A extends AnyMessage> = [A] extends [Message<any, infer B, any>]
  ? S.ParsedShapeOf<B>
  : void

export type TagsOf<A extends AnyMessage> = A["_tag"]
export type ExtractTagged<A extends AnyMessage, Tag extends string> = Extract<
  A,
  TypedMessage<Tag, any, any>
>

type MessageFactoryOf<A extends AnyMessage> = [A] extends [
  Message<infer Tag, infer Req, infer Res>
]
  ? MessageFactory<Tag, Req, Res>
  : never

export interface MessageRegistry<F1>
  extends Record<string, [F1] extends [AnyMessage] ? MessageFactoryOf<F1> : never> {}

export type InstanceOf<A> = [A] extends [{ new (...any: any[]): infer B }] ? B : never

export interface MessageFactory<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
> {
  readonly Tag: Tag
  readonly RequestSchema: Req
  readonly ResponseSchema: Res

  new (
    _: IsEqualTo<S.ParsedShapeOf<Req>, {}> extends true ? void : S.ParsedShapeOf<Req>
  ): TypedMessage<Tag, Req, Res>
}

export type AnyMessageFactory = MessageFactory<any, S.SchemaAny, S.SchemaAny>

export type TypeOf<A extends MessageRegistry<any>> = [A] extends [
  MessageRegistry<infer B>
]
  ? B
  : never

export function Message<
  Tag extends string,
  Req extends S.SchemaUPI,
  Res extends S.SchemaUPI
>(Tag: Tag, Req: Req, Res: Res): MessageFactory<Tag, Req, Res> {
  // @ts-expect-error
  return class {
    static RequestSchema = Req
    static ResponseSchema = Res
    static Tag = Tag

    readonly _tag = Tag;

    readonly [ResponseSchemaSymbol] = Res;
    readonly [RequestSchemaSymbol] = Req

    constructor(ps?: any) {
      if (ps) {
        for (const k of Object.keys(ps)) {
          Object.defineProperty(this, k, { value: ps[k] })
        }
      }
    }
  }
}

export function messages<Messages extends AnyMessageFactory>(
  messages: Record<string, Messages>
): MessageRegistry<InstanceOf<Messages>> {
  return Object.values(messages).reduce(
    (obj, entry) => ({ ...obj, [entry.Tag]: entry }),
    {} as MessageRegistry<any>
  )
}
