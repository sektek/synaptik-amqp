type AmqpSerializerReturnType = Uint8Array | readonly number[] | string;

export type AmqpSerializerFn<T> = (
  event: T,
) => AmqpSerializerReturnType | PromiseLike<AmqpSerializerReturnType>;

export interface AmqpSerializer<T> {
  serialize: AmqpSerializerFn<T>;
}
