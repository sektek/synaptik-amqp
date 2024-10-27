import { ConsumeMessage } from 'amqplib';

export type MessageEventExtractorFn<T> = (message: ConsumeMessage) => T;

export interface MessageEventExtractor<T> {
  extract: MessageEventExtractorFn<T>;
}
