import { Component } from '@sektek/utility-belt';
import { ConsumeMessage } from 'amqplib';
import { Event } from '@sektek/synaptik';

export type MessageEventExtractorFn<T extends Event = Event> = (
  message: ConsumeMessage,
) => T | PromiseLike<T>;

export interface MessageEventExtractor<T extends Event = Event> {
  extract: MessageEventExtractorFn<T>;
}

export type MessageEventExtractorComponent<T extends Event = Event> = Component<
  MessageEventExtractor<T>,
  'extract'
>;
