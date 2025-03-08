import { ConsumeMessage } from 'amqplib';
import { Event } from '@sektek/synaptik';

export const defaultEventExtractor = <T extends Event = Event>(
  message: ConsumeMessage,
) => {
  return JSON.parse(message.content.toString()) as T;
};
