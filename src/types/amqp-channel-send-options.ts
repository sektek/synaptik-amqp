import { EventChannelSendOptions } from '@sektek/synaptik';
import { Options } from 'amqplib';

export type AmqpChannelSendOptions = EventChannelSendOptions &
  Options.Publish & {
    exchange?: string;
    routingKey?: string;
  };
