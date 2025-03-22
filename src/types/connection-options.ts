import { Options } from 'amqplib';

export type ConnectionOptions = Options.Connect & {
  url?: string;
};
