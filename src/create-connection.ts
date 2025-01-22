import { connect as amqpConnect } from 'amqplib';

import { ConnectionOptions } from './types/index.js';

export const createConnection = async (options: ConnectionOptions) => {
  return await amqpConnect(options.url ?? options);
};
