import { expect } from 'chai';

import { ChannelModel, connect } from 'amqplib';

import { DefaultChannelProvider } from './default-channel-provider.js';

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';

describe('DefaultChannelProvider', function () {
  describe('Provided connection', function () {
    let connection: ChannelModel;

    before(async function () {
      connection = await connect({ hostname: AMQP_HOST });
    });

    after(async function () {
      await connection.close();
    });

    it('should return a channel', async function () {
      const provider = new DefaultChannelProvider({ connection });
      const channel = await provider.get();
      expect(channel.constructor.name).to.equal('Channel');
    });
  });

  describe('Provided options', function () {
    it('should return a channel', async function () {
      const provider = new DefaultChannelProvider({
        connectionOptions: { hostname: AMQP_HOST },
      });
      const channel = await provider.get();
      expect(channel.constructor.name).to.equal('Channel');
      expect(channel.connection.constructor.name).to.equal('Connection');
    });
  });

  describe('Provided a url', function () {
    it('should return a channel', async function () {
      const provider = new DefaultChannelProvider({
        connectionOptions: { url: `amqp://${AMQP_HOST}` },
      });
      const channel = await provider.get();
      expect(channel.constructor.name).to.equal('Channel');
      expect(channel.connection.constructor.name).to.equal('Connection');
    });
  });
});
