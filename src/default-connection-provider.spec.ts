import { expect, use } from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';

import { connect } from 'amqplib';

import { DefaultConnectionProvider } from './default-connection-provider.js';

use(sinonChai);

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';

describe('DefaultConnectionProvider', function () {
  let provider: DefaultConnectionProvider | undefined;

  afterEach(async function () {
    if (provider) {
      await provider.stop();
      provider = undefined;
    }
  });

  describe('Provided connection', function () {
    let connection: Awaited<ReturnType<typeof connect>>;

    beforeEach(async function () {
      connection = await connect({ hostname: AMQP_HOST });
    });

    afterEach(async function () {
      await connection.close();
    });

    it('should return the provided connection', async function () {
      provider = new DefaultConnectionProvider({ connection });
      const result = await provider.get();
      expect(result).to.equal(connection);
    });

    it('should not close the provided connection on stop', async function () {
      provider = new DefaultConnectionProvider({ connection });
      await provider.stop();
      const channel = await connection.createChannel();
      expect(channel).to.exist;
      await channel.close();
    });
  });

  describe('Provided options', function () {
    it('should return a connection', async function () {
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
      });
      const connection = await provider.get();
      expect(connection.constructor.name).to.equal('ChannelModel');
    });
  });

  describe('Provided a url', function () {
    it('should return a connection', async function () {
      provider = new DefaultConnectionProvider({
        connectionOptions: { url: `amqp://${AMQP_HOST}` },
      });
      const connection = await provider.get();
      expect(connection.constructor.name).to.equal('ChannelModel');
    });
  });

  describe('events', function () {
    let provider: DefaultConnectionProvider | undefined;

    beforeEach(function () {
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
      });
    });

    afterEach(async function () {
      if (provider) {
        await provider.stop();
        provider = undefined;
      }
    });

    it('should emit connection:created when a connection is created', async function () {
      const connectionCreated = sinon.fake();
      provider?.on('connection:created', connectionCreated);
      await provider?.get();
      expect(connectionCreated).to.have.been.calledOnce;
    });

    // Unable to test other events as amqplib only emits 'error' and 'close'
    // on the connection when there is an issue with the connection, which is
    // difficult to simulate in a test environment without mocking amqplib.
  });
});
