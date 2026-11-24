import {
  ProcessManager,
  SERVICE_STOPPED,
  SERVICE_STOP_ERROR,
  isStoppable,
} from '@sektek/utility-belt';
import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';

import { connect } from 'amqplib';

import { DefaultConnectionProvider } from './default-connection-provider.js';

use(sinonChai);
use(chaiAsPromised);

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

  describe('lifecycle events', function () {
    afterEach(function () {
      sinon.restore();
    });

    it('should be stoppable', function () {
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
      });

      expect(isStoppable(provider)).to.be.true;
    });

    it('should emit service:stopped once on stop', async function () {
      const listener = sinon.fake();
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
      }).on(SERVICE_STOPPED, listener);
      await provider.get();

      await provider.stop();

      expect(listener).to.have.been.calledOnceWithExactly();
    });

    it('should emit service:stopped after the connection has closed', async function () {
      const closed = sinon.fake();
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
      });
      const connection = await provider.get();
      const close = sinon.stub(connection, 'close').callsFake(async () => {
        closed();
      });
      const listener = sinon.fake();
      provider.on(SERVICE_STOPPED, listener);

      await provider.stop();

      expect(closed).to.have.been.calledBefore(listener);
      close.restore();
      await connection.close();
    });

    it('should be stopped by a ProcessManager', async function () {
      const listener = sinon.fake();
      const processManager = new ProcessManager({});
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
        processManager,
      }).on(SERVICE_STOPPED, listener);
      await provider.get();

      await processManager.stop();

      expect(listener).to.have.been.calledOnce;
    });

    it('should emit service:stopError and rethrow the same error when stop fails', async function () {
      const error = new Error('remove failed');
      const stopped = sinon.fake();
      const errored = sinon.fake();
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
      })
        .on(SERVICE_STOPPED, stopped)
        .on(SERVICE_STOP_ERROR, errored);
      await provider.get();
      sinon.stub(ProcessManager.prototype, 'remove').throws(error);

      await expect(provider.stop()).to.be.rejectedWith(error);

      expect(errored).to.have.been.calledOnceWithExactly(error);
      expect(stopped).to.not.have.been.called;
    });

    it('should wrap a non-Error failure for the event and rethrow the original value', async function () {
      const thrown = { reason: 'boom' };
      const errored = sinon.fake();
      provider = new DefaultConnectionProvider({
        connectionOptions: { hostname: AMQP_HOST },
      }).on(SERVICE_STOP_ERROR, errored);
      await provider.get();
      sinon.stub(ProcessManager.prototype, 'remove').throws(thrown);

      const rejection = await provider.stop().catch((err: unknown) => err);

      expect(rejection).to.equal(thrown);
      const [emitted] = errored.firstCall.args;
      expect(emitted).to.be.instanceOf(Error);
      expect(emitted.cause).to.equal(thrown);
    });
  });
});
