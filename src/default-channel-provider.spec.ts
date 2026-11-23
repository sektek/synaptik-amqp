import {
  SERVICE_STOPPED,
  SERVICE_STOP_ERROR,
  isStoppable,
} from '@sektek/utility-belt';
import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';
import sinon from 'sinon';

import { connect } from 'amqplib';

import { DefaultChannelProvider } from './default-channel-provider.js';
import { DefaultConnectionProvider } from './default-connection-provider.js';

use(chaiAsPromised);

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';

describe('DefaultChannelProvider', function () {
  let provider: DefaultChannelProvider | undefined;

  afterEach(async function () {
    if (provider) {
      await provider.stop();
      provider = undefined;
    }
  });

  describe('Provided connection', function () {
    let connection: Awaited<ReturnType<typeof connect>>;

    afterEach(async function () {
      await connection.close();
    });

    it('should return a channel', async function () {
      connection = await connect({ hostname: AMQP_HOST });
      provider = new DefaultChannelProvider({ connection });
      const channel = await provider.get();
      expect(channel.constructor.name).to.equal('Channel');
    });
  });

  describe('Provided options', function () {
    it('should return a channel', async function () {
      provider = new DefaultChannelProvider({
        connectionOptions: { hostname: AMQP_HOST },
      });
      const channel = await provider.get();
      expect(channel.constructor.name).to.equal('Channel');
      expect(channel.connection.constructor.name).to.equal('Connection');
    });
  });

  describe('Provided a url', function () {
    it('should return a channel', async function () {
      provider = new DefaultChannelProvider({
        connectionOptions: { url: `amqp://${AMQP_HOST}` },
      });
      const channel = await provider.get();
      expect(channel.constructor.name).to.equal('Channel');
      expect(channel.connection.constructor.name).to.equal('Connection');
    });
  });

  describe('lifecycle events', function () {
    afterEach(function () {
      sinon.restore();
    });

    it('should be stoppable', function () {
      provider = new DefaultChannelProvider({
        connectionOptions: { hostname: AMQP_HOST },
      });

      expect(isStoppable(provider)).to.be.true;
    });

    it('should emit service:stopped once on stop', async function () {
      const listener = sinon.fake();
      provider = new DefaultChannelProvider({
        connectionOptions: { hostname: AMQP_HOST },
      }).on(SERVICE_STOPPED, listener);
      await provider.get();

      await provider.stop();

      expect(listener.calledOnceWithExactly()).to.be.true;
    });

    it('should propagate stop to its own connection provider without re-emitting its events', async function () {
      const connectionStop = sinon.spy(
        DefaultConnectionProvider.prototype,
        'stop',
      );
      const connectionEmit = sinon.spy(
        DefaultConnectionProvider.prototype,
        'emit',
      );
      const listener = sinon.fake();
      provider = new DefaultChannelProvider({
        connectionOptions: { hostname: AMQP_HOST },
      }).on(SERVICE_STOPPED, listener);
      await provider.get();

      await provider.stop();

      expect(connectionStop.calledOnce).to.be.true;
      expect(connectionEmit.withArgs(SERVICE_STOPPED).calledOnce).to.be.true;
      expect(listener.calledOnce).to.be.true;
      expect(connectionEmit.calledBefore(listener)).to.be.true;
    });

    it('should emit service:stopError and rethrow the same error when the propagated stop fails', async function () {
      const error = new Error('connection stop failed');
      const original = DefaultConnectionProvider.prototype.stop;
      sinon
        .stub(DefaultConnectionProvider.prototype, 'stop')
        .callsFake(async function (this: DefaultConnectionProvider) {
          await original.call(this);
          throw error;
        });
      const stopped = sinon.fake();
      const errored = sinon.fake();
      provider = new DefaultChannelProvider({
        connectionOptions: { hostname: AMQP_HOST },
      })
        .on(SERVICE_STOPPED, stopped)
        .on(SERVICE_STOP_ERROR, errored);
      await provider.get();

      await expect(provider.stop()).to.be.rejectedWith(error);

      expect(errored.calledOnceWithExactly(error)).to.be.true;
      expect(stopped.called).to.be.false;
    });

    it('should wrap a non-Error failure for the event and rethrow the original value', async function () {
      const thrown = { reason: 'boom' };
      const original = DefaultConnectionProvider.prototype.stop;
      sinon
        .stub(DefaultConnectionProvider.prototype, 'stop')
        .callsFake(async function (this: DefaultConnectionProvider) {
          await original.call(this);
          throw thrown;
        });
      const errored = sinon.fake();
      provider = new DefaultChannelProvider({
        connectionOptions: { hostname: AMQP_HOST },
      }).on(SERVICE_STOP_ERROR, errored);
      await provider.get();

      const rejection = await provider.stop().catch((err: unknown) => err);

      expect(rejection).to.equal(thrown);
      const [emitted] = errored.firstCall.args;
      expect(emitted).to.be.instanceOf(Error);
      expect(emitted.cause).to.equal(thrown);
    });
  });
});
