import { randomUUID } from 'crypto';

import { expect, use } from 'chai';
import sinon, { fake, match, spy, stub } from 'sinon';
import chaiAsPromised from 'chai-as-promised';
import sinonChai from 'sinon-chai';

import { Channel, ChannelModel, ConsumeMessage, connect } from 'amqplib';
import {
  SERVICE_STARTED,
  SERVICE_START_ERROR,
  SERVICE_STOPPED,
  SERVICE_STOP_ERROR,
  isStartable,
  isStoppable,
} from '@sektek/utility-belt';
import { EventBuilder } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { AmqpGateway } from './amqp-gateway.js';
import { DefaultChannelProvider } from './default-channel-provider.js';

use(sinonChai);
use(chaiAsPromised);

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';
const WAIT_TIME = 500;

describe('AmqpGateway', function () {
  let queueName: string;
  let gateway: AmqpGateway | undefined;
  let connection: ChannelModel, channel: Channel;
  let channelProvider: DefaultChannelProvider;
  let originalChannelProvider: DefaultChannelProvider;
  let eventChannel: AmqpChannel;

  beforeEach(async function () {
    queueName = `test-queue-${randomUUID()}`;
    connection = await connect({ hostname: AMQP_HOST });
    channelProvider = originalChannelProvider = new DefaultChannelProvider({
      connection,
    });
    channel = await channelProvider.get();
    await channel.assertQueue(queueName);
    eventChannel = new AmqpChannel({ channelProvider, queueName });
  });

  afterEach(async function () {
    if (gateway) {
      await gateway.stop();
      gateway = undefined;
    }
    await channel.deleteQueue(queueName);
    // Some tests reassign `channelProvider` to a wrapper built from a raw
    // `channel`, so `originalChannelProvider` is stopped explicitly to
    // guarantee the connection-backed provider (and its ProcessManager)
    // created in beforeEach is always cleaned up.
    await originalChannelProvider.stop();
    await connection.close();
    sinon.reset();
  });

  it('should receive events and send them to the handler', async function () {
    const handler = fake();
    gateway = new AmqpGateway({ channelProvider, handler, queueName });

    const event = await new EventBuilder().create();
    await eventChannel.send(event);
    await gateway.start();
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    expect(handler).to.have.been.calledWith(event);
  });

  it('should acknowledge messages by default', async function () {
    channel = spy(channel);
    channelProvider = new DefaultChannelProvider({ channel });
    const handler = fake();
    gateway = new AmqpGateway({ channelProvider, handler, queueName });

    const event = await new EventBuilder().create();
    await eventChannel.send(event);
    await gateway.start();
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    expect(channel.ack).to.have.been.called;
  });

  it('should continue processing messages after an error', async function () {
    const handler = stub().onFirstCall().throws(new Error('Handler error'));
    gateway = new AmqpGateway({
      channelProvider,
      handler,
      queueName,
      requeueOnError: false,
    });

    const firstEvent = await new EventBuilder().create();
    await eventChannel.send(firstEvent);
    await gateway.start();
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    const secondEvent = await new EventBuilder().create();
    await eventChannel.send(secondEvent);
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    expect(handler).to.have.been.calledTwice;
    expect(handler.secondCall).to.have.been.calledWith(secondEvent);
    await gateway.stop();
  });

  describe('event emitter', function () {
    it('should emit a message:received event', async function () {
      const event = await new EventBuilder().create();
      const handler = fake();
      const listener = (message: ConsumeMessage) => {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      };

      gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:received', listener);

      await eventChannel.send(event);
      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));
    });

    it('should emit an event:received event', async function () {
      const event = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake();
      const listener = fake();
      gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('event:received', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(event);
    });

    it('should emit an event:processed event', async function () {
      const event = await new EventBuilder().create();
      const result = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake.returns(result);
      const listener = fake();
      gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('event:processed', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(event, result);
    });

    it('should emit a message:processed event', async function () {
      const event = await new EventBuilder().create();
      const result = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake.returns(result);
      const listener = fake();
      gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:processed', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(
        match.typeOf('object'),
        event,
        result,
      );
    });

    it('should emit a message:acknowledged event', async function () {
      const event = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake();
      const listener = fake();
      gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:acknowledged', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(
        // eslint-disable-next-line sonarjs/no-same-argument-assert
        match.typeOf('object'),
        match.typeOf('object'),
        event,
      );
    });

    it('should not emit a message:acknowledged event when noAck is true', async function () {
      const event = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake();
      const listener = fake();
      gateway = new AmqpGateway({
        channelProvider,
        handler,
        queueName,
        consumeOptions: { noAck: true },
      });
      gateway.on('message:acknowledged', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.not.have.been.called;
    });

    it('should emit an event:error on error', async function () {
      const event = await new EventBuilder().create();
      const error = new Error('Handler error');
      await eventChannel.send(event);

      const handler = fake.throws(error);
      const listener = fake();
      gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('event:error', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(error, event);
    });

    it('should emit a message:error on error', async function () {
      const event = await new EventBuilder().create();
      const error = new Error('Handler error');
      await eventChannel.send(event);

      const handler = fake.throws(error);
      const listener = fake();
      gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:error', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(error, match.typeOf('object'));
    });
  });

  describe('lifecycle events', function () {
    it('should be startable and stoppable', function () {
      gateway = new AmqpGateway({
        channelProvider,
        handler: fake(),
        queueName,
      });

      expect(isStartable(gateway)).to.be.true;
      expect(isStoppable(gateway)).to.be.true;
    });

    it('should emit gateway:started with the channel and consumer', async function () {
      const listener = fake();
      gateway = new AmqpGateway({
        channelProvider,
        handler: fake(),
        queueName,
      });
      gateway.on('gateway:started', listener);

      await gateway.start();

      expect(listener).to.have.been.calledOnceWithExactly(
        channel,
        match.has('consumerTag'),
      );
    });

    it('should emit gateway:stopped without arguments', async function () {
      const listener = fake();
      gateway = new AmqpGateway({
        channelProvider,
        handler: fake(),
        queueName,
      });
      gateway.on('gateway:stopped', listener);

      await gateway.start();
      await gateway.stop();

      expect(listener).to.have.been.calledOnceWithExactly();
    });

    it('should emit service:started and service:stopped once each', async function () {
      const started = fake();
      const stopped = fake();
      gateway = new AmqpGateway({
        channelProvider,
        handler: fake(),
        queueName,
      });
      gateway.on(SERVICE_STARTED, started).on(SERVICE_STOPPED, stopped);

      await gateway.start();
      await gateway.stop();

      expect(started).to.have.been.calledOnceWithExactly();
      expect(stopped).to.have.been.calledOnceWithExactly();
      expect(started).to.have.been.calledBefore(stopped);
    });

    it('should emit gateway:started before service:started', async function () {
      const gatewayListener = fake();
      const serviceListener = fake();
      gateway = new AmqpGateway({
        channelProvider,
        handler: fake(),
        queueName,
      });
      gateway
        .on('gateway:started', gatewayListener)
        .on(SERVICE_STARTED, serviceListener);

      await gateway.start();

      expect(gatewayListener).to.have.been.calledBefore(serviceListener);
    });

    it('should emit gateway:stopped before service:stopped', async function () {
      const gatewayListener = fake();
      const serviceListener = fake();
      gateway = new AmqpGateway({
        channelProvider,
        handler: fake(),
        queueName,
      });
      gateway
        .on('gateway:stopped', gatewayListener)
        .on(SERVICE_STOPPED, serviceListener);

      await gateway.start();
      await gateway.stop();

      expect(gatewayListener).to.have.been.calledBefore(serviceListener);
    });

    it('should not emit stop events when stopped without having been started', async function () {
      const gatewayListener = fake();
      const serviceListener = fake();
      gateway = new AmqpGateway({
        channelProvider,
        handler: fake(),
        queueName,
      });
      gateway
        .on('gateway:stopped', gatewayListener)
        .on(SERVICE_STOPPED, serviceListener);

      await gateway.stop();

      expect(gatewayListener).to.not.have.been.called;
      expect(serviceListener).to.not.have.been.called;
    });

    describe('failures', function () {
      it('should emit service:startError and rethrow the same error when start() fails', async function () {
        const error = new Error('start failed');
        const startedListener = fake();
        const gatewayListener = fake();
        const errorListener = fake();
        gateway = new AmqpGateway({
          channelProvider: fake.rejects(error),
          handler: fake(),
          queueName,
        });
        gateway
          .on(SERVICE_STARTED, startedListener)
          .on('gateway:started', gatewayListener)
          .on(SERVICE_START_ERROR, errorListener);

        await expect(gateway.start()).to.be.rejectedWith(error);

        expect(errorListener).to.have.been.calledOnceWithExactly(error);
        expect(startedListener).to.not.have.been.called;
        expect(gatewayListener).to.not.have.been.called;
      });

      it('should wrap a non-Error start failure for the event and rethrow the original value', async function () {
        const thrown = { reason: 'boom' };
        const errorListener = fake();
        gateway = new AmqpGateway({
          channelProvider: async () => {
            throw thrown;
          },
          handler: fake(),
          queueName,
        });
        gateway.on(SERVICE_START_ERROR, errorListener);

        const rejection = await gateway.start().catch((err: unknown) => err);

        expect(rejection).to.equal(thrown);
        const [emitted] = errorListener.firstCall.args;
        expect(emitted).to.be.instanceOf(Error);
        expect(emitted.cause).to.equal(thrown);
      });

      it('should emit service:stopError and rethrow the same error when stop() fails', async function () {
        const error = new Error('stop failed');
        let failNext = false;
        const flakyProvider = async () => {
          if (failNext) {
            throw error;
          }

          return channelProvider.get();
        };
        const stoppedListener = fake();
        const gatewayListener = fake();
        const errorListener = fake();
        gateway = new AmqpGateway({
          channelProvider: flakyProvider,
          handler: fake(),
          queueName,
        });
        gateway
          .on(SERVICE_STOPPED, stoppedListener)
          .on('gateway:stopped', gatewayListener)
          .on(SERVICE_STOP_ERROR, errorListener);
        await gateway.start();

        failNext = true;
        await expect(gateway.stop()).to.be.rejectedWith(error);
        failNext = false;

        expect(errorListener).to.have.been.calledOnceWithExactly(error);
        expect(stoppedListener).to.not.have.been.called;
        expect(gatewayListener).to.not.have.been.called;
      });
    });
  });
});
